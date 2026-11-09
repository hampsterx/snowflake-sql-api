"""Opt-in synthetic DML probes. Skips do not establish transaction or replay semantics."""

from __future__ import annotations

import inspect
import json
import os
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
import pytest

from snowflake_sql_api import (
    AsyncMultiStatementHandle,
    AsyncSnowflakeClient,
    MultiStatementHandle,
    RetryPolicy,
    SnowflakeClient,
    SnowflakeMultiStatementError,
    SnowflakeProgrammingError,
)
from snowflake_sql_api.escaping import quote_identifier

from .test_submissions import LoseAsyncResponse, LoseSyncResponse, LostResponse

REQUIRED = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_WAREHOUSE",
)
pytestmark = [
    pytest.mark.smoke,
    pytest.mark.skipif(
        os.environ.get("SNOWFLAKE_LIVE_DML") != "1"
        or not all(os.environ.get(name, "").strip() for name in REQUIRED),
        reason="requires live DML opt-in, credentials and explicit disposable namespace",
    ),
]


async def value(result):
    return await result if inspect.isawaitable(result) else result


@pytest.fixture
def multi_options():
    return {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key_path": os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        "private_key_passphrase": os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "host": os.environ.get("SNOWFLAKE_HOST"),
        "statement_timeout": 15,
        "poll_interval": 0.25,
        "retry_policy": RetryPolicy(max_attempts=1),
        "parameters": {
            "QUERY_TAG": "multi-statement-smoke",
            "USE_CACHED_RESULT": "false",
        },
    }


@asynccontextmanager
async def multi_client(options, async_mode, **kwargs):
    if async_mode:
        async with AsyncSnowflakeClient(**options, **kwargs) as client:
            yield client
    else:
        with SnowflakeClient(**options, **kwargs) as client:
            yield client


@pytest.fixture
def multi_table(multi_options):
    table = quote_identifier("MULTI_SMOKE_" + uuid4().hex.upper())
    with SnowflakeClient(**multi_options) as observer:
        observer.execute(f"CREATE TRANSIENT TABLE {table} (ID INTEGER, AMOUNT INTEGER)")
        try:
            observer.execute(f"INSERT INTO {table} VALUES (0, 0)")
            yield table, observer
        finally:
            observer.execute(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.asyncio
async def test_request_scoped_temporary_context(multi_options, async_mode):
    table = quote_identifier("MULTI_TEMP_" + uuid4().hex.upper())
    async with multi_client(multi_options, async_mode) as client:
        result = await value(
            client.execute_statements(
                [
                    f"CREATE TEMPORARY TABLE {table} (N INTEGER)",
                    f"INSERT INTO {table} VALUES (1), (2)",
                    f"SELECT N FROM {table} ORDER BY N",
                    f"DROP TABLE {table}",
                ]
            )
        )
    assert len(result.results) == 4
    assert result.results[2].rows == [{"N": 1}, {"N": 2}]


@pytest.mark.asyncio
async def test_transaction_commit_and_rollback_release_locks(
    multi_options, multi_table, async_mode, record_property
):
    table, observer = multi_table
    async with multi_client(multi_options, async_mode) as client:
        settings = await value(
            client.query("SHOW PARAMETERS LIKE 'AUTOCOMMIT' IN SESSION")
        )
        record_property("autocommit", json.dumps(settings, default=str))
        assert len(settings) == 1 and settings[0]["value"].lower() == "true"
        successful = await value(
            client.execute_transaction(
                [
                    f"UPDATE {table} SET AMOUNT=10 WHERE ID=0",
                    f"INSERT INTO {table} VALUES (1, 20)",
                ]
            )
        )
        assert len(successful.results) == 4
        assert observer.query(f"SELECT ID, AMOUNT FROM {table} ORDER BY ID") == [
            {"ID": 0, "AMOUNT": 10},
            {"ID": 1, "AMOUNT": 20},
        ]
        failing_body = [
            f"UPDATE {table} SET AMOUNT=99 WHERE ID=0",
            f"INSERT INTO {table} VALUES (2, 30)",
            f"UPDATE {table} SET AMOUNT=1/ID WHERE ID=0",
        ]
        with pytest.raises(SnowflakeMultiStatementError) as caught:
            await value(client.execute_transaction(failing_body))
        assert caught.value.status_code == 422
        assert "division by zero" in str(caught.value).lower()
        record_property("transaction_failure_code", caught.value.code)
        record_property("transaction_failure_sql_state", caught.value.sql_state)
        record_property("transaction_parent", caught.value.statement_handle)
        record_property("transaction_request_id", caught.value.request_id)
        assert observer.query(f"SELECT ID, AMOUNT FROM {table} ORDER BY ID") == [
            {"ID": 0, "AMOUNT": 10},
            {"ID": 1, "AMOUNT": 20},
        ]
        # Separate request: 15-second server/poll budget, independent HTTP timeout.
        observer.execute(f"UPDATE {table} SET AMOUNT=11 WHERE ID=0")
        assert observer.query_scalar(f"SELECT AMOUNT FROM {table} WHERE ID=0") == 11
        history = observer.query(
            "SELECT QUERY_ID, QUERY_TEXT, EXECUTION_STATUS, ERROR_CODE, ERROR_MESSAGE, "
            "TRANSACTION_ID FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER("
            "END_TIME_RANGE_START => DATEADD('minute', -15, CURRENT_TIMESTAMP()), "
            "RESULT_LIMIT => 10000, INCLUDE_CLIENT_GENERATED_STATEMENT => TRUE)) "
            "WHERE RTRIM(QUERY_TEXT, '; \\n\\r\\t') IN (?, ?, ?)",
            failing_body,
        )
        record_property("transaction_body_history", json.dumps(history, default=str))
        assert len(history) == 3, "require all body statements in recent query history"
        by_sql = {row["QUERY_TEXT"].rstrip("; \n\r\t"): row for row in history}
        assert set(by_sql) == set(failing_body)
        assert all(
            by_sql[sql]["EXECUTION_STATUS"] == "success" for sql in failing_body[:2]
        ), "the prefix must execute before the intended failure"
        failed = by_sql[failing_body[2]]
        assert failed["EXECUTION_STATUS"] == "failed_with_error"
        assert "division by zero" in (failed["ERROR_MESSAGE"] or "").lower()
        assert failed["TRANSACTION_ID"] not in (None, 0, "0")
        assert all(row["TRANSACTION_ID"] == failed["TRANSACTION_ID"] for row in history)
        record_property("rollback_and_lock_release", "verified by independent requests")


class LostMultiResponse(LostResponse):
    """Discard one actual response while retaining harness-only recovery evidence."""

    def __init__(self):
        super().__init__(None)
        self.payload = None

    def intercept(self, request, response):
        if request.method == "POST" and request.url.path == "/api/v2/statements":
            self.request_id = request.url.params["requestId"]
            self.payload = json.loads(request.content)
        return super().intercept(request, response)


@pytest.mark.asyncio
async def test_completed_parent_lost_response_replay(
    multi_options, multi_table, async_mode, record_property
):
    table, observer = multi_table
    observation = LostMultiResponse()
    transport = (
        LoseAsyncResponse(observation) if async_mode else LoseSyncResponse(observation)
    )
    http_class = httpx.AsyncClient if async_mode else httpx.Client
    http = http_class(transport=transport)
    try:
        async with multi_client(multi_options, async_mode, http_client=http) as client:
            with pytest.raises(SnowflakeMultiStatementError) as caught:
                await value(
                    client.execute_statements(
                        [
                            f"INSERT INTO {table} VALUES (1, 1)",
                            f"SELECT COUNT(*) AS N FROM {table}",
                        ]
                    )
                )
            assert caught.value.outcome == "unknown"
    finally:
        await value(http.aclose() if async_mode else http.close())
    assert observation.handle and observation.payload and observation.forwarded == 1
    async with multi_client(multi_options, async_mode) as recovered:
        handle_type = AsyncMultiStatementHandle if async_mode else MultiStatementHandle
        await value(
            handle_type(recovered, observation.handle, expected_count=2).result()
        )
        replay = await value(
            recovered._transport.submit(
                observation.payload,
                request_id=observation.request_id,
                retry=True,
            )
        )
        await value(
            handle_type(recovered, replay.statement_handle, expected_count=2).result()
        )
    assert observer.query_scalar(f"SELECT COUNT(*) FROM {table} WHERE ID=1") == 1
    record_property(
        "completed_parent_replay",
        "one inserted row after discarded response and retry=true",
    )


@pytest.mark.asyncio
async def test_partial_failure_replay_observations(
    multi_options, multi_table, async_mode, record_property
):
    table, observer = multi_table
    statements = [
        f"INSERT INTO {table} VALUES (1, 1)",
        f"UPDATE {table} SET AMOUNT=1/ID WHERE ID=0",
        f"INSERT INTO {table} VALUES (2, 2)",
    ]
    payload = {
        "statement": "\n;\n".join(statements),
        "parameters": {"MULTI_STATEMENT_COUNT": "3"},
    }
    request_id = str(uuid4())
    observations = []
    async with multi_client(multi_options, async_mode) as client:
        payload = {
            **client._build_payload(payload["statement"], None),
            "parameters": {
                **multi_options["parameters"],
                "MULTI_STATEMENT_COUNT": "3",
            },
        }
        for retry in (False, True):
            with pytest.raises(SnowflakeProgrammingError) as caught:
                response = await value(
                    client._transport.submit(
                        payload, request_id=request_id, retry=retry
                    )
                )
                if response.is_running:
                    await value(
                        client._poll(
                            response.statement_handle, 15, cancel_on_timeout=True
                        )
                    )
            observations.append(
                {
                    "retry": retry,
                    "code": caught.value.code,
                    "prefix_rows": observer.query_scalar(
                        f"SELECT COUNT(*) FROM {table} WHERE ID=1"
                    ),
                }
            )
            assert (
                observer.query_scalar(f"SELECT COUNT(*) FROM {table} WHERE ID=2") == 0
            )
    assert observations[0]["prefix_rows"] == 1
    record_property("partial_failure_replay", json.dumps(observations))
