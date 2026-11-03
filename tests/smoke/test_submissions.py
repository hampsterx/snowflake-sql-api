"""Opt-in replay probes using disposable tables, procedures and synthetic rows.

Requires SNOWFLAKE_LIVE_DML=1, keypair credentials, and explicit DATABASE,
SCHEMA and WAREHOUSE in a disposable test namespace. Creates owned resources
outside the tested request, then drops them. These tests spend warehouse time.
Run with pytest tests/smoke/test_submissions.py -v --junitxml=replay-results.xml
for recorded probe timings and outcomes. Skips never establish server deduplication.
"""

from __future__ import annotations

import inspect
import json
import os
import time
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

from snowflake_sql_api import (
    AsyncSnowflakeClient,
    QueryHandle,
    SnowflakeClient,
)
from snowflake_sql_api.escaping import quote_identifier
from snowflake_sql_api.exceptions import SnowflakeProgrammingError, SnowflakeRetryError
from snowflake_sql_api.retry import RetryPolicy

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
        or not all(os.environ.get(name) for name in REQUIRED),
        reason="requires SNOWFLAKE_LIVE_DML=1, credentials and explicit disposable namespace",
    ),
]


async def value(result):
    return await result if inspect.isawaitable(result) else result


class LostResponse:
    """Harness-only observation, never part of the worker's durable job record."""

    def __init__(self, request_id):
        self.request_id = request_id
        self.handle = None
        self.forwarded = 0
        self.received_at = None

    def intercept(self, request, response):
        if (
            request.method == "POST"
            and request.url.params.get("requestId") == self.request_id
        ):
            self.forwarded += 1
            assert response.status_code in (200, 202), response.text
            self.handle = response.json()["statementHandle"]
            self.received_at = time.monotonic()
            raise httpx.ReadError(
                "test harness discarded successful response", request=request
            )
        return response


class LoseSyncResponse(httpx.BaseTransport):
    def __init__(self, observation):
        self.inner = httpx.HTTPTransport()
        self.observation = observation

    def handle_request(self, request):
        response = self.inner.handle_request(request)
        response.read()
        return self.observation.intercept(request, response)

    def close(self):
        self.inner.close()


class LoseAsyncResponse(httpx.AsyncBaseTransport):
    def __init__(self, observation):
        self.inner = httpx.AsyncHTTPTransport()
        self.observation = observation

    async def handle_async_request(self, request):
        response = await self.inner.handle_async_request(request)
        await response.aread()
        # A fully read response has already closed its transport stream.
        return self.observation.intercept(request, response)

    async def aclose(self):
        await self.inner.aclose()


@pytest.fixture
def connection():
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
        "statement_timeout": 120,
        "parameters": {"QUERY_TAG": "replay-smoke", "USE_CACHED_RESULT": "false"},
        "poll_interval": 0.25,
    }


@asynccontextmanager
async def worker(async_mode, connection, observation=None):
    async with AsyncExitStack() as stack:
        options = dict(connection)
        if observation is not None:
            options["retry_policy"] = RetryPolicy(max_attempts=1)
            if async_mode:
                options["http_client"] = await stack.enter_async_context(
                    httpx.AsyncClient(
                        transport=LoseAsyncResponse(observation),
                        timeout=60,
                    )
                )
            else:
                options["http_client"] = stack.enter_context(
                    httpx.Client(
                        transport=LoseSyncResponse(observation),
                        timeout=60,
                    )
                )
        cls = AsyncSnowflakeClient if async_mode else SnowflakeClient
        client = cls(**options)
        if async_mode:
            await stack.enter_async_context(client)
        else:
            stack.enter_context(client)
        yield client


@pytest.fixture
async def table(connection):
    name = quote_identifier("REPLAY_PROBE_" + uuid4().hex.upper())
    with SnowflakeClient(**connection) as control:
        control.execute(f"CREATE TABLE {name} (job_id NUMBER, label VARCHAR)")
        try:
            yield name, control
        finally:
            control.execute(f"DROP TABLE {name}")


@pytest.mark.parametrize("original_state", ["completed", "running"])
async def test_lost_response_new_worker(
    async_mode, connection, table, original_state, record_property
):
    name, control = table
    procedure = quote_identifier("REPLAY_WAIT_" + uuid4().hex.upper())
    is_running = original_state == "running"
    if is_running:
        # A single CALL supplies a controllable running DML operation. Resource
        # setup is outside the tested submission and is never replayed.
        control.execute(f"""
            CREATE PROCEDURE {procedure}(job_id NUMBER, label VARCHAR)
            RETURNS VARCHAR LANGUAGE SQL EXECUTE AS CALLER AS $$
            BEGIN
                CALL SYSTEM$WAIT(20);
                INSERT INTO {name} VALUES (:job_id, :label);
                RETURN 'done';
            END;
            $$
        """)
    try:
        job = json.dumps(
            {
                "id": str(uuid4()),
                "sql": (
                    f"CALL {procedure}(?, ?)"
                    if is_running
                    else f"INSERT INTO {name} VALUES (?, ?)"
                ),
                "params": [7, "synthetic"],
            }
        )
        saved = json.loads(job)
        observation = LostResponse(saved["id"])
        record_property("started_utc", datetime.now(timezone.utc).isoformat())
        record_property("original_state", original_state)
        async with worker(async_mode, connection, observation) as first:
            identity = await value(
                first.query_one("SELECT CURRENT_ACCOUNT() AS A, CURRENT_USER() AS U")
            )
            assert identity is not None and identity["A"] and identity["U"]
            with pytest.raises(SnowflakeRetryError) as caught:
                await value(
                    first.submit(saved["sql"], saved["params"], request_id=saved["id"])
                )
            assert caught.value.request_id == saved["id"]
        assert observation.forwarded == 1
        assert observation.handle is not None
        original = QueryHandle(control, observation.handle)
        if is_running:
            assert (
                original.status() == "RUNNING"
            ), "probe did not observe the running window"
        else:
            original.result(timeout=120)
        saved = json.loads(job)
        # No original handle is supplied to this new worker.
        async with worker(async_mode, connection) as second:
            same_identity = await value(
                second.query_one("SELECT CURRENT_ACCOUNT() AS A, CURRENT_USER() AS U")
            )
            record_property("same_account_user", same_identity == identity)
            assert same_identity == identity
            replay_started = time.monotonic()
            record_property(
                "response_to_replay_seconds", replay_started - observation.received_at
            )
            replay = await value(
                second.resubmit(saved["sql"], saved["params"], request_id=saved["id"])
            )
            await value(replay.result(timeout=120))
        original.result(timeout=120)
        count = control.query_scalar(f"SELECT COUNT(*) FROM {name}")
        record_property("finished_utc", datetime.now(timezone.utc).isoformat())
        record_property("independent_row_count", count)
        assert count == 1
    finally:
        if is_running:
            control.execute(f"DROP PROCEDURE {procedure}(NUMBER, VARCHAR)")


async def test_never_sent_id(async_mode, connection, table, record_property):
    name, control = table
    saved = json.loads(
        json.dumps(
            {
                "id": str(uuid4()),
                "sql": f"INSERT INTO {name} VALUES (?, ?)",
                "params": [8, "never sent"],
            }
        )
    )
    record_property("started_utc", datetime.now(timezone.utc).isoformat())
    async with worker(async_mode, connection) as client:
        # This is the first POST for the saved ID. Never fall back to submit().
        replay = await value(
            client.resubmit(saved["sql"], saved["params"], request_id=saved["id"])
        )
        await value(replay.result(timeout=120))
    count = control.query_scalar(f"SELECT COUNT(*) FROM {name}")
    record_property("finished_utc", datetime.now(timezone.utc).isoformat())
    record_property("independent_row_count", count)
    assert count == 1


async def test_unchanged_sql_failure(async_mode, connection, table, record_property):
    name, control = table
    saved_id = str(uuid4())
    bad_sql = f"INSERT INTO {name}(missing_column) VALUES (?)"
    record_property("started_utc", datetime.now(timezone.utc).isoformat())
    for operation in ["submit", "resubmit"]:
        async with worker(async_mode, connection) as client:
            with pytest.raises(SnowflakeProgrammingError) as caught:
                handle = await value(
                    getattr(client, operation)(bad_sql, [9], request_id=saved_id)
                )
                await value(handle.result(timeout=120))
            prefix = "original" if operation == "submit" else "replay"
            record_property(f"{prefix}_error_code", caught.value.code)
            record_property(f"{prefix}_sql_state", caught.value.sql_state)
    count = control.query_scalar(f"SELECT COUNT(*) FROM {name}")
    record_property("failed_sql_row_count", count)
    assert count == 0
    async with worker(async_mode, connection) as client:
        corrected = await value(
            client.submit(
                f"INSERT INTO {name} VALUES (?, ?)",
                [9, "corrected"],
                request_id=str(uuid4()),
            )
        )
        await value(corrected.result(timeout=120))
    count = control.query_scalar(f"SELECT COUNT(*) FROM {name}")
    record_property("corrected_sql_row_count", count)
    record_property("finished_utc", datetime.now(timezone.utc).isoformat())
    assert count == 1
