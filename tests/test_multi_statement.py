"""Paired HTTP-level contracts for ordered SQL batches and transaction wrappers."""

from __future__ import annotations

import inspect
import json
import pickle
from dataclasses import FrozenInstanceError
from decimal import Decimal

import httpx
import pytest

from snowflake_sql_api import (
    AsyncMultiStatementHandle,
    MultiStatementHandle,
    ResultNotReady,
    RetryPolicy,
    SnowflakeConfigError,
    SnowflakeMultiStatementError,
    SnowflakeRequestError,
    SnowflakeTimeoutError,
)
from snowflake_sql_api.testing import FakeSnowflake

from .streaming_support import fake_client, wire_client
from .support import ok_body, running_body


async def invoke(target, method, *args, **kwargs):
    result = getattr(target, method)(*args, **kwargs)
    return await result if inspect.isawaitable(result) else result


def attach(client, async_mode, parent="parent", count=2):
    cls = AsyncMultiStatementHandle if async_mode else MultiStatementHandle
    return cls(client, parent, expected_count=count)


def result_body(handle, value="1"):
    return ok_body(
        [{"name": "N", "type": "fixed", "scale": 0}], [[value]], handle=handle
    )


@pytest.mark.asyncio
async def test_payload_order_partitions_coercion_and_configuration(async_mode):
    fake = FakeSnowflake()
    entries = ["SELECT ';' -- delimiter in a comment;", "SELECT amount FROM totals"]
    fake.register(entries[0], [{"N": 1}, {"N": 2}, {"N": 3}], partitions=3)
    fake.register(entries[1], [{"N": Decimal("1.25")}])
    fake.register_multi(entries)
    calls = []
    parameters = {
        "multi_statement_count": "99",
        "Multi_Statement_Count": "0",
        "QUERY_TAG": "batch-test",
    }
    async with fake_client(
        fake, async_mode, parameters=parameters, on_query=lambda *a: calls.append(a)
    ) as client:
        result = await invoke(
            client, "execute_statements", [" " + entries[0] + " ", entries[1]]
        )
        assert [r.index for r in result.results] == [0, 1]
        assert result.results[0].rows == [{"N": 1}, {"N": 2}, {"N": 3}]
        assert result.results[1].rows == [{"N": Decimal("1.25")}]
        assert result.results[1].columns[0].scale == 2
        assert len({r.statement_handle for r in result.results}) == 2
        assert result.statement_handle not in [
            r.statement_handle for r in result.results
        ]
        assert isinstance(result.results, tuple)
        with pytest.raises(FrozenInstanceError):
            result.results = ()
        payload = json.loads(fake.requests[0].content)
        assert payload["statement"] == "\n;\n".join(entries)
        assert payload["parameters"] == {
            "MULTI_STATEMENT_COUNT": "2",
            "QUERY_TAG": "batch-test",
        }
        assert "bindings" not in payload
        assert calls == [(payload["statement"], None)]
        assert client._parameters == parameters
        assert client._build_payload("SELECT 1", None)["parameters"] == parameters
        assert [
            r.url.params["partition"]
            for r in fake.requests
            if "partition" in r.url.params
        ] == ["1", "2"]
        assert sum(r.method == "POST" for r in fake.requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entries", [[], "SELECT 1", b"SELECT 1", [None], [" "], (x for x in ["SELECT 1"])]
)
async def test_invalid_sequence_fails_before_hook_or_http(async_mode, entries):
    fake = FakeSnowflake()
    calls = []
    async with fake_client(
        fake, async_mode, on_query=lambda *a: calls.append(a)
    ) as client:
        with pytest.raises(SnowflakeConfigError):
            await invoke(client, "execute_statements", entries)
    assert not fake.requests and not calls


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["AUTOCOMMIT", "autocommit", "AutoCommit"])
@pytest.mark.parametrize(
    "method", ["execute_statements", "submit_statements", "execute_transaction"]
)
async def test_autocommit_rejected_locally(async_mode, key, method):
    fake = FakeSnowflake()
    calls = []
    async with fake_client(
        fake, async_mode, parameters={key: True}, on_query=lambda *a: calls.append(a)
    ) as client:
        with pytest.raises(SnowflakeConfigError, match="AUTOCOMMIT"):
            await invoke(client, method, ["SELECT 1"])
    assert not fake.requests and not calls


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, -1, True, 1.5, "2"])
async def test_attachment_count_validates_without_http(async_mode, count):
    fake = FakeSnowflake()
    async with fake_client(fake, async_mode) as client:
        with pytest.raises(SnowflakeConfigError):
            attach(client, async_mode, count=count)
    assert not fake.requests


@pytest.mark.asyncio
@pytest.mark.parametrize("parent", ["", " ", None, 12])
async def test_attachment_parent_validates_without_http(async_mode, parent):
    fake = FakeSnowflake()
    async with fake_client(fake, async_mode) as client:
        with pytest.raises(SnowflakeConfigError):
            attach(client, async_mode, parent=parent)
    assert not fake.requests


@pytest.mark.asyncio
@pytest.mark.parametrize("submit", [False, True])
async def test_single_entry_uses_parent_result_and_every_partition(async_mode, submit):
    fake = FakeSnowflake().register("SELECT 1", [{"N": 1}, {"N": 2}], partitions=2)
    fake.register_multi(["SELECT 1"])
    async with fake_client(fake, async_mode) as client:
        if submit:
            handle = await invoke(client, "submit_statements", ["SELECT 1"])
            result = await invoke(handle, "result")
        else:
            result = await invoke(client, "execute_statements", ["SELECT 1"])
    assert len(result.results) == 1
    assert result.results[0].statement_handle == result.statement_handle
    assert result.results[0].rows == [{"N": 1}, {"N": 2}]
    assert (
        json.loads(fake.requests[0].content)["parameters"]["MULTI_STATEMENT_COUNT"]
        == "1"
    )
    assert len(fake.requests) == (3 if submit else 2)


@pytest.mark.asyncio
async def test_submit_parent_only_attach_status_cancel_and_get_only_recovery(
    async_mode,
):
    fake = (
        FakeSnowflake()
        .register("SELECT 1", [{"N": 1}])
        .register("SELECT 2", [{"N": 2}])
    )
    fake.register_multi(["SELECT 1", "SELECT 2"])
    hooks = []
    async with fake_client(
        fake, async_mode, on_query=lambda *a: hooks.append(a)
    ) as client:
        original = await invoke(client, "submit_statements", ["SELECT 1", "SELECT 2"])
        assert fake.requests[0].url.params["async"] == "true"
        saved = attach(
            client, async_mode, original.statement_handle, original.expected_count
        )
        assert len(fake.requests) == 1
        assert await invoke(saved, "status") == "SUCCESS"
        result = await invoke(saved, "result")
        assert [r.rows for r in result.results] == [[{"N": 1}], [{"N": 2}]]
        await invoke(saved, "cancel")
    assert len(hooks) == 1
    assert (
        len(
            [
                r
                for r in fake.requests
                if r.method == "POST" and not r.url.path.endswith("/cancel")
            ]
        )
        == 1
    )
    assert fake.requests[-1].url.path.endswith(
        "/" + original.statement_handle + "/cancel"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "children",
    [None, [], ["one"], ["one", ""], ["one", 2], "one,two", ["one", "two", "three"]],
)
async def test_malformed_success_metadata_is_protocol_failure(
    async_mode, private_key_pem, children
):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            200, json={"statementHandle": "parent", "statementHandles": children}
        )

    async with wire_client(handler, private_key_pem, async_mode) as client:
        with pytest.raises(SnowflakeMultiStatementError) as caught:
            await invoke(client, "execute_statements", ["SELECT 1", "SELECT 2"])
    error = caught.value
    assert error.phase == "execution" and error.outcome == "unknown"
    assert isinstance(error.__cause__, SnowflakeRequestError)
    assert error.known_statement_handles == () and error.failed_index is None
    assert len(requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("child_running", [False, True])
async def test_poll_false_preserves_relevant_handle(
    async_mode, private_key_pem, child_running
):
    def handler(request):
        if child_running and request.url.path.endswith("/parent"):
            return httpx.Response(
                200,
                json={"statementHandle": "parent", "statementHandles": ["one", "two"]},
            )
        return httpx.Response(
            202, json=running_body("one" if child_running else "parent")
        )

    async with wire_client(handler, private_key_pem, async_mode) as client:
        with pytest.raises(ResultNotReady) as caught:
            await invoke(attach(client, async_mode), "result", poll=False)
    assert caught.value.statement_handle == ("one" if child_running else "parent")


@pytest.mark.asyncio
async def test_combined_polling_budget(async_mode, private_key_pem, monkeypatch):
    clock = [100.0]
    monkeypatch.setattr("snowflake_sql_api.multi_statement.monotonic", lambda: clock[0])
    budgets = []

    def handler(request):
        return httpx.Response(
            202, json=running_body(request.url.path.rsplit("/", 1)[-1])
        )

    async with wire_client(handler, private_key_pem, async_mode) as client:

        def poll(handle, timeout, **kwargs):
            budgets.append(timeout)
            clock[0] += 3
            body = (
                {"statementHandle": "parent", "statementHandles": ["one", "two"]}
                if handle == "parent"
                else result_body(handle)
            )
            from snowflake_sql_api.transport import StatementResponse

            return StatementResponse(200, body)

        async def apoll(*args, **kwargs):
            return poll(*args, **kwargs)

        monkeypatch.setattr(client, "_poll", apoll if async_mode else poll)
        result = await invoke(attach(client, async_mode), "result", timeout=10)
    assert budgets == [10, 7, 4]
    assert len(result.results) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("owned", [False, True])
async def test_parent_timeout_unknown_and_owner_controls_cancel(
    async_mode, private_key_pem, monkeypatch, owned
):
    from .streaming_support import expire_poll_deadlines

    expire_poll_deadlines(monkeypatch)
    requests = []

    def handler(request):
        requests.append(request)
        if request.url.path.endswith("/cancel"):
            return httpx.Response(200, json={})
        return httpx.Response(202, json=running_body("parent"))

    async with wire_client(
        handler, private_key_pem, async_mode, statement_timeout=1
    ) as client:
        with pytest.raises(SnowflakeMultiStatementError) as caught:
            if owned:
                await invoke(client, "execute_statements", ["SELECT 1", "SELECT 2"])
            else:
                await invoke(attach(client, async_mode), "result", timeout=0)
    assert caught.value.outcome == "unknown"
    assert isinstance(caught.value.__cause__, SnowflakeTimeoutError)
    assert caught.value.failed_index is None
    assert sum(r.url.path.endswith("/cancel") for r in requests) == int(owned)


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_timeout", [False, True])
@pytest.mark.parametrize("status", [400, 401, 403, 422, 404])
async def test_submission_outcome_retains_attempt_history(
    async_mode, private_key_pem, prior_timeout, status
):
    requests = []

    def handler(request):
        requests.append(request)
        if prior_timeout and len(requests) == 1:
            raise httpx.ReadTimeout("lost response", request=request)
        return httpx.Response(
            status,
            json={
                "message": "rejected",
                "statementHandle": "parent",
                "code": "bad",
                "sqlState": "42000",
                "statementHandles": ["one"],
            },
        )

    async with wire_client(handler, private_key_pem, async_mode) as client:
        with pytest.raises(SnowflakeMultiStatementError) as caught:
            await invoke(client, "execute_statements", ["SELECT 1", "SELECT 2"])
    error = caught.value
    assert error.submission_attempts == (2 if prior_timeout else 1)
    assert error.outcome == (
        "unknown"
        if prior_timeout or status == 404
        else "failed" if status == 422 else "not_executed"
    )
    assert error.phase == ("execution" if status == 422 else "submission")
    assert error.statement_handle == "parent" and error.expected_count == 2
    assert (
        error.code == "bad"
        and error.sql_state == "42000"
        and error.status_code == status
    )
    assert error.known_statement_handles == (("one",) if status == 422 else ())
    assert error.completed_results == () and error.failed_index is None
    if prior_timeout:
        assert (
            requests[0].url.params["requestId"] == requests[1].url.params["requestId"]
        )
        assert requests[1].url.params["retry"] == "true"
        assert requests[0].content == requests[1].content
    restored = pickle.loads(pickle.dumps(error))
    assert restored.__dict__ == error.__dict__


@pytest.mark.asyncio
async def test_result_fetch_failure_reports_prefix_and_never_reposts(
    async_mode, private_key_pem
):
    requests = []
    fail = [True]

    def handler(request):
        requests.append(request)
        if request.method == "POST" or request.url.path.endswith("/parent"):
            return httpx.Response(
                200,
                json={"statementHandle": "parent", "statementHandles": ["one", "two"]},
            )
        if request.url.path.endswith("/two") and fail[0]:
            return httpx.Response(503, json={"message": "download unavailable"})
        return httpx.Response(
            200, json=result_body(request.url.path.rsplit("/", 1)[-1])
        )

    async with wire_client(
        handler,
        private_key_pem,
        async_mode,
        retry_policy=RetryPolicy(max_attempts=2, base_backoff=0),
    ) as client:
        with pytest.raises(SnowflakeMultiStatementError) as caught:
            await invoke(client, "execute_statements", ["SELECT 1", "SELECT 2"])
        error = caught.value
        assert error.phase == "result_fetch" and error.outcome == "succeeded"
        assert error.known_statement_handles == ("one", "two")
        assert error.failed_index == 1 and error.submission_attempts == 1
        assert [r.rows for r in error.completed_results] == [[{"N": 1}]]
        fail[0] = False
        result = await invoke(
            attach(client, async_mode, error.statement_handle, error.expected_count),
            "result",
        )
        assert len(result.results) == 2
    assert sum(r.method == "POST" for r in requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata", [None, "one", [None], [""], ["one", 2], ["one", "two"]]
)
async def test_parent_poll_422_retains_only_valid_error_handles(
    async_mode, private_key_pem, metadata
):
    def handler(request):
        if request.method == "POST":
            return httpx.Response(202, json=running_body("parent"))
        return httpx.Response(
            422, json={"statementHandle": "parent", "statementHandles": metadata}
        )

    async with wire_client(handler, private_key_pem, async_mode) as client:
        with pytest.raises(SnowflakeMultiStatementError) as caught:
            await invoke(client, "execute_statements", ["SELECT 1", "SELECT 2"])
    error = caught.value
    assert error.phase == "execution" and error.outcome == "failed"
    assert error.failed_index is None and error.completed_results == ()
    assert error.known_statement_handles == (
        ("one", "two") if metadata == ["one", "two"] else ()
    )


@pytest.mark.asyncio
async def test_transaction_one_post_exact_count_and_wrapped_results(async_mode):
    body = ["-- prefix\r\n/* comment */ INSERT INTO events SELECT 1", "SELECT 1"]
    entries = ["BEGIN TRANSACTION", *body, "COMMIT"]
    fake = FakeSnowflake()
    for sql in entries:
        fake.register(sql, [{"VALUE": "ok"}])
    fake.register_multi(entries)
    async with fake_client(fake, async_mode) as client:
        result = await invoke(client, "execute_transaction", body)
    assert [r.index for r in result.results] == [0, 1, 2, 3]
    posts = [r for r in fake.requests if r.method == "POST"]
    assert len(posts) == 1
    payload = json.loads(posts[0].content)
    assert payload["statement"] == "\n;\n".join(entries)
    assert payload["parameters"]["MULTI_STATEMENT_COUNT"] == "4"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        [],
        ["-- only"],
        ["/* only */"],
        ["/* unclosed"],
        ["/* nested /* */ SELECT 1"],
        ["// nope\nSELECT 1"],
        ["WITH x AS (SELECT 1) SELECT * FROM x"],
        ["CREATE TABLE x (id INT)"],
        ["CALL p()"],
        ["EXECUTE IMMEDIATE $$SELECT 1$$"],
        ["ALTER SESSION SET x=1"],
        ["BEGIN"],
        ["COMMIT"],
        ["ROLLBACK"],
        ["TRUNCATE TABLE x"],
    ],
)
async def test_transaction_disallowed_forms_fail_before_notifications(async_mode, body):
    fake = FakeSnowflake()
    notifications = []
    async with fake_client(
        fake, async_mode, on_query=lambda *a: notifications.append(a)
    ) as client:
        with pytest.raises(SnowflakeConfigError):
            await invoke(client, "execute_transaction", body)
    assert not fake.requests and not notifications


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "keyword",
    [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET n=1",
        "DELETE FROM t",
        "MERGE INTO t USING s ON t.n=s.n WHEN MATCHED THEN DELETE",
        "SELECT 1",
    ],
)
async def test_transaction_allowed_leading_keywords(async_mode, keyword):
    entries = ["BEGIN TRANSACTION", "\t/* first */-- second\n" + keyword, "COMMIT"]
    fake = FakeSnowflake()
    for sql in entries:
        fake.register(sql.strip(), [])
    fake.register_multi(entries)
    async with fake_client(fake, async_mode) as client:
        assert (
            len((await invoke(client, "execute_transaction", [entries[1]])).results)
            == 3
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("submit", [False, True])
async def test_missing_parent_and_local_signing_failure(
    async_mode, private_key_pem, monkeypatch, submit
):
    def handler(request):
        return httpx.Response(202, json={"message": "no parent"})

    async with wire_client(handler, private_key_pem, async_mode) as client:
        method = "submit_statements" if submit else "execute_statements"
        with pytest.raises(SnowflakeMultiStatementError) as caught:
            await invoke(client, method, ["SELECT 1", "SELECT 2"])
        assert caught.value.statement_handle is None
        assert caught.value.outcome == "unknown"
        assert caught.value.submission_attempts == 1

        def fail_signing():
            raise ValueError("local signing fault")

        monkeypatch.setattr(client._auth, "token", fail_signing)
        with pytest.raises(SnowflakeMultiStatementError) as caught:
            await invoke(client, method, ["SELECT 1", "SELECT 2"])
        assert caught.value.outcome == "not_executed"
        assert caught.value.submission_attempts == 0


@pytest.mark.asyncio
async def test_multi_has_no_binding_api_and_parent_management_keeps_error_types(
    async_mode, private_key_pem
):
    def handler(request):
        return httpx.Response(403, json={"message": "forbidden"})

    async with wire_client(handler, private_key_pem, async_mode) as client:
        with pytest.raises(TypeError):
            await invoke(client, "execute_statements", ["SELECT ?"], params=[1])
        for method in ("status", "cancel"):
            with pytest.raises(SnowflakeRequestError):
                await invoke(attach(client, async_mode), method)
