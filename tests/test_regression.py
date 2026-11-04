"""Named regression tests for the four spike-discovered bugs, plus later fixes.

Each fixed bug gets a test marked ``regression`` so ``pytest -k regression``
proves they stay fixed. Bug #2 (on_query in the streaming path) is covered by
``test_regression_bug2_stream_notifies_once_and_binds_params`` in test_streaming.py.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx
import pytest
import respx

from snowflake_sql_api import AsyncSnowflakeClient, SnowflakeClient
from snowflake_sql_api import aclient as aclient_module
from snowflake_sql_api.auth import normalize_account_locator
from snowflake_sql_api.exceptions import (
    ResultNotReady,
    SnowflakeConfigError,
    SnowflakeForbiddenError,
    SnowflakeProgrammingError,
    SnowflakeRequestError,
    SnowflakeRetryError,
    SnowflakeTimeoutError,
)
from snowflake_sql_api.testing import FakeSnowflake

from .streaming_support import (
    call,
    collect,
    expire_poll_deadlines,
    fake_client,
    wire_client,
)
from .support import ACCOUNT, STATEMENTS_URL, USER, ok_body, running_body, statement_url

pytestmark = pytest.mark.regression

INT_COL = [{"name": "N", "type": "fixed", "scale": 0}]

FORBIDDEN_BODY = {
    "code": "TEST403",
    "message": "SQL API is not enabled for this account",
    "sqlState": "TEST1",
    "statementHandle": "forbidden-handle",
}


def test_regression_bug1_jwt_account_locator_strips_region() -> None:
    """Bug #1: the JWT claim account must drop the region/cloud suffix.

    Leaving the region in (``XY12345.AP-SOUTHEAST-2``) makes the JWT invalid.
    """
    assert normalize_account_locator("xy12345.ap-southeast-2") == "XY12345"
    assert normalize_account_locator("xy12345.us-east-1.aws") == "XY12345"
    assert normalize_account_locator("xy12345") == "XY12345"
    # Org-account dash form is preserved (no dot to strip).
    assert normalize_account_locator("myorg-myaccount") == "MYORG-MYACCOUNT"


@respx.mock
def test_regression_bug3_result_poll_false_raises_on_202(
    private_key_pem: bytes,
) -> None:
    """Bug #3: result(poll=False) on a still-running statement signals not-ready.

    It must raise ResultNotReady rather than hand back the 202 body as a result.
    """
    handle = "still-running"
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(202, json=running_body(handle))
    )
    respx.get(statement_url(handle)).mock(
        return_value=httpx.Response(202, json=running_body(handle, code="333333"))
    )
    client = SnowflakeClient(ACCOUNT, USER, private_key=private_key_pem)
    handle_obj = client.submit("CALL long_running()")
    with pytest.raises(ResultNotReady) as info:
        handle_obj.result(poll=False)
    assert info.value.statement_handle == handle
    client.close()


@respx.mock
def test_regression_bug4_fetches_all_partitions(private_key_pem: bytes) -> None:
    """Bug #4: the main query path must fetch every partition, not just 0.

    Stopping at partition 0 silently truncates large result sets.
    """
    handle = "multi"
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(
            200, json=ok_body(INT_COL, [["0"]], partitions=3, handle=handle)
        )
    )

    def by_partition(request: httpx.Request) -> httpx.Response:
        index = request.url.params.get("partition")
        if index == "1":
            return httpx.Response(200, json={"data": [["1"], ["2"]]})
        if index == "2":
            return httpx.Response(200, json={"data": [["3"]]})
        return httpx.Response(200, json={"data": []})

    respx.get(statement_url(handle)).mock(side_effect=by_partition)

    client = SnowflakeClient(ACCOUNT, USER, private_key=private_key_pem)
    rows = client.query("SELECT n FROM big_table")
    client.close()
    # Partition 0 (inline) + partitions 1 and 2, in order, none dropped.
    assert [r["N"] for r in rows] == [0, 1, 2, 3]


def _assert_forbidden(exc: SnowflakeForbiddenError) -> None:
    # Still a SnowflakeRequestError, so existing handlers keep catching 403.
    assert isinstance(exc, SnowflakeRequestError)
    assert exc.status_code == 403
    assert exc.code == "TEST403"
    assert exc.sql_state == "TEST1"
    assert exc.statement_handle == "forbidden-handle"
    assert str(exc) == "SQL API is not enabled for this account"


@respx.mock
def test_regression_403_raises_forbidden_error(private_key_pem: bytes) -> None:
    """HTTP 403 maps to SnowflakeForbiddenError, not the generic request error.

    An authenticated-but-refused request must be distinguishable from a
    malformed one without parsing ``status_code``.
    """
    route = respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(403, json=FORBIDDEN_BODY)
    )
    client = SnowflakeClient(ACCOUNT, USER, private_key=private_key_pem)
    with pytest.raises(SnowflakeForbiddenError) as info:
        client.query("SELECT 1")
    client.close()
    _assert_forbidden(info.value)
    # 403 is not retryable: one request, no retry loop.
    assert route.call_count == 1


@respx.mock
async def test_regression_403_raises_forbidden_error_async(
    private_key_pem: bytes,
) -> None:
    """Async parity for the 403 mapping."""
    route = respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(403, json=FORBIDDEN_BODY)
    )
    client = AsyncSnowflakeClient(ACCOUNT, USER, private_key=private_key_pem)
    with pytest.raises(SnowflakeForbiddenError) as info:
        await client.query("SELECT 1")
    await client.aclose()
    _assert_forbidden(info.value)
    assert route.call_count == 1


# -- poll timeout cancels the statement the client started ---------------------

SLOW_SQL = "SELECT slow"


def _cancels(requests: list[httpx.Request]) -> list[str]:
    return [r.url.path for r in requests if r.url.path.endswith("/cancel")]


async def _run_owned(client, async_mode: bool, method: str) -> None:
    if method == "stream":
        await collect(client.query_stream(SLOW_SQL), async_mode)
    else:
        await call(client, async_mode, method, SLOW_SQL)


@pytest.mark.parametrize("method", ["query", "execute", "stream"])
async def test_regression_poll_timeout_cancels_owned_statement(
    async_mode: bool, method: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A client-owned poll that times out cancels the statement before raising.

    Without the cancel, the client gives up but Snowflake keeps running the
    statement until the account statement timeout (two days by default).
    """
    expire_poll_deadlines(monkeypatch)
    fake = FakeSnowflake()
    fake.register(SLOW_SQL, [{"N": 1}], polls_before_ready=10)
    async with fake_client(fake, async_mode, statement_timeout=1) as client:
        with pytest.raises(SnowflakeTimeoutError) as info:
            await _run_owned(client, async_mode, method)
    assert _cancels(fake.requests) == ["/api/v2/statements/stmt-1/cancel"]
    assert info.value.cancel_error is None
    assert info.value.__cause__ is None
    assert info.value.__notes__ == ["cancellation requested for stmt-1"]


async def test_regression_poll_timeout_cancel_failure_keeps_timeout(
    async_mode: bool, private_key_pem: bytes, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed best-effort cancel is attached to the timeout, never raised instead."""
    expire_poll_deadlines(monkeypatch)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/cancel"):
            return httpx.Response(500, json={"message": "cancel unavailable"})
        return httpx.Response(202, json=running_body("slow-h"))

    async with wire_client(
        handler, private_key_pem, async_mode, statement_timeout=1
    ) as client:
        with pytest.raises(SnowflakeTimeoutError) as info:
            await call(client, async_mode, "query", SLOW_SQL)
    err = info.value
    assert "did not finish" in str(err)
    assert isinstance(err.cancel_error, SnowflakeRetryError)
    assert err.__cause__ is None
    # The note must say the cancel failed, never that it was requested.
    assert len(err.__notes__) == 1
    assert err.__notes__[0].startswith("cancelling slow-h after the timeout failed:")
    # The default policy retries the 500 before giving up on the cancel.
    assert set(_cancels(requests)) == {"/api/v2/statements/slow-h/cancel"}


async def test_regression_handle_result_timeout_does_not_cancel(
    async_mode: bool,
) -> None:
    """The caller owns a submitted handle, so its poll timeout leaves it running."""
    fake = FakeSnowflake()
    fake.register(SLOW_SQL, [{"N": 1}], polls_before_ready=10)
    async with fake_client(fake, async_mode) as client:
        handle = await call(client, async_mode, "submit", SLOW_SQL)
        with pytest.raises(SnowflakeTimeoutError) as info:
            pending = handle.result(timeout=0)
            await pending if async_mode else pending
    assert _cancels(fake.requests) == []
    assert info.value.cancel_error is None


async def test_regression_poll_timeout_cancel_rejected_after_finish_keeps_timeout(
    async_mode: bool, private_key_pem: bytes, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancel Snowflake rejects because the statement already finished.

    The statement can finish between the last running poll and the cancel. The
    SQL API reference documents a 422 ``QueryFailureStatus`` for a cancel it
    cannot apply, without the exact wording for a finished statement, so this
    models a 422. The timeout is still what is raised, with the rejection
    attached, and a 422 is not retried.
    """
    expire_poll_deadlines(monkeypatch)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/cancel"):
            return httpx.Response(
                422,
                json={
                    "code": "000709",
                    "message": "Statement done-h not found",
                    "sqlState": "02000",
                    "statementHandle": "done-h",
                },
            )
        return httpx.Response(202, json=running_body("done-h"))

    async with wire_client(
        handler, private_key_pem, async_mode, statement_timeout=1
    ) as client:
        with pytest.raises(SnowflakeTimeoutError) as info:
            await call(client, async_mode, "query", SLOW_SQL)
    err = info.value
    assert "did not finish" in str(err)
    assert isinstance(err.cancel_error, SnowflakeProgrammingError)
    assert err.__cause__ is None
    assert len(err.__notes__) == 1
    assert err.__notes__[0].startswith("cancelling done-h after the timeout failed:")
    assert _cancels(requests) == ["/api/v2/statements/done-h/cancel"]


@pytest.mark.parametrize("value", [0, -1, True, False, 1.5, "30"])
@pytest.mark.parametrize("client_cls", [SnowflakeClient, AsyncSnowflakeClient])
def test_regression_statement_timeout_must_be_positive_integer(
    client_cls: Any, value: Any, private_key_pem: bytes
) -> None:
    """``statement_timeout`` of 0 or below is rejected at construction.

    The client used it as a zero-second polling deadline and cancelled any
    statement still running at the first poll, while Snowflake reads a
    server-side ``timeout`` of 0 as its maximum (604800 seconds).
    """
    with pytest.raises(SnowflakeConfigError, match="statement_timeout"):
        client_cls(ACCOUNT, USER, private_key=private_key_pem, statement_timeout=value)


@pytest.mark.parametrize("client_cls", [SnowflakeClient, AsyncSnowflakeClient])
def test_regression_statement_timeout_accepts_none_and_positive(
    client_cls: Any, private_key_pem: bytes
) -> None:
    unset = client_cls(ACCOUNT, USER, private_key=private_key_pem)
    assert "timeout" not in unset._build_payload("SELECT 1", None)
    one = client_cls(ACCOUNT, USER, private_key=private_key_pem, statement_timeout=1)
    assert one._build_payload("SELECT 1", None)["timeout"] == 1


async def test_regression_poll_deadline_ignores_wall_clock_steps(
    async_mode: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A wall-clock step does not cut a poll short.

    Every ``time.time()`` reading here lands a day after the last, as a clock
    correction would. A deadline taken from the wall clock times out at the
    first running poll; the monotonic deadline waits for the result.
    """
    wall = iter(range(0, 10**9, 86_400))
    monkeypatch.setattr(time, "time", lambda: float(next(wall)))
    fake = FakeSnowflake()
    fake.register(SLOW_SQL, [{"N": 1}], polls_before_ready=2)
    async with fake_client(fake, async_mode, statement_timeout=60) as client:
        assert await call(client, async_mode, "query", SLOW_SQL) == [{"N": 1}]
    assert _cancels(fake.requests) == []


# -- async task cancellation and sync interruption -------------------------


async def _until(predicate: Any, what: str) -> None:
    for _ in range(10_000):
        if predicate():
            return
        await asyncio.sleep(0)
    pytest.fail(f"never saw {what}")


async def _first_poll(requests: list[httpx.Request]) -> None:
    await _until(lambda: any(r.method == "GET" for r in requests), "a status poll")


def _running_async_handler(requests: list[httpx.Request], on_cancel: Any = None) -> Any:
    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/cancel"):
            if on_cancel is not None:
                await on_cancel()
            return httpx.Response(200, json={"statementHandle": "slow-h"})
        return httpx.Response(202, json=running_body("slow-h"))

    return handler


@pytest.mark.parametrize("method", ["query", "execute", "stream"])
async def test_regression_async_task_cancel_cancels_owned_statement(
    method: str, private_key_pem: bytes
) -> None:
    """Cancelling the task awaiting a client-owned poll cancels the statement.

    Without it the ``CancelledError`` unwinds the poll and Snowflake keeps
    running a statement nobody holds a handle to.
    """
    requests: list[httpx.Request] = []
    async with wire_client(
        _running_async_handler(requests), private_key_pem, True
    ) as client:
        task = asyncio.ensure_future(_run_owned(client, True, method))
        await _first_poll(requests)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert task.cancelled()
    assert _cancels(requests) == ["/api/v2/statements/slow-h/cancel"]


async def test_regression_async_timeout_context_still_raises_timeout(
    private_key_pem: bytes,
) -> None:
    """The ``CancelledError`` is re-raised unchanged, so ``asyncio.timeout`` works.

    An enclosing ``asyncio.timeout`` turns its own cancellation into
    ``TimeoutError`` only when that cancellation comes back out of the block.
    """
    requests: list[httpx.Request] = []
    async with wire_client(
        _running_async_handler(requests), private_key_pem, True, poll_interval=0.01
    ) as client:
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.05):
                await client.query(SLOW_SQL)
    assert _cancels(requests) == ["/api/v2/statements/slow-h/cancel"]


async def test_regression_async_task_cancel_bounds_a_hanging_cancel(
    private_key_pem: bytes, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cancel request that never answers delays the cancellation only briefly."""
    monkeypatch.setattr(aclient_module, "CANCEL_ON_TASK_CANCEL_TIMEOUT", 0.01)
    requests: list[httpx.Request] = []
    async with wire_client(
        _running_async_handler(requests, on_cancel=asyncio.Event().wait),
        private_key_pem,
        True,
    ) as client:
        task = asyncio.ensure_future(client.query(SLOW_SQL))
        await _first_poll(requests)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
    assert _cancels(requests) == ["/api/v2/statements/slow-h/cancel"]


async def test_regression_async_task_cancel_survives_a_second_cancel(
    private_key_pem: bytes,
) -> None:
    """A second task cancel abandons neither the cancel request nor its retry.

    The client context lives inside the cancelled task, as it does for a caller
    using ``async with``: returning from the cleanup early would close the HTTP
    client before the retried cancel is sent, leaving the statement running.
    """
    requests: list[httpx.Request] = []
    release = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/cancel"):
            if len(_cancels(requests)) == 1:
                await release.wait()
                return httpx.Response(503, json={"message": "busy"})
            return httpx.Response(200, json={"statementHandle": "slow-h"})
        return httpx.Response(202, json=running_body("slow-h"))

    async def owned_query() -> None:
        async with wire_client(handler, private_key_pem, True) as client:
            await client.query(SLOW_SQL)

    task = asyncio.ensure_future(owned_query())
    await _first_poll(requests)
    task.cancel()
    await _until(lambda: _cancels(requests), "the cancel request")
    task.cancel()
    for _ in range(100):
        await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert _cancels(requests) == ["/api/v2/statements/slow-h/cancel"] * 2


async def test_regression_async_task_cancel_during_timeout_cancel_resends(
    private_key_pem: bytes, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A task cancelled while the timeout's own cancel is in flight sends it again.

    The cancellation cuts that request off, so without the resend the statement
    keeps running exactly as if no cancel had been attempted.
    """
    expire_poll_deadlines(monkeypatch)
    requests: list[httpx.Request] = []

    async def on_cancel() -> None:
        if len(_cancels(requests)) == 1:
            await asyncio.Event().wait()  # held until the task cancel cuts it off

    async with wire_client(
        _running_async_handler(requests, on_cancel=on_cancel),
        private_key_pem,
        True,
        statement_timeout=1,
    ) as client:
        task = asyncio.ensure_future(client.query(SLOW_SQL))
        await _until(lambda: _cancels(requests), "the timeout's cancel request")
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
    assert _cancels(requests) == ["/api/v2/statements/slow-h/cancel"] * 2


async def test_regression_async_handle_result_task_cancel_does_not_cancel(
    private_key_pem: bytes,
) -> None:
    """A caller-held handle stays running when the task polling it is cancelled."""
    requests: list[httpx.Request] = []
    async with wire_client(
        _running_async_handler(requests), private_key_pem, True
    ) as client:
        handle = await client.submit(SLOW_SQL)
        task = asyncio.ensure_future(handle.result())
        await _first_poll(requests)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert _cancels(requests) == []


def test_regression_sync_keyboard_interrupt_does_not_cancel(
    private_key_pem: bytes,
) -> None:
    """A sync ``KeyboardInterrupt`` while polling propagates without a cancel.

    The async client cancels on task cancellation; the sync client does not
    make an HTTP call while the process is being interrupted, since it can hang.
    """
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            raise KeyboardInterrupt
        return httpx.Response(202, json=running_body("slow-h"))

    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as http,
        SnowflakeClient(
            ACCOUNT, USER, private_key=private_key_pem, http_client=http
        ) as client,
        pytest.raises(KeyboardInterrupt),
    ):
        client.query(SLOW_SQL)
    assert _cancels(requests) == []


@pytest.mark.parametrize("helper", ["insert_many", "insert_batches"])
async def test_regression_write_helpers_quote_identifiers_case_exact(
    async_mode: bool, helper: str
) -> None:
    """Write helpers quote table and column names exactly as given.

    Snowflake stores an unquoted identifier in uppercase, so the documented
    examples use uppercase names. A lowercase name must stay lowercase (quoted,
    never folded): folding would silently change which object a quoted,
    lowercase-created table resolves to.
    """
    import json

    from snowflake_sql_api.testing import (
        FakeSnowflake,
        make_async_client,
        make_client,
    )

    upper = 'INSERT INTO "EVENTS" ("ID", "LABEL") VALUES (?, ?)'
    lower = 'INSERT INTO "events" ("id", "label") VALUES (?, ?)'
    fake = FakeSnowflake().register_dml(upper, 1).register_dml(lower, 1)
    rows = [(1, "a")]
    if async_mode:
        async with make_async_client(fake) as aclient:
            await getattr(aclient, helper)("EVENTS", ["ID", "LABEL"], rows)
            await getattr(aclient, helper)("events", ["id", "label"], rows)
    else:
        with make_client(fake) as client:
            getattr(client, helper)("EVENTS", ["ID", "LABEL"], rows)
            getattr(client, helper)("events", ["id", "label"], rows)
    sent = [
        json.loads(request.content)["statement"]
        for request in fake.requests
        if request.method == "POST"
    ]
    assert sent == [upper, lower]
