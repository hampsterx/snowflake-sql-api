"""Named regression tests for the four spike-discovered bugs, plus later fixes.

Each fixed bug gets a test marked ``regression`` so ``pytest -k regression``
proves they stay fixed. Bug #2 (on_query in the streaming path) is covered by
``test_regression_bug2_stream_notifies_once_and_binds_params`` in test_streaming.py.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from snowflake_sql_api import AsyncSnowflakeClient, SnowflakeClient
from snowflake_sql_api.auth import normalize_account_locator
from snowflake_sql_api.exceptions import (
    ResultNotReady,
    SnowflakeForbiddenError,
    SnowflakeRequestError,
    SnowflakeRetryError,
    SnowflakeTimeoutError,
)
from snowflake_sql_api.testing import FakeSnowflake

from .streaming_support import call, collect, fake_client, wire_client
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
    async_mode: bool, method: str
) -> None:
    """A client-owned poll that times out cancels the statement before raising.

    Without the cancel, the client gives up but Snowflake keeps running the
    statement until the account statement timeout (two days by default).
    """
    fake = FakeSnowflake()
    fake.register(SLOW_SQL, [{"N": 1}], polls_before_ready=10)
    async with fake_client(fake, async_mode, statement_timeout=0) as client:
        with pytest.raises(SnowflakeTimeoutError) as info:
            await _run_owned(client, async_mode, method)
    assert _cancels(fake.requests) == ["/api/v2/statements/stmt-1/cancel"]
    assert info.value.cancel_error is None
    assert info.value.__cause__ is None
    assert info.value.__notes__ == ["cancellation requested for stmt-1"]


async def test_regression_poll_timeout_cancel_failure_keeps_timeout(
    async_mode: bool, private_key_pem: bytes
) -> None:
    """A failed best-effort cancel is attached to the timeout, never raised instead."""
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path.endswith("/cancel"):
            return httpx.Response(500, json={"message": "cancel unavailable"})
        return httpx.Response(202, json=running_body("slow-h"))

    async with wire_client(
        handler, private_key_pem, async_mode, statement_timeout=0
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
