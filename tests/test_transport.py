"""Transport: status-code mapping, retry, and idempotent requestId reuse."""

from __future__ import annotations

import httpx
import pytest
import respx

from snowflake_sql_api.auth import KeypairAuthenticator
from snowflake_sql_api.exceptions import (
    SnowflakeAuthError,
    SnowflakeForbiddenError,
    SnowflakeProgrammingError,
    SnowflakeRequestError,
    SnowflakeRetryError,
)
from snowflake_sql_api.retry import RetryPolicy
from snowflake_sql_api.transport import Transport

from .support import ACCOUNT, HOST, STATEMENTS_URL, USER, ok_body

FAST_RETRY = RetryPolicy(max_attempts=3, base_backoff=0.0, jitter=False)


def make_transport(
    private_key_pem: bytes, retry: RetryPolicy = FAST_RETRY
) -> Transport:
    auth = KeypairAuthenticator(ACCOUNT, USER, private_key_pem)
    return Transport(HOST, auth, retry_policy=retry)


@respx.mock
def test_success_returns_statement_response(private_key_pem: bytes) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body([], []))
    )
    transport = make_transport(private_key_pem)
    resp = transport.submit({"statement": "SELECT 1"}, request_id="r1")
    assert resp.status_code == 200
    assert not resp.is_running


@respx.mock
def test_202_is_running(private_key_pem: bytes) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(202, json={"statementHandle": "h"})
    )
    transport = make_transport(private_key_pem)
    resp = transport.submit({"statement": "x"}, request_id="r1", async_exec=True)
    assert resp.is_running
    assert resp.statement_handle == "h"


@respx.mock
def test_422_raises_programming_error(private_key_pem: bytes) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(
            422,
            json={
                "code": "000904",
                "message": "SQL compilation error",
                "sqlState": "42000",
                "statementHandle": "h",
            },
        )
    )
    transport = make_transport(private_key_pem)
    with pytest.raises(SnowflakeProgrammingError) as info:
        transport.submit({"statement": "bad"}, request_id="r1")
    assert info.value.status_code == 422
    assert info.value.code == "000904"
    assert info.value.sql_state == "42000"


@respx.mock
def test_401_raises_auth_error(private_key_pem: bytes) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(401, json={"message": "bad token"})
    )
    transport = make_transport(private_key_pem)
    with pytest.raises(SnowflakeAuthError):
        transport.submit({"statement": "x"}, request_id="r1")


@respx.mock
def test_other_4xx_raises_request_error(private_key_pem: bytes) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(400, json={"message": "malformed"})
    )
    transport = make_transport(private_key_pem)
    with pytest.raises(SnowflakeRequestError):
        transport.submit({"statement": "x"}, request_id="r1")


@respx.mock
def test_retry_on_429_then_success(private_key_pem: bytes) -> None:
    route = respx.post(STATEMENTS_URL).mock(
        side_effect=[
            httpx.Response(429, json={"message": "slow down"}),
            httpx.Response(200, json=ok_body([], [])),
        ]
    )
    transport = make_transport(private_key_pem)
    resp = transport.submit({"statement": "x"}, request_id="rid-1")
    assert resp.status_code == 200
    assert route.call_count == 2


@respx.mock
def test_idempotent_retry_reuses_request_id(private_key_pem: bytes) -> None:
    route = respx.post(STATEMENTS_URL).mock(
        side_effect=[
            httpx.Response(503, json={"message": "down"}),
            httpx.Response(200, json=ok_body([], [])),
        ]
    )
    transport = make_transport(private_key_pem)
    transport.submit({"statement": "INSERT INTO t VALUES (1)"}, request_id="rid-1")
    first, second = route.calls[0].request, route.calls[1].request
    # Same requestId on the retry, plus retry=true so Snowflake deduplicates.
    assert first.url.params["requestId"] == "rid-1"
    assert second.url.params["requestId"] == "rid-1"
    assert "retry" not in first.url.params
    assert second.url.params["retry"] == "true"


@respx.mock
def test_retry_on_408_request_timeout(private_key_pem: bytes) -> None:
    route = respx.post(STATEMENTS_URL).mock(
        side_effect=[
            httpx.Response(408, json={"message": "request timeout"}),
            httpx.Response(200, json=ok_body([], [])),
        ]
    )
    transport = make_transport(private_key_pem)
    resp = transport.submit({"statement": "x"}, request_id="r1")
    assert resp.status_code == 200
    assert route.call_count == 2


@respx.mock
def test_retry_exhaustion_raises_retry_error(private_key_pem: bytes) -> None:
    route = respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(503, json={"message": "down"})
    )
    transport = make_transport(private_key_pem)
    with pytest.raises(SnowflakeRetryError):
        transport.submit({"statement": "x"}, request_id="r1")
    assert route.call_count == 3  # max_attempts


@respx.mock
def test_custom_policy_retrying_403_chains_plain_request_error(
    private_key_pem: bytes,
) -> None:
    # Retried statuses never reach _interpret, so an exhausted 403 retry carries
    # the generic SnowflakeRequestError as its cause, like every other status.
    route = respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(403, json={"message": "forbidden"})
    )
    retry_403 = RetryPolicy(
        max_attempts=2, base_backoff=0.0, jitter=False, retry_statuses=frozenset({403})
    )
    transport = make_transport(private_key_pem, retry=retry_403)
    with pytest.raises(SnowflakeRetryError) as info:
        transport.submit({"statement": "x"}, request_id="r1")
    cause = info.value.__cause__
    assert isinstance(cause, SnowflakeRequestError)
    assert not isinstance(cause, SnowflakeForbiddenError)
    assert cause.status_code == 403
    assert route.call_count == 2


@respx.mock
def test_network_error_is_retried(private_key_pem: bytes) -> None:
    route = respx.post(STATEMENTS_URL).mock(
        side_effect=[
            httpx.ConnectError("boom"),
            httpx.Response(200, json=ok_body([], [])),
        ]
    )
    transport = make_transport(private_key_pem)
    resp = transport.submit({"statement": "x"}, request_id="r1")
    assert resp.status_code == 200
    assert route.call_count == 2


@respx.mock
def test_headers_carry_jwt_and_token_type(private_key_pem: bytes) -> None:
    route = respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body([], []))
    )
    transport = make_transport(private_key_pem)
    transport.submit({"statement": "x"}, request_id="r1")
    headers = route.calls[0].request.headers
    assert headers["authorization"].startswith("Bearer ")
    assert headers["x-snowflake-authorization-token-type"] == "KEYPAIR_JWT"


@pytest.mark.parametrize(
    "failures, delays",
    [
        ([(429, {})], [5.0]),
        ([(503, {})], [0.5]),
        ([(429, {"Retry-After": "12"})], [12.0]),
        ([(503, {"Retry-After": "999"})], [60.0]),
        ([(429, {"Retry-After": "invalid"})], [5.0]),
        ([(429, {"Retry-After": "0"})], [0.0]),
        ([(429, {"Retry-After": "12"}), (503, {})], [12.0, 1.0]),
        ([(429, {"Retry-After": "12"}), None], [12.0, 1.0]),
        ([(429, {}), (429, {})], [5.0, 8.0]),
    ],
)
@pytest.mark.parametrize("operation", ["submit", "get_statement", "cancel"])
async def test_regression_retry_after_and_status_schedule(
    async_mode: bool,
    failures: list,
    delays: list[float],
    operation: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from snowflake_sql_api.transport import AsyncTransport

    sleep = AsyncMock() if async_mode else Mock()
    monkeypatch.setattr(
        (
            "snowflake_sql_api.transport.asyncio.sleep"
            if async_mode
            else "snowflake_sql_api.transport.time.sleep"
        ),
        sleep,
    )
    requests = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        index = len(requests) - 1
        if index >= len(failures):
            return httpx.Response(200, json=ok_body([], []))
        failure = failures[index]
        if failure is None:
            raise httpx.ConnectError("connection failed", request=request)
        status, headers = failure
        return httpx.Response(status, headers=headers, json={"message": "retry"})

    http_client = (httpx.AsyncClient if async_mode else httpx.Client)(
        transport=httpx.MockTransport(handle)
    )
    transport = (AsyncTransport if async_mode else Transport)(
        HOST,
        SimpleNamespace(token=lambda: "test-token"),
        client=http_client,
        retry_policy=RetryPolicy(jitter=False),
    )
    try:
        if operation == "submit":
            result = transport.submit({"statement": "select 1"}, request_id="stable-id")
        else:
            result = getattr(transport, operation)("handle")
        if async_mode:
            result = await result
        assert result.status_code == 200
        assert [call.args[0] for call in sleep.call_args_list] == delays
        if async_mode:
            assert sleep.await_count == len(delays)
        assert len(requests) == len(failures) + 1
        if operation == "submit":
            assert result.submission_attempts == len(requests)
            assert all(r.url.params["requestId"] == "stable-id" for r in requests)
            assert "retry" not in requests[0].url.params
            assert all(r.url.params["retry"] == "true" for r in requests[1:])
    finally:
        if async_mode:
            await http_client.aclose()
        else:
            http_client.close()


@pytest.mark.parametrize("status", [200, 400, 429])
async def test_retry_after_only_sleeps_when_another_attempt_will_run(
    async_mode: bool, status: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from snowflake_sql_api.transport import AsyncTransport

    sleep = AsyncMock() if async_mode else Mock()
    monkeypatch.setattr(
        (
            "snowflake_sql_api.transport.asyncio.sleep"
            if async_mode
            else "snowflake_sql_api.transport.time.sleep"
        ),
        sleep,
    )

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status, headers={"Retry-After": "10"}, json=ok_body([], [])
        )

    http_client = (httpx.AsyncClient if async_mode else httpx.Client)(
        transport=httpx.MockTransport(handle)
    )
    transport = (AsyncTransport if async_mode else Transport)(
        HOST,
        SimpleNamespace(token=lambda: "test-token"),
        client=http_client,
        retry_policy=RetryPolicy(max_attempts=1),
    )
    try:

        async def submit():
            result = transport.submit({"statement": "select 1"}, request_id="r")
            return await result if async_mode else result

        if status == 200:
            assert (await submit()).status_code == 200
        else:
            expected = SnowflakeRetryError if status == 429 else SnowflakeRequestError
            with pytest.raises(expected):
                await submit()
        sleep.assert_not_called()
    finally:
        if async_mode:
            await http_client.aclose()
        else:
            http_client.close()
