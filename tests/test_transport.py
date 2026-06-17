"""Transport: status-code mapping, retry, and idempotent requestId reuse."""

from __future__ import annotations

import httpx
import pytest
import respx

from snowflake_sql_api.auth import KeypairAuthenticator
from snowflake_sql_api.exceptions import (
    SnowflakeAuthError,
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
