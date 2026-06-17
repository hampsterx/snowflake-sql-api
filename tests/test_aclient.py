"""Async client parity over a mocked SQL API."""

from __future__ import annotations

from typing import AsyncIterator

import httpx
import pytest
import respx

from snowflake_sql_api import AsyncSnowflakeClient
from snowflake_sql_api.exceptions import ResultNotReady, SnowflakeConfigError

from .support import ACCOUNT, STATEMENTS_URL, USER, ok_body, running_body, statement_url

INT_COL = [{"name": "N", "type": "fixed", "scale": 0}]


@pytest.fixture
async def aclient(private_key_pem: bytes) -> AsyncIterator[AsyncSnowflakeClient]:
    c = AsyncSnowflakeClient(
        ACCOUNT, USER, private_key=private_key_pem, poll_interval=0.0
    )
    yield c
    await c.aclose()


@respx.mock
async def test_async_query(aclient: AsyncSnowflakeClient) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, [["1"], ["2"]]))
    )
    assert await aclient.query("SELECT n FROM t") == [{"N": 1}, {"N": 2}]


@respx.mock
async def test_async_query_scalar(aclient: AsyncSnowflakeClient) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, [["99"]]))
    )
    assert await aclient.query_scalar("SELECT 99") == 99


@respx.mock
async def test_async_execute(aclient: AsyncSnowflakeClient) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(
            200, json=ok_body([], [["4"]], stats={"numRowsUpdated": 4})
        )
    )
    assert await aclient.execute("UPDATE t SET x=1") == 4


@respx.mock
async def test_async_polls_on_202(aclient: AsyncSnowflakeClient) -> None:
    handle = "ah"
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(202, json=running_body(handle))
    )
    respx.get(statement_url(handle)).mock(
        side_effect=[
            httpx.Response(202, json=running_body(handle, code="333333")),
            httpx.Response(200, json=ok_body(INT_COL, [["5"]], handle=handle)),
        ]
    )
    assert await aclient.query("SELECT 5") == [{"N": 5}]


@respx.mock
async def test_async_multi_partition(aclient: AsyncSnowflakeClient) -> None:
    handle = "ap"
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(
            200, json=ok_body(INT_COL, [["0"]], partitions=2, handle=handle)
        )
    )
    respx.get(statement_url(handle)).mock(
        return_value=httpx.Response(200, json={"data": [["1"], ["2"]]})
    )
    rows = await aclient.query("SELECT n FROM big")
    assert [r["N"] for r in rows] == [0, 1, 2]


@respx.mock
async def test_async_result_poll_false_raises(aclient: AsyncSnowflakeClient) -> None:
    handle = "ar"
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(202, json=running_body(handle))
    )
    respx.get(statement_url(handle)).mock(
        return_value=httpx.Response(202, json=running_body(handle, code="333333"))
    )
    handle_obj = await aclient.submit("CALL slow()")
    with pytest.raises(ResultNotReady):
        await handle_obj.result(poll=False)


@respx.mock
async def test_async_context_manager(private_key_pem: bytes) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, [["1"]]))
    )
    async with AsyncSnowflakeClient(
        ACCOUNT, USER, private_key=private_key_pem
    ) as client:
        assert await client.query_scalar("SELECT 1") == 1


@respx.mock
async def test_async_query_one_and_column(aclient: AsyncSnowflakeClient) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, [["1"], ["2"]]))
    )
    assert await aclient.query_one("SELECT n FROM t") == {"N": 1}


@respx.mock
async def test_async_query_one_empty(aclient: AsyncSnowflakeClient) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, []))
    )
    assert await aclient.query_one("SELECT n FROM t WHERE 1=0") is None
    assert await aclient.query_scalar("SELECT n FROM t WHERE 1=0") is None


@respx.mock
async def test_async_query_column(aclient: AsyncSnowflakeClient) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, [["1"], ["2"], ["3"]]))
    )
    assert await aclient.query_column("SELECT n FROM t") == [1, 2, 3]


@respx.mock
async def test_async_insert_many(aclient: AsyncSnowflakeClient) -> None:
    import json

    route = respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(
            200, json=ok_body([], [["2"]], stats={"numRowsInserted": 2})
        )
    )
    n = await aclient.insert_many("t", ["a", "b"], [[1, "x"], [2, "y"]])
    assert n == 2
    sent = json.loads(route.calls[0].request.content)
    assert sent["statement"] == 'INSERT INTO "t" ("a", "b") VALUES (?, ?), (?, ?)'


async def test_async_insert_many_ragged(aclient: AsyncSnowflakeClient) -> None:
    with pytest.raises(SnowflakeConfigError):
        await aclient.insert_many("t", ["a", "b"], [[1]])


async def test_async_insert_many_empty(aclient: AsyncSnowflakeClient) -> None:
    assert await aclient.insert_many("t", ["a"], []) == 0


@respx.mock
async def test_async_submit_status_cancel(aclient: AsyncSnowflakeClient) -> None:
    handle = "asc"
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(202, json=running_body(handle))
    )
    respx.get(statement_url(handle)).mock(
        return_value=httpx.Response(202, json=running_body(handle, code="333333"))
    )
    cancel_route = respx.post(f"{statement_url(handle)}/cancel").mock(
        return_value=httpx.Response(200, json={"code": "090001"})
    )
    handle_obj = await aclient.submit("CALL slow()")
    assert await handle_obj.status() == "RUNNING"
    await handle_obj.cancel()
    assert cancel_route.called


def test_async_requires_private_key() -> None:
    with pytest.raises(SnowflakeConfigError):
        AsyncSnowflakeClient(ACCOUNT, USER)


def test_async_from_env_requires_account(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    with pytest.raises(SnowflakeConfigError):
        AsyncSnowflakeClient.from_env()


@respx.mock
async def test_async_payload_includes_session_context(private_key_pem: bytes) -> None:
    import json

    route = respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, [["1"]]))
    )
    client = AsyncSnowflakeClient(
        ACCOUNT,
        USER,
        private_key=private_key_pem,
        role="MYROLE",
        warehouse="WH",
        database="DB",
        schema="PUBLIC",
        timezone="UTC",
        statement_timeout=45,
        parameters={"WEEK_START": 1},
    )
    await client.query("SELECT 1")
    await client.aclose()
    sent = json.loads(route.calls[0].request.content)
    assert sent["role"] == "MYROLE"
    assert sent["warehouse"] == "WH"
    assert sent["database"] == "DB"
    assert sent["schema"] == "PUBLIC"
    assert sent["timeout"] == 45
    assert sent["parameters"] == {"WEEK_START": 1, "TIMEZONE": "UTC"}
