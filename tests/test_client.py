"""End-to-end sync client behavior over a mocked SQL API."""

from __future__ import annotations

import json
from typing import Iterator

import httpx
import pytest
import respx

from snowflake_sql_api import SnowflakeClient
from snowflake_sql_api.exceptions import SnowflakeConfigError

from .support import ACCOUNT, STATEMENTS_URL, USER, ok_body, running_body, statement_url

INT_COL = [{"name": "N", "type": "fixed", "scale": 0}]
ROW2 = [
    {"name": "ID", "type": "fixed", "scale": 0},
    {"name": "NAME", "type": "text"},
]


@pytest.fixture
def client(private_key_pem: bytes) -> Iterator[SnowflakeClient]:
    c = SnowflakeClient(ACCOUNT, USER, private_key=private_key_pem, poll_interval=0.0)
    yield c
    c.close()


@respx.mock
def test_query_returns_coerced_rows(client: SnowflakeClient) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(
            200, json=ok_body(ROW2, [["1", "alice"], ["2", "bob"]])
        )
    )
    rows = client.query("SELECT id, name FROM t")
    assert rows == [{"ID": 1, "NAME": "alice"}, {"ID": 2, "NAME": "bob"}]


@respx.mock
def test_query_one(client: SnowflakeClient) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, [["7"], ["8"]]))
    )
    assert client.query_one("SELECT n FROM t") == {"N": 7}


@respx.mock
def test_query_one_empty_is_none(client: SnowflakeClient) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, []))
    )
    assert client.query_one("SELECT n FROM t WHERE 1=0") is None


@respx.mock
def test_query_scalar(client: SnowflakeClient) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, [["42"]]))
    )
    assert client.query_scalar("SELECT 42") == 42


@respx.mock
def test_query_column(client: SnowflakeClient) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, [["1"], ["2"], ["3"]]))
    )
    assert client.query_column("SELECT n FROM t") == [1, 2, 3]


@respx.mock
def test_execute_returns_rows_affected(client: SnowflakeClient) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(
            200,
            json=ok_body([], [["5"]], stats={"numRowsInserted": 5}),
        )
    )
    assert client.execute("INSERT INTO t VALUES (1)") == 5


@respx.mock
def test_execute_rows_affected_from_data_fallback(client: SnowflakeClient) -> None:
    # No stats: fall back to the count in data[0][0].
    body = ok_body([{"name": "n", "type": "fixed"}], [["3"]])
    respx.post(STATEMENTS_URL).mock(return_value=httpx.Response(200, json=body))
    assert client.execute("DELETE FROM t") == 3


@respx.mock
def test_insert_many_builds_bound_multirow_insert(client: SnowflakeClient) -> None:
    route = respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(
            200, json=ok_body([], [["2"]], stats={"numRowsInserted": 2})
        )
    )
    n = client.insert_many("my_schema.t", ["a", "b"], [[1, "x"], [2, "y"]])
    assert n == 2
    sent = json.loads(route.calls[0].request.content)
    assert sent["statement"] == (
        'INSERT INTO "my_schema"."t" ("a", "b") VALUES (?, ?), (?, ?)'
    )
    assert sent["bindings"] == {
        "1": {"type": "FIXED", "value": "1"},
        "2": {"type": "TEXT", "value": "x"},
        "3": {"type": "FIXED", "value": "2"},
        "4": {"type": "TEXT", "value": "y"},
    }


@respx.mock
def test_insert_many_empty_rows_is_noop(client: SnowflakeClient) -> None:
    route = respx.post(STATEMENTS_URL).mock(return_value=httpx.Response(200, json={}))
    assert client.insert_many("t", ["a"], []) == 0
    assert route.call_count == 0


def test_insert_many_rejects_ragged_rows(client: SnowflakeClient) -> None:
    with pytest.raises(SnowflakeConfigError):
        client.insert_many("t", ["a", "b"], [[1]])


@respx.mock
def test_query_polls_on_202(client: SnowflakeClient) -> None:
    handle = "h-async"
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(202, json=running_body(handle))
    )
    respx.get(statement_url(handle)).mock(
        side_effect=[
            httpx.Response(202, json=running_body(handle, code="333333")),
            httpx.Response(200, json=ok_body(INT_COL, [["7"]], handle=handle)),
        ]
    )
    assert client.query("SELECT 7") == [{"N": 7}]


@respx.mock
def test_submit_returns_handle_and_status(client: SnowflakeClient) -> None:
    handle = "h-1"
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(202, json=running_body(handle))
    )
    respx.get(statement_url(handle)).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, [["1"]], handle=handle))
    )
    h = client.submit("CALL proc()")
    assert h.statement_handle == handle
    assert h.status() == "SUCCESS"
    assert h.result() == [{"N": 1}]


def test_on_query_hook_fires(private_key_pem: bytes) -> None:
    seen = []
    client = SnowflakeClient(
        ACCOUNT,
        USER,
        private_key=private_key_pem,
        on_query=lambda sql, params: seen.append((sql, params)),
    )
    with respx.mock:
        respx.post(STATEMENTS_URL).mock(
            return_value=httpx.Response(200, json=ok_body(INT_COL, [["1"]]))
        )
        client.query("SELECT 1", [42])
    client.close()
    assert seen == [("SELECT 1", [42])]


@respx.mock
def test_context_manager_closes(private_key_pem: bytes) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, [["1"]]))
    )
    with SnowflakeClient(ACCOUNT, USER, private_key=private_key_pem) as client:
        assert client.query_scalar("SELECT 1") == 1


def test_requires_private_key() -> None:
    with pytest.raises(SnowflakeConfigError):
        SnowflakeClient(ACCOUNT, USER)


def test_from_env_requires_account(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    with pytest.raises(SnowflakeConfigError):
        SnowflakeClient.from_env()


@respx.mock
def test_payload_includes_session_context(private_key_pem: bytes) -> None:
    route = respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, [["1"]]))
    )
    client = SnowflakeClient(
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
    client.query("SELECT 1")
    client.close()
    sent = json.loads(route.calls[0].request.content)
    assert sent["role"] == "MYROLE"
    assert sent["warehouse"] == "WH"
    assert sent["database"] == "DB"
    assert sent["schema"] == "PUBLIC"
    assert sent["timeout"] == 45
    assert sent["parameters"] == {"WEEK_START": 1, "TIMEZONE": "UTC"}


@respx.mock
def test_query_scalar_empty_is_none(client: SnowflakeClient) -> None:
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, []))
    )
    assert client.query_scalar("SELECT n FROM t WHERE 1=0") is None


@respx.mock
def test_handle_cancel(client: SnowflakeClient) -> None:
    handle = "h-cancel"
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(202, json=running_body(handle))
    )
    cancel_route = respx.post(f"{statement_url(handle)}/cancel").mock(
        return_value=httpx.Response(200, json={"code": "090001"})
    )
    handle_obj = client.submit("CALL slow()")
    handle_obj.cancel()
    assert cancel_route.called


@respx.mock
def test_host_override_changes_url(private_key_pem: bytes) -> None:
    custom = "my-host.example.com"
    route = respx.post(f"https://{custom}/api/v2/statements").mock(
        return_value=httpx.Response(200, json=ok_body(INT_COL, [["1"]]))
    )
    client = SnowflakeClient(ACCOUNT, USER, private_key=private_key_pem, host=custom)
    client.query("SELECT 1")
    client.close()
    assert route.called
