"""Streaming contracts: demand-driven fetching, conversion, and bounded batches."""

from __future__ import annotations

import gzip
import json
import weakref
from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from snowflake_sql_api.exceptions import (
    SnowflakeConfigError,
    SnowflakeRequestError,
    SnowflakeTimeoutError,
)
from snowflake_sql_api.pagination import iter_result_rows, iter_result_rows_async
from snowflake_sql_api.testing import FakeSnowflake
from snowflake_sql_api.transport import StatementResponse
from snowflake_sql_api.types import ColumnMeta

from .streaming_support import (
    advance,
    call,
    close,
    collect,
    expire_poll_deadlines,
    fake_client,
    wire_client,
)
from .support import ok_body

INT_COL = [{"name": "N", "type": "fixed", "scale": 0}]


async def test_stream_submits_and_fetches_only_on_demand(async_mode: bool) -> None:
    fake = FakeSnowflake()
    expected = [{"N": n} for n in range(6)]
    fake.register("SELECT n FROM t", expected, partitions=3)
    async with fake_client(fake, async_mode) as client:
        stream = client.query_stream("SELECT n FROM t")
        assert fake.requests == []
        assert await advance(stream, async_mode) == expected[0]
        assert len(fake.requests) == 1
        assert await advance(stream, async_mode) == expected[1]
        assert len(fake.requests) == 1
        assert await advance(stream, async_mode) == expected[2]
        assert [r.url.params.get("partition") for r in fake.requests] == [None, "1"]
        assert await collect(stream, async_mode) == expected[3:]
    assert fake.submitted_statements == ["SELECT n FROM t"]
    assert [r.url.params.get("partition") for r in fake.requests] == [None, "1", "2"]


@pytest.mark.regression
async def test_regression_bug2_stream_notifies_once_and_binds_params(
    async_mode: bool,
) -> None:
    fake = FakeSnowflake()
    values = [
        {"N": Decimal("12.34"), "D": date(2023, 1, 1), "V": {"x": [1, None]}},
        {"N": Decimal("56.78"), "D": date(2023, 1, 2), "V": {"x": []}},
    ]
    fake.register("SELECT n, d, v FROM t WHERE active = ?", values, partitions=2)
    notifications = []
    async with fake_client(
        fake,
        async_mode,
        on_query=lambda sql, params: notifications.append((sql, params)),
    ) as client:
        stream = client.query_stream("SELECT n, d, v FROM t WHERE active = ?", [True])
        assert notifications == []
        assert await collect(stream, async_mode) == values
    assert notifications == [("SELECT n, d, v FROM t WHERE active = ?", [True])]
    assert json.loads(fake.requests[0].content)["bindings"] == {
        "1": {"type": "BOOLEAN", "value": "true"}
    }


async def test_batches_cross_partitions_without_prefetch(async_mode: bool) -> None:
    fake = FakeSnowflake()
    fake.register("SELECT n", [{"N": n} for n in range(7)], partitions=3)
    async with fake_client(fake, async_mode) as client:
        batches = client.query_batches("SELECT n", batch_size=2)
        assert fake.requests == []
        first = await advance(batches, async_mode)
        assert first == [{"N": 0}, {"N": 1}]
        assert len(fake.requests) == 1
        second = await advance(batches, async_mode)
        assert second == [{"N": 2}, {"N": 3}]
        assert first is not second
        assert await collect(batches, async_mode) == [
            [{"N": 4}, {"N": 5}],
            [{"N": 6}],
        ]


@pytest.mark.parametrize("batch_size", [0, -1, True, 1.5, "2"])
async def test_invalid_batch_sizes_fail_before_submission(
    async_mode: bool, batch_size: Any
) -> None:
    fake = FakeSnowflake()
    async with fake_client(fake, async_mode) as client:
        with pytest.raises(SnowflakeConfigError, match="positive integer"):
            await collect(
                client.query_batches("SELECT n", batch_size=batch_size), async_mode
            )
    assert fake.requests == []


@pytest.mark.parametrize("method", ["query_stream", "query_batches"])
async def test_closing_streams_does_not_fetch_or_close_client(
    async_mode: bool, method: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeSnowflake()
    fake.register("SELECT n", [{"N": 1}, {"N": 2}], partitions=2)
    async with fake_client(fake, async_mode) as client:
        captured = []
        original = client._stream_result
        if async_mode:

            async def capture(*args: Any) -> Any:
                result = await original(*args)
                captured.append(result[1])
                return result

        else:

            def capture(*args: Any) -> Any:
                result = original(*args)
                captured.append(result[1])
                return result

        monkeypatch.setattr(client, "_stream_result", capture)
        kwargs = {"batch_size": 1} if method == "query_batches" else {}
        unstarted = getattr(client, method)("SELECT n", **kwargs)
        await close(unstarted, async_mode)
        assert fake.requests == []
        started = getattr(client, method)("SELECT n", **kwargs)
        await advance(started, async_mode)
        await close(started, async_mode)
        assert len(fake.requests) == 1
        inner = captured[0]
        assert (inner.ag_frame if async_mode else inner.gi_frame) is None
        assert await call(client, async_mode, "query_scalar", "SELECT n") == 1


async def test_empty_streams_and_batches(async_mode: bool) -> None:
    fake = FakeSnowflake()
    fake.register("SELECT empty", [], columns=INT_COL)
    async with fake_client(fake, async_mode) as client:
        assert await collect(client.query_stream("SELECT empty"), async_mode) == []
        assert await collect(client.query_batches("SELECT empty"), async_mode) == []


async def test_stream_polls_before_yielding(async_mode: bool) -> None:
    fake = FakeSnowflake()
    fake.register("SELECT slow", [{"N": 3}], polls_before_ready=2)
    async with fake_client(fake, async_mode) as client:
        assert await collect(client.query_stream("SELECT slow"), async_mode) == [
            {"N": 3}
        ]
    assert len(fake.requests) == 4


async def test_stream_preserves_poll_timeout(
    async_mode: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    expire_poll_deadlines(monkeypatch)
    fake = FakeSnowflake()
    fake.register("SELECT slow", [{"N": 3}], polls_before_ready=10)
    async with fake_client(fake, async_mode, statement_timeout=1) as client:
        with pytest.raises(SnowflakeTimeoutError):
            await collect(client.query_stream("SELECT slow"), async_mode)


async def test_same_partition_conversion_is_lazy(
    async_mode: bool, private_key_pem: bytes
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=ok_body(INT_COL, [["1"], ["invalid integer"]]))

    async with wire_client(handler, private_key_pem, async_mode) as client:
        stream = client.query_stream("SELECT n")
        assert await advance(stream, async_mode) == {"N": 1}
        with pytest.raises(ValueError):
            await advance(stream, async_mode)


async def test_valid_empty_partition_does_not_drop_later_rows(
    async_mode: bool, private_key_pem: bytes
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json=ok_body(INT_COL, [], partitions=2))
        return httpx.Response(200, json=[["7"]])

    async with wire_client(handler, private_key_pem, async_mode) as client:
        assert await collect(client.query_stream("SELECT n"), async_mode) == [{"N": 7}]


async def test_gzip_bare_array_partition_and_retry(
    async_mode: bool, private_key_pem: bytes
) -> None:
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(200, json=ok_body(INT_COL, [["1"]], partitions=2))
        if len(requests) == 2:
            return httpx.Response(503, json={"message": "retry this partition"})
        return httpx.Response(
            200,
            content=gzip.compress(json.dumps([["2"], ["3"]]).encode()),
            headers={"Content-Encoding": "gzip", "Content-Type": "application/json"},
        )

    async with wire_client(handler, private_key_pem, async_mode) as client:
        assert await collect(client.query_stream("SELECT n"), async_mode) == [
            {"N": 1},
            {"N": 2},
            {"N": 3},
        ]
    assert [request.method for request in requests] == ["POST", "GET", "GET"]
    assert all(request.url.params["partition"] == "1" for request in requests[1:])


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"data": None},
        {"data": {}},
        {"data": ["bad row"]},
        {"data": [["1", "extra"]]},
        {"data": [[]]},
    ],
)
async def test_malformed_partition_raises_instead_of_truncating(
    async_mode: bool, private_key_pem: bytes, payload: Any
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(200, json=ok_body(INT_COL, [["1"]], partitions=2))
        if payload is None:
            return httpx.Response(200, content=b"not JSON")
        return httpx.Response(200, json=payload)

    async with wire_client(handler, private_key_pem, async_mode) as client:
        stream = client.query_stream("SELECT n")
        assert await advance(stream, async_mode) == {"N": 1}
        with pytest.raises(SnowflakeRequestError, match="result (partition|row)"):
            await collect(stream, async_mode)


async def test_partition_references_are_released_before_next_fetch_and_on_close(
    async_mode: bool,
) -> None:
    class Partition(list):
        pass

    first = Partition([["1"]])
    reference = weakref.ref(first)
    body = ok_body(INT_COL, first, partitions=2)
    # ok_body may normalize its input; the lifetime assertion concerns this object.
    body["data"] = first
    second = Partition([["2"]])
    second_reference = weakref.ref(second)
    bodies = [{"data": second}]

    def fetch(*args: Any, **kwargs: Any) -> StatementResponse:
        assert reference() is None
        return StatementResponse(200, bodies.pop())

    transport = Mock()
    transport.get_statement = (
        AsyncMock(side_effect=fetch) if async_mode else Mock(side_effect=fetch)
    )
    factory = iter_result_rows_async if async_mode else iter_result_rows
    stream = factory(transport, "handle", body, [ColumnMeta("N", "fixed")])
    del first, body, second
    assert await advance(stream, async_mode) == {"N": 1}
    assert await advance(stream, async_mode) == {"N": 2}
    await close(stream, async_mode)
    assert second_reference() is None
