"""Result dictionaries must not silently discard columns or malformed cells."""

from __future__ import annotations

import io
from typing import Any

import httpx
import pytest

from snowflake_sql_api.exceptions import SnowflakeConfigError, SnowflakeRequestError
from snowflake_sql_api.exports import CSVWriter
from snowflake_sql_api.testing import FakeSnowflake
from snowflake_sql_api.types import ColumnMeta, coerce_row

from .streaming_support import advance, call, close, collect, fake_client, wire_client
from .support import ok_body

RESULT_METHODS = [
    "query",
    "query_one",
    "query_scalar",
    "query_column",
    "query_stream",
    "query_batches",
    "result",
]


async def read_result(client: Any, async_mode: bool, method: str, sql: str) -> Any:
    if method == "result":
        handle = await call(client, async_mode, "submit", sql)
        return await call(handle, async_mode, "result")
    if method in ("query_stream", "query_batches"):
        stream = getattr(client, method)(sql)
        try:
            return await collect(stream, async_mode)
        finally:
            await close(stream, async_mode)
    return await call(client, async_mode, method, sql)


@pytest.mark.regression
@pytest.mark.parametrize("method", RESULT_METHODS)
@pytest.mark.parametrize("empty", [False, True], ids=["rows", "empty"])
async def test_regression_duplicate_columns_rejected_before_first_row(
    async_mode: bool, method: str, empty: bool
) -> None:
    fake = FakeSnowflake()
    sql = "SELECT o.id, c.id, o.name, c.name, o.id FROM orders o JOIN customers c ON o.cid = c.id"
    fake.register(
        sql,
        [] if empty else [[1, 2, "order", "customer", 1]],
        columns=["ID", "ID", "NAME", "NAME", "ID"],
        partitions=2,
        polls_before_ready=1,
    )
    async with fake_client(fake, async_mode, poll_interval=0) as client:
        with pytest.raises(SnowflakeConfigError) as info:
            if method in ("query_stream", "query_batches"):
                stream = getattr(client, method)(sql)
                try:
                    await advance(stream, async_mode)
                finally:
                    await close(stream, async_mode)
            else:
                await read_result(client, async_mode, method, sql)
    message = str(info.value)
    assert "unique column names" in message
    assert message.count("'ID'") == 1
    assert message.count("'NAME'") == 1
    assert "distinct SQL aliases" in message
    assert all("partition" not in request.url.params for request in fake.requests)


@pytest.mark.regression
async def test_regression_repeated_literal_columns_need_aliases(
    async_mode: bool,
) -> None:
    fake = FakeSnowflake().register("SELECT 1, 1", [[1, 1]], columns=["1", "1"])
    async with fake_client(fake, async_mode) as client:
        with pytest.raises(SnowflakeConfigError, match="'1'.*distinct SQL aliases"):
            await call(client, async_mode, "query_scalar", "SELECT 1, 1")


@pytest.mark.regression
@pytest.mark.parametrize("method", ["export_csv", "export_jsonl"])
@pytest.mark.parametrize("empty", [False, True], ids=["rows", "empty"])
async def test_regression_duplicate_exports_leave_sink_untouched(
    async_mode: bool, method: str, empty: bool
) -> None:
    fake = FakeSnowflake().register(
        "SELECT 1, 1", [] if empty else [[1, 1]], columns=["1", "1"]
    )
    output = io.StringIO()
    async with fake_client(fake, async_mode) as client:
        with pytest.raises(SnowflakeConfigError, match="'1'.*distinct SQL aliases"):
            await call(client, async_mode, method, "SELECT 1, 1", output)
    assert output.getvalue() == ""
    assert not output.closed


def test_csv_writer_retains_export_validation() -> None:
    output = io.StringIO()
    with pytest.raises(
        SnowflakeConfigError, match="exports require unique column names"
    ):
        CSVWriter(output, [ColumnMeta("ID", "fixed")] * 2)
    assert output.getvalue() == ""


@pytest.mark.parametrize("method", RESULT_METHODS)
async def test_distinct_aliases_preserve_values_and_case(
    async_mode: bool, method: str
) -> None:
    sql = 'SELECT 1 AS "ID", 2 AS "id"'
    fake = FakeSnowflake().register(sql, [[1, 2]], columns=["ID", "id"])
    row = {"ID": 1, "id": 2}
    expected = {
        "query": [row],
        "query_one": row,
        "query_scalar": 1,
        "query_column": [1],
        "query_stream": [row],
        "query_batches": [[row]],
        "result": [row],
    }
    async with fake_client(fake, async_mode) as client:
        assert await read_result(client, async_mode, method, sql) == expected[method]


@pytest.mark.regression
@pytest.mark.parametrize("method", RESULT_METHODS)
@pytest.mark.parametrize("later_partition", [False, True], ids=["inline", "later"])
async def test_regression_short_result_rows_raise(
    async_mode: bool, method: str, later_partition: bool, private_key_pem: bytes
) -> None:
    columns = [{"name": name, "type": "fixed", "scale": 0} for name in ("A", "B")]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("partition") == "1":
            return httpx.Response(200, json={"data": [["1"]]})
        return httpx.Response(
            200,
            json=ok_body(
                columns,
                [["1", "2"]] if later_partition else [["1"]],
                partitions=2 if later_partition else 1,
            ),
        )

    async with wire_client(handler, private_key_pem, async_mode) as client:
        with pytest.raises(SnowflakeRequestError) as info:
            await read_result(client, async_mode, method, "SELECT a, b FROM t")
    assert str(info.value) == "result row must be an array with 2 values"


@pytest.mark.regression
@pytest.mark.parametrize(
    "raw", [[], ["1"], ["1", "2", "3"], "12", None, {"A": "1", "B": "2"}]
)
def test_regression_coerce_row_rejects_malformed_width_or_type(raw: Any) -> None:
    columns = [ColumnMeta(name, "fixed", scale=0) for name in ("A", "B")]
    with pytest.raises(
        SnowflakeRequestError, match="result row must be an array with 2 values"
    ):
        coerce_row(raw, columns)


def test_coerce_row_preserves_nulls_and_accepts_zero_columns() -> None:
    assert coerce_row(
        ["1", None], [ColumnMeta("A", "fixed"), ColumnMeta("B", "text")]
    ) == {"A": 1, "B": None}
    assert coerce_row([], []) == {}


async def test_dml_ignores_result_column_names(async_mode: bool) -> None:
    fake = FakeSnowflake().register_match(
        lambda sql: True,
        [[2, 3]],
        columns=["ID", "ID"],
        stats={"numRowsInserted": 2},
    )
    async with fake_client(fake, async_mode) as client:
        assert await call(client, async_mode, "execute", "UPDATE t SET n = 1") == 2
        assert (
            await call(client, async_mode, "insert_many", "t", ["N"], [[1], [2]]) == 2
        )
        progress = await call(
            client, async_mode, "insert_batches", "t", ["N"], [[1], [2]]
        )
        assert progress.rows_affected == 2
