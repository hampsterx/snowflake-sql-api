"""Multi-partition result fetching.

The SQL API splits large result sets into partitions: partition 0 arrives in
the initial response's ``data``; partitions 1..N are fetched by index from
``GET /api/v2/statements/{handle}?partition={n}`` (gzip-compressed, ``data``
only - reuse partition 0's metadata to decode them, which ``httpx`` decompresses
transparently).

Fetching **every** partition is core behavior, not optional: stopping at
partition 0 silently truncates large results (regression bug #4).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, Generator, List

from .exceptions import SnowflakeRequestError
from .types import ColumnMeta, Row, coerce_row

if TYPE_CHECKING:
    from .transport import AsyncTransport, Transport

__all__ = [
    "partition_count",
    "fetch_all_partitions",
    "fetch_all_partitions_async",
    "iter_result_rows",
    "iter_result_rows_async",
]


def partition_count(first_body: Dict[str, Any]) -> int:
    """Number of partitions in a result, from ``resultSetMetaData.partitionInfo``."""
    metadata = first_body.get("resultSetMetaData") or {}
    partitions = metadata.get("partitionInfo") or []
    return len(partitions)


def _rows(body: Dict[str, Any]) -> List[List[Any]]:
    return list(body.get("data") or [])


def fetch_all_partitions(
    transport: Transport, statement_handle: str, first_body: Dict[str, Any]
) -> List[List[Any]]:
    """Return every partition's raw rows: partition 0 (inline) + partitions 1..N."""
    rows = _rows(first_body)
    for index in range(1, partition_count(first_body)):
        response = transport.get_statement(statement_handle, partition=index)
        rows.extend(_rows(response.body))
    return rows


async def fetch_all_partitions_async(
    transport: AsyncTransport, statement_handle: str, first_body: Dict[str, Any]
) -> List[List[Any]]:
    """Async variant of :func:`fetch_all_partitions`."""
    rows = _rows(first_body)
    for index in range(1, partition_count(first_body)):
        response = await transport.get_statement(statement_handle, partition=index)
        rows.extend(_rows(response.body))
    return rows


def _stream_data(body: Dict[str, Any]) -> List[Any]:
    data = body.get("data")
    if not isinstance(data, list):
        raise SnowflakeRequestError("result partition data must be an array of rows")
    return data


def _stream_row(raw: Any, columns: List[ColumnMeta]) -> Row:
    return coerce_row(raw, columns)


def iter_result_rows(
    transport: Transport,
    statement_handle: str,
    first_body: Dict[str, Any],
    columns: List[ColumnMeta],
) -> Generator[Row, None, None]:
    """Decode rows on demand, releasing each partition before fetching the next."""
    count = max(1, partition_count(first_body))
    data = _stream_data(first_body)
    del first_body
    try:
        for index in range(count):
            if index:
                data = _stream_data(
                    transport.get_statement(statement_handle, partition=index).body
                )
            offset = 0
            while offset < len(data):
                yield _stream_row(data[offset], columns)
                offset += 1
            data = []
    finally:
        # A caller may retain a closed generator; release its buffered rows.
        data = []


async def iter_result_rows_async(
    transport: AsyncTransport,
    statement_handle: str,
    first_body: Dict[str, Any],
    columns: List[ColumnMeta],
) -> AsyncGenerator[Row, None]:
    """Async variant of iter_result_rows, without partition prefetch."""
    count = max(1, partition_count(first_body))
    data = _stream_data(first_body)
    del first_body
    try:
        for index in range(count):
            if index:
                data = _stream_data(
                    (
                        await transport.get_statement(statement_handle, partition=index)
                    ).body
                )
            offset = 0
            while offset < len(data):
                yield _stream_row(data[offset], columns)
                offset += 1
            data = []
    finally:
        # A caller may retain a closed generator; release its buffered rows.
        data = []
