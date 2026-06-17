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

from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from .transport import AsyncTransport, Transport

__all__ = ["partition_count", "fetch_all_partitions", "fetch_all_partitions_async"]


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
