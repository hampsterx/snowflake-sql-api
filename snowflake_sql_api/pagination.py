"""Multi-partition result fetching.

The SQL API splits large result sets into partitions: the first arrives with
the initial response, the rest are fetched by index from
``GET /api/v2/statements/{handle}?partition=N``. Fetching every partition is
**core behavior**, not optional - skipping it silently truncates results to
partition 0 (regression bug #4). Partition bodies may be gzip-compressed.

Scaffold only: the fetch loop lands in Phase 2.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    from .transport import AsyncTransport, Transport

__all__ = ["fetch_all_partitions", "fetch_all_partitions_async"]


def fetch_all_partitions(
    transport: Transport, statement_handle: str, metadata: Dict[str, Any]
) -> List[List[Any]]:
    """Fetch and concatenate every partition's raw rows (partition 0 + rest). (Phase 2.)"""
    raise NotImplementedError


async def fetch_all_partitions_async(
    transport: AsyncTransport, statement_handle: str, metadata: Dict[str, Any]
) -> List[List[Any]]:
    """Async variant of :func:`fetch_all_partitions`. (Phase 2.)"""
    raise NotImplementedError
