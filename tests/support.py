"""Shared constants and helpers for the HTTP-mocked test suites."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

ACCOUNT = "xy12345.ap-southeast-2"
USER = "test_user"
HOST = "xy12345.ap-southeast-2.snowflakecomputing.com"
BASE_URL = f"https://{HOST}"
STATEMENTS_URL = f"{BASE_URL}/api/v2/statements"


def statement_url(handle: str) -> str:
    return f"{STATEMENTS_URL}/{handle}"


def ok_body(
    row_type: List[Dict[str, Any]],
    data: List[List[Any]],
    *,
    partitions: int = 1,
    handle: str = "stmt-1",
    stats: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """Build a 200 success body. ``partitions`` sets the partitionInfo length.

    Partition 0's ``rowCount`` reflects ``data``; the remaining entries are
    placeholders (their rows are served by the partition GET routes).
    """
    if partitions < 1:
        raise ValueError("partitions must be >= 1")
    partition_info = [{"rowCount": len(data)}]
    partition_info += [{"rowCount": 0} for _ in range(partitions - 1)]
    body: Dict[str, Any] = {
        "resultSetMetaData": {
            "numRows": len(data),
            "format": "jsonv2",
            "partitionInfo": partition_info,
            "rowType": row_type,
        },
        "data": data,
        "code": "090001",
        "sqlState": "00000",
        "statementHandle": handle,
        "statementStatusUrl": f"/api/v2/statements/{handle}",
    }
    if stats is not None:
        body["stats"] = stats
    return body


def running_body(handle: str = "stmt-1", code: str = "333334") -> Dict[str, Any]:
    """Build a 202 'still running / submitted async' body."""
    return {
        "code": code,
        "message": "Asynchronous execution in progress.",
        "statementHandle": handle,
        "statementStatusUrl": f"/api/v2/statements/{handle}",
    }
