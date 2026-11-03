"""CSV and JSONL serialization for caller-owned text streams."""

from __future__ import annotations

import csv
import json
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, List, TextIO

from .exceptions import SnowflakeConfigError
from .types import ColumnMeta, Row, _validate_column_names


def validate_batch_size(batch_size: int) -> None:
    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or batch_size <= 0
    ):
        raise SnowflakeConfigError("batch_size must be a positive integer")


def validate_export_columns(columns: List[ColumnMeta]) -> None:
    _validate_column_names(columns, context="exports")


def _json_default(value: Any) -> str:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _json(value: Any) -> str:
    return json.dumps(value, default=_json_default, ensure_ascii=False, allow_nan=False)


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (dict, list, tuple, bool)):
        return _json(value)
    if isinstance(value, (Decimal, datetime, date, time, bytes, bytearray)):
        return _json_default(value)
    return value


class CSVWriter:
    """Write a metadata-derived header, then one converted row at a time."""

    def __init__(self, output: TextIO, columns: List[ColumnMeta]) -> None:
        validate_export_columns(columns)
        self._names = [column.name for column in columns]
        self._writer = csv.writer(output)
        self._writer.writerow(self._names)

    def write(self, row: Row) -> None:
        self._writer.writerow([_csv_value(row[name]) for name in self._names])


def write_jsonl(output: TextIO, row: Row) -> None:
    """Write exactly one JSON record, using the CLI's native-value encodings."""
    output.write(_json(row) + "\n")
