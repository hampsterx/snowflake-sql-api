"""Result-set type coercion.

The SQL API returns every cell as a JSON string (or JSON ``null``) plus
per-column metadata naming the Snowflake type. This module turns that into
native Python values.

The default ``jsonv2`` result format encodes temporal types as **raw numbers**,
not ISO strings (this is the trap the predecessor fell into):

- ``date`` -> integer **days since 1970-01-01** (``"19358"`` -> 2023-01-01).
- ``time`` -> fractional **seconds since midnight** (``"82919.0"`` -> 23:01:59).
- ``timestamp_ntz`` / ``timestamp_ltz`` -> fractional **epoch seconds**.
- ``timestamp_tz`` -> ``"<epoch_seconds> <offset>"`` where the real UTC offset in
  minutes is ``offset - 1440``.

Decoding matches ``snowflake-connector-python``'s ``converter.py``, the
authoritative implementation of this wire format.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

__all__ = ["Row", "ColumnMeta", "coerce_value", "coerce_rows"]

#: A result row keyed by column name (Snowflake returns names as declared).
Row = Dict[str, Any]

_EPOCH_NAIVE = datetime(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)


class ColumnMeta:
    """Column metadata from a SQL API ``resultSetMetaData.rowType`` entry.

    ``type`` is the lowercase Snowflake type string (``"fixed"``,
    ``"timestamp_tz"``, ...); ``scale`` distinguishes int from Decimal for
    ``fixed`` and sets fractional precision for temporal types.
    """

    __slots__ = ("name", "type", "scale", "precision", "nullable")

    def __init__(
        self,
        name: str,
        type: str,  # mirrors the SQL API ``rowType[].type`` field name
        *,
        scale: Optional[int] = None,
        precision: Optional[int] = None,
        nullable: bool = True,
    ) -> None:
        self.name = name
        self.type = type
        self.scale = scale
        self.precision = precision
        self.nullable = nullable

    @classmethod
    def from_row_type(cls, entry: Dict[str, Any]) -> ColumnMeta:
        """Build from a raw ``rowType[]`` dict."""
        return cls(
            name=entry["name"],
            type=str(entry["type"]).lower(),
            scale=entry.get("scale"),
            precision=entry.get("precision"),
            nullable=bool(entry.get("nullable", True)),
        )


def _split_seconds(value: str) -> Tuple[int, int]:
    """Split a numeric time string into ``(whole_seconds, nanoseconds)``.

    Handles an optional fractional part and negative values; the nanosecond
    component carries the same sign as the seconds so both point the same way
    relative to the epoch (or midnight).
    """
    negative = value.startswith("-")
    int_part, _, frac_part = value.partition(".")
    whole = int(int_part)
    nanos = int((frac_part + "000000000")[:9]) if frac_part else 0
    if negative:
        nanos = -nanos
    return whole, nanos


def _nanos_to_micros(nanos: int) -> int:
    """Convert nanoseconds to microseconds, truncating toward zero.

    Floor division (``//``) would round an extra microsecond away from zero for
    the negative sub-second part of a pre-1970 timestamp; truncation matches the
    whole-second split so the two recombine correctly.
    """
    return nanos // 1000 if nanos >= 0 else -((-nanos) // 1000)


def _decode_date(value: str) -> date:
    return date(1970, 1, 1) + timedelta(days=int(value))


def _decode_time(value: str) -> time:
    secs, nanos = _split_seconds(value)
    hours, rem = divmod(secs, 3600)
    minutes, seconds = divmod(rem, 60)
    return time(
        hour=hours % 24,
        minute=minutes,
        second=seconds,
        microsecond=_nanos_to_micros(nanos),
    )


def _decode_timestamp_ntz(value: str) -> datetime:
    secs, nanos = _split_seconds(value)
    return _EPOCH_NAIVE + timedelta(seconds=secs, microseconds=_nanos_to_micros(nanos))


def _decode_timestamp_ltz(value: str) -> datetime:
    # LTZ is an absolute instant; we return it UTC-aware. That is the correct
    # moment regardless of the session TIMEZONE - callers wanting a specific
    # zone can ``.astimezone(...)``. (We do not thread session tz into coercion.)
    secs, nanos = _split_seconds(value)
    return _EPOCH_UTC + timedelta(seconds=secs, microseconds=_nanos_to_micros(nanos))


def _decode_timestamp_tz(value: str) -> datetime:
    ts_part, _, offset_part = value.partition(" ")
    secs, nanos = _split_seconds(ts_part)
    instant = _EPOCH_UTC + timedelta(seconds=secs, microseconds=_nanos_to_micros(nanos))
    if not offset_part:
        return instant
    tz = timezone(timedelta(minutes=int(offset_part) - 1440))
    return instant.astimezone(tz)


def _decode_fixed(value: str, scale: Optional[int]) -> Any:
    if scale in (None, 0):
        return int(value)
    return Decimal(value)


def coerce_value(raw: Any, column: ColumnMeta) -> Any:
    """Coerce one raw SQL API cell (a string or ``None``) to a Python value."""
    if raw is None:
        return None

    kind = column.type
    if kind == "fixed":
        return _decode_fixed(raw, column.scale)
    if kind in ("real", "float", "double"):
        return float(raw)
    if kind in ("text", "char", "string", "varchar"):
        return raw
    if kind == "boolean":
        # Default wire form is "true"/"false"; tolerate "1"/"0" defensively.
        return str(raw).strip().lower() in ("true", "1")
    if kind == "binary":
        return bytes.fromhex(raw)
    if kind == "date":
        return _decode_date(raw)
    if kind == "time":
        return _decode_time(raw)
    if kind == "timestamp_ntz":
        return _decode_timestamp_ntz(raw)
    if kind == "timestamp_ltz":
        return _decode_timestamp_ltz(raw)
    if kind == "timestamp_tz":
        return _decode_timestamp_tz(raw)
    if kind in ("variant", "object", "array"):
        return json.loads(raw)
    # Unknown type: pass the raw string through unchanged.
    return raw


def coerce_rows(raw_rows: List[List[Any]], columns: List[ColumnMeta]) -> List[Row]:
    """Coerce a partition of raw rows into a list of name-keyed :data:`Row`."""
    names = [c.name for c in columns]
    return [
        {name: coerce_value(cell, col) for name, col, cell in zip(names, columns, row)}
        for row in raw_rows
    ]
