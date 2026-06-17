"""Parameter binding.

The SQL API binds values server-side via the ``bindings`` object on the
statement request: each parameter is sent as ``{"type": ..., "value": ...}``
keyed by its 1-based position, with ``value`` always a JSON string (or JSON
``null`` for SQL NULL). This module converts Python values into that wire shape
so helpers never interpolate user values into SQL text.

Temporal bind values are **epoch numbers, not ISO strings** (the SQL API binding
format, confirmed against the docs - distinct from, and the inverse of, the
result encoding in :mod:`snowflake_sql_api.types`):

- ``DATE`` -> milliseconds since the epoch.
- ``TIME`` -> nanoseconds since midnight.
- ``TIMESTAMP_NTZ`` / ``TIMESTAMP_LTZ`` -> nanoseconds since the epoch.
- ``TIMESTAMP_TZ`` -> ``"<epoch_nanos> <offset>"`` where ``offset`` is the real
  UTC offset in minutes biased by +1440 (so UTC is ``1440``, ``-08:00`` is
  ``960``), matching the documented example ``1616173619000000000 960``.

isinstance order matters: ``bool`` is a subclass of ``int`` and ``datetime`` a
subclass of ``date``, so the narrower type is checked first.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Dict, Optional, Sequence

from .exceptions import SnowflakeConfigError

__all__ = ["BindingValue", "to_bindings"]

#: One entry of the SQL API ``bindings`` map: ``{"type": str, "value": str|None}``.
BindingValue = Dict[str, Optional[str]]

_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_NAIVE = datetime(1970, 1, 1)
_EPOCH_UTC = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MILLIS_PER_DAY = 86_400_000


def _epoch_nanos(value: datetime, epoch: datetime) -> int:
    delta = value - epoch
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + (
        delta.microseconds * 1_000
    )


def _bind_datetime(value: datetime) -> BindingValue:
    if value.tzinfo is not None:
        nanos = _epoch_nanos(value, _EPOCH_UTC)
        offset = value.utcoffset()
        offset_min = round(offset.total_seconds() / 60) if offset else 0
        return {"type": "TIMESTAMP_TZ", "value": f"{nanos} {offset_min + 1440}"}
    return {"type": "TIMESTAMP_NTZ", "value": str(_epoch_nanos(value, _EPOCH_NAIVE))}


def _bind_one(value: Any) -> BindingValue:
    if value is None:
        # The server ignores the type on a null binding; TEXT is a safe carrier.
        return {"type": "TEXT", "value": None}
    if isinstance(value, bool):
        return {"type": "BOOLEAN", "value": "true" if value else "false"}
    if isinstance(value, int):
        return {"type": "FIXED", "value": str(value)}
    if isinstance(value, Decimal):
        return {"type": "FIXED", "value": str(value)}
    if isinstance(value, float):
        return {"type": "REAL", "value": repr(value)}
    if isinstance(value, (bytes, bytearray)):
        return {"type": "BINARY", "value": bytes(value).hex()}
    if isinstance(value, datetime):  # before date: datetime is a date subclass
        return _bind_datetime(value)
    if isinstance(value, date):
        days = (value - _EPOCH_DATE).days
        return {"type": "DATE", "value": str(days * _MILLIS_PER_DAY)}
    if isinstance(value, time):
        nanos = (
            value.hour * 3_600 + value.minute * 60 + value.second
        ) * 1_000_000_000 + value.microsecond * 1_000
        return {"type": "TIME", "value": str(nanos)}
    if isinstance(value, str):
        return {"type": "TEXT", "value": value}
    if isinstance(value, (list, dict, tuple)):
        return {"type": "TEXT", "value": json.dumps(value)}
    raise SnowflakeConfigError(
        f"cannot bind value of type {type(value).__name__}: {value!r}"
    )


def to_bindings(params: Sequence[Any]) -> Dict[str, BindingValue]:
    """Convert positional params into the SQL API ``bindings`` map.

    Keys are 1-based string positions (``"1"``, ``"2"``, ...). Each Python value
    maps to a ``{"type", "value"}`` pair; unsupported types raise
    :class:`~snowflake_sql_api.exceptions.SnowflakeConfigError`.
    """
    return {str(i): _bind_one(value) for i, value in enumerate(params, start=1)}
