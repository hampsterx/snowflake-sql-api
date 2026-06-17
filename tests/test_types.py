"""Type-coercion matrix.

Worked examples are taken from the verified SQL API ``jsonv2`` wire spec: the
default format ships temporal types as raw epoch numbers, not ISO strings.
"""

from __future__ import annotations

from datetime import date, datetime, time, timezone
from decimal import Decimal

import pytest

from snowflake_sql_api.types import ColumnMeta, coerce_rows, coerce_value


def col(type_: str, **kw: object) -> ColumnMeta:
    return ColumnMeta("c", type_, **kw)  # type: ignore[arg-type]


def test_fixed_scale_zero_is_int() -> None:
    assert coerce_value("42", col("fixed", scale=0)) == 42
    assert isinstance(coerce_value("42", col("fixed", scale=0)), int)


def test_fixed_scale_none_is_int() -> None:
    assert coerce_value("7", col("fixed")) == 7


def test_fixed_with_scale_is_decimal() -> None:
    value = coerce_value("1.50", col("fixed", scale=2))
    assert value == Decimal("1.50")
    assert isinstance(value, Decimal)


def test_real_is_float() -> None:
    assert coerce_value("3.14", col("real")) == 3.14
    assert coerce_value("1.5E10", col("real")) == 1.5e10


def test_text_passthrough() -> None:
    assert coerce_value("hello", col("text")) == "hello"


def test_boolean() -> None:
    assert coerce_value("true", col("boolean")) is True
    assert coerce_value("false", col("boolean")) is False


def test_boolean_numeric_wire_form() -> None:
    # Tolerate "1"/"0" as well as the textual form.
    assert coerce_value("1", col("boolean")) is True
    assert coerce_value("0", col("boolean")) is False


def test_binary_is_hex_decoded() -> None:
    assert coerce_value("48454C4C4F", col("binary")) == b"HELLO"


def test_date_is_days_since_epoch() -> None:
    assert coerce_value("19358", col("date")) == date(2023, 1, 1)
    assert coerce_value("0", col("date")) == date(1970, 1, 1)
    assert coerce_value("-1", col("date")) == date(1969, 12, 31)


def test_time_is_seconds_since_midnight() -> None:
    assert coerce_value("82919.000000000", col("time")) == time(23, 1, 59)
    assert coerce_value("3600", col("time")) == time(1, 0, 0)
    assert coerce_value("82919.500000000", col("time")) == time(23, 1, 59, 500000)


def test_timestamp_ntz_is_naive() -> None:
    value = coerce_value("1672574400.000000000", col("timestamp_ntz"))
    assert value == datetime(2023, 1, 1, 12, 0, 0)
    assert value.tzinfo is None


def test_timestamp_ntz_keeps_subsecond() -> None:
    value = coerce_value("1672574400.123456789", col("timestamp_ntz"))
    assert value == datetime(2023, 1, 1, 12, 0, 0, 123456)


def test_timestamp_ntz_negative_truncates_toward_zero() -> None:
    # Pre-1970: -1.999999999s must truncate to -1.999999s, not floor to -2.0s.
    value = coerce_value("-1.999999999", col("timestamp_ntz"))
    assert value == datetime(1969, 12, 31, 23, 59, 58, 1)


def test_timestamp_ltz_is_utc_aware() -> None:
    value = coerce_value("1672574400.000000000", col("timestamp_ltz"))
    assert value == datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    assert value.utcoffset().total_seconds() == 0  # type: ignore[union-attr]


def test_timestamp_tz_applies_offset() -> None:
    # "<epoch> <offset>" where real offset minutes = offset - 1440. 1740 -> +300.
    value = coerce_value("1672563600.000000000 1740", col("timestamp_tz"))
    assert value.utcoffset().total_seconds() == 300 * 60  # type: ignore[union-attr]
    # 1672563600 = 2023-01-01T09:00:00Z, rendered at +05:00 -> 14:00 local.
    assert value.hour == 14
    # Same instant as the UTC moment, regardless of display offset.
    assert value == datetime(2023, 1, 1, 9, 0, 0, tzinfo=timezone.utc)


def test_timestamp_tz_utc_offset_bias() -> None:
    # 1440 means UTC (1440 - 1440 = 0).
    value = coerce_value("1672531200.000000000 1440", col("timestamp_tz"))
    assert value.utcoffset().total_seconds() == 0  # type: ignore[union-attr]


def test_timestamp_tz_negative_offset() -> None:
    # 1140 -> 1140 - 1440 = -300 -> -05:00.
    value = coerce_value("1672531200.000000000 1140", col("timestamp_tz"))
    assert value.utcoffset().total_seconds() == -300 * 60  # type: ignore[union-attr]


def test_variant_object_array_parsed() -> None:
    assert coerce_value('{"a": 1}', col("variant")) == {"a": 1}
    assert coerce_value("[1, 2]", col("array")) == [1, 2]
    assert coerce_value('{"k": "v"}', col("object")) == {"k": "v"}


def test_null_is_none_regardless_of_type() -> None:
    assert coerce_value(None, col("fixed", scale=0)) is None
    assert coerce_value(None, col("timestamp_tz")) is None


def test_unknown_type_passthrough() -> None:
    assert coerce_value("raw", col("geography")) == "raw"


def test_coerce_rows_keys_by_column_name() -> None:
    columns = [ColumnMeta("ID", "fixed", scale=0), ColumnMeta("NAME", "text")]
    rows = coerce_rows([["1", "alice"], ["2", "bob"]], columns)
    assert rows == [{"ID": 1, "NAME": "alice"}, {"ID": 2, "NAME": "bob"}]


@pytest.mark.parametrize(
    "raw,expected",
    [("0", time(0, 0, 0)), ("86399", time(23, 59, 59))],
)
def test_time_bounds(raw: str, expected: time) -> None:
    assert coerce_value(raw, col("time")) == expected
