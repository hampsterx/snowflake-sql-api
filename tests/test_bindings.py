"""Binding matrix: Python values -> SQL API ``bindings`` wire shape."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from snowflake_sql_api.bindings import to_bindings
from snowflake_sql_api.exceptions import SnowflakeConfigError


def test_positions_are_one_based_strings() -> None:
    out = to_bindings(["a", "b"])
    assert set(out.keys()) == {"1", "2"}


def test_int_is_fixed() -> None:
    assert to_bindings([123])["1"] == {"type": "FIXED", "value": "123"}


def test_bool_is_boolean_not_fixed() -> None:
    # bool is a subclass of int; it must be checked first.
    assert to_bindings([True])["1"] == {"type": "BOOLEAN", "value": "true"}
    assert to_bindings([False])["1"] == {"type": "BOOLEAN", "value": "false"}


def test_decimal_is_fixed() -> None:
    assert to_bindings([Decimal("1.50")])["1"] == {"type": "FIXED", "value": "1.50"}


def test_float_is_real() -> None:
    assert to_bindings([3.14])["1"] == {"type": "REAL", "value": "3.14"}


def test_str_is_text() -> None:
    assert to_bindings(["hi"])["1"] == {"type": "TEXT", "value": "hi"}


def test_bytes_is_binary_hex() -> None:
    # The predecessor mishandled bytes (str(b"...")); we send hex.
    assert to_bindings([b"HELLO"])["1"] == {"type": "BINARY", "value": "48454c4c4f"}


def test_none_is_json_null() -> None:
    assert to_bindings([None])["1"] == {"type": "TEXT", "value": None}


def test_date_is_epoch_millis() -> None:
    # DATE binds as milliseconds since the epoch (19358 days -> ms).
    assert to_bindings([date(2023, 1, 1)])["1"] == {
        "type": "DATE",
        "value": "1672531200000",
    }


def test_date_pre_epoch_is_negative_millis() -> None:
    assert to_bindings([date(1969, 12, 31)])["1"] == {
        "type": "DATE",
        "value": "-86400000",
    }


def test_naive_datetime_is_timestamp_ntz_epoch_nanos() -> None:
    out = to_bindings([datetime(2023, 1, 1, 12, 0, 0)])["1"]
    assert out == {"type": "TIMESTAMP_NTZ", "value": "1672574400000000000"}


def test_aware_datetime_utc_is_timestamp_tz_offset_1440() -> None:
    out = to_bindings([datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)])["1"]
    assert out == {"type": "TIMESTAMP_TZ", "value": "1672574400000000000 1440"}


def test_aware_datetime_offset_is_biased_by_1440() -> None:
    tz = timezone(timedelta(hours=5))
    out = to_bindings([datetime(2023, 1, 1, 12, 0, 0, tzinfo=tz)])["1"]
    # 12:00+05:00 == 07:00Z == epoch 1672556400; offset 300 -> 1740.
    assert out == {"type": "TIMESTAMP_TZ", "value": "1672556400000000000 1740"}


def test_time_is_nanos_since_midnight() -> None:
    assert to_bindings([time(1, 2, 3)])["1"] == {
        "type": "TIME",
        "value": "3723000000000",
    }


def test_time_with_microseconds() -> None:
    assert to_bindings([time(1, 2, 3, 500000)])["1"] == {
        "type": "TIME",
        "value": "3723500000000",
    }


def test_list_and_dict_are_json_text() -> None:
    assert to_bindings([[1, 2]])["1"] == {"type": "TEXT", "value": "[1, 2]"}
    assert to_bindings([{"a": 1}])["1"] == {"type": "TEXT", "value": '{"a": 1}'}


def test_unsupported_type_raises() -> None:
    with pytest.raises(SnowflakeConfigError):
        to_bindings([object()])


def test_mixed_ordering() -> None:
    out = to_bindings([1, "x", True])
    assert out["1"]["type"] == "FIXED"
    assert out["2"]["type"] == "TEXT"
    assert out["3"]["type"] == "BOOLEAN"
