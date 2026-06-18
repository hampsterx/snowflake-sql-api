"""Tests for the shipped testing helper (snowflake_sql_api.testing).

Drives both the sync and async clients through ``FakeSnowflake`` with no network
and no respx, exercising the query/DML/async-submit/partition paths and the
native<->wire round trip for every coerced type.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from snowflake_sql_api import AsyncSnowflakeClient, SnowflakeClient
from snowflake_sql_api.exceptions import ResultNotReady, SnowflakeProgrammingError
from snowflake_sql_api.testing import (
    FakeSnowflake,
    FakeSnowflakeError,
    make_async_client,
    make_client,
)

# ---------------------------------------------------------------------------
# Sync client through FakeSnowflake
# ---------------------------------------------------------------------------


def test_query_returns_registered_rows() -> None:
    fake = FakeSnowflake()
    fake.register(
        "SELECT id, name FROM users",
        [{"ID": 1, "NAME": "alice"}, {"ID": 2, "NAME": "bob"}],
    )
    with make_client(fake) as client:
        assert client.query("SELECT id, name FROM users") == [
            {"ID": 1, "NAME": "alice"},
            {"ID": 2, "NAME": "bob"},
        ]


def test_make_client_returns_real_client() -> None:
    fake = FakeSnowflake()
    fake.register("SELECT 1", [{"N": 1}])
    client = make_client(fake)
    assert isinstance(client, SnowflakeClient)
    assert client.query_scalar("SELECT 1") == 1
    client.close()


def test_make_client_rejects_managed_kwargs() -> None:
    fake = FakeSnowflake()
    with pytest.raises(FakeSnowflakeError, match="manages"):
        make_client(fake, private_key=b"x")


def test_query_with_positional_rows_and_columns() -> None:
    fake = FakeSnowflake()
    fake.register(
        "SELECT id, name FROM t",
        [[1, "alice"], [2, "bob"]],
        columns=["ID", "NAME"],
    )
    with make_client(fake) as client:
        assert client.query("SELECT id, name FROM t") == [
            {"ID": 1, "NAME": "alice"},
            {"ID": 2, "NAME": "bob"},
        ]


def test_explicit_column_types() -> None:
    fake = FakeSnowflake()
    fake.register(
        "SELECT amount FROM t",
        [{"AMOUNT": Decimal("10.50")}],
        columns=[{"name": "AMOUNT", "type": "fixed", "scale": 2}],
    )
    with make_client(fake) as client:
        assert client.query("SELECT amount FROM t") == [{"AMOUNT": Decimal("10.50")}]


def test_empty_result() -> None:
    fake = FakeSnowflake()
    fake.register("SELECT 1 WHERE 1=0", [])
    with make_client(fake) as client:
        assert client.query("SELECT 1 WHERE 1=0") == []
        assert client.query_one("SELECT 1 WHERE 1=0") is None


def test_multi_partition_preserves_order() -> None:
    fake = FakeSnowflake()
    rows = [{"N": n} for n in range(4)]
    fake.register("SELECT n FROM big", rows, partitions=3)
    with make_client(fake) as client:
        assert client.query("SELECT n FROM big") == rows


def test_out_of_range_partition_raises() -> None:
    fake = FakeSnowflake()
    fake.register("SELECT n", [{"N": 1}])
    with make_client(fake) as client:
        client.query("SELECT n")  # creates the single-partition handle stmt-1
        with pytest.raises(FakeSnowflakeError, match="out of range"):
            client._transport.get_statement("stmt-1", partition=99)


def test_register_dml_returns_rowcount() -> None:
    fake = FakeSnowflake()
    fake.register_dml("DELETE FROM t", 7)
    with make_client(fake) as client:
        assert client.execute("DELETE FROM t") == 7


def test_insert_many_through_fake() -> None:
    fake = FakeSnowflake()
    sql = 'INSERT INTO "t" ("a", "b") VALUES (?, ?), (?, ?)'
    fake.register_dml(sql, 2)
    with make_client(fake) as client:
        assert client.insert_many("t", ["a", "b"], [[1, "x"], [2, "y"]]) == 2
    assert fake.submitted_statements == [sql]


def test_register_error_raises_programming_error() -> None:
    fake = FakeSnowflake()
    fake.register_error("SELECT bad", "SQL compilation error", code="000904")
    with make_client(fake) as client, pytest.raises(SnowflakeProgrammingError) as info:
        client.query("SELECT bad")
    assert info.value.code == "000904"


def test_register_match_predicate() -> None:
    fake = FakeSnowflake()
    fake.register_match(lambda sql: sql.startswith("SELECT count"), [{"N": 99}])
    with make_client(fake) as client:
        assert client.query_scalar("SELECT count(*) FROM t") == 99


def test_unregistered_statement_raises() -> None:
    # Single `with` per statement here: a parenthesized multi-context `with`
    # is 3.10+ syntax and breaks the 3.9 leg of the test matrix.
    fake = FakeSnowflake()
    client = make_client(fake)
    with pytest.raises(FakeSnowflakeError, match="no result registered"):
        client.query("SELECT nope")
    client.close()


def test_submitted_statements_records_order() -> None:
    fake = FakeSnowflake()
    fake.register("SELECT 1", [{"N": 1}])
    fake.register("SELECT 2", [{"N": 2}])
    with make_client(fake) as client:
        client.query("SELECT 1")
        client.query("SELECT 2")
    assert fake.submitted_statements == ["SELECT 1", "SELECT 2"]


# ---------------------------------------------------------------------------
# Async-submit / polling behavior
# ---------------------------------------------------------------------------


def test_submit_then_result() -> None:
    fake = FakeSnowflake()
    fake.register("CALL proc()", [{"N": 1}])
    with make_client(fake) as client:
        handle = client.submit("CALL proc()")
        assert handle.status() == "SUCCESS"
        assert handle.result() == [{"N": 1}]


def test_result_poll_false_raises_on_running() -> None:
    fake = FakeSnowflake()
    fake.register("CALL slow()", [{"N": 1}], polls_before_ready=1)
    with make_client(fake) as client:
        handle = client.submit("CALL slow()")
        with pytest.raises(ResultNotReady):
            handle.result(poll=False)


def test_query_polls_until_ready() -> None:
    fake = FakeSnowflake()
    fake.register("SELECT slow", [{"N": 42}], polls_before_ready=2)
    with make_client(fake) as client:
        assert client.query("SELECT slow") == [{"N": 42}]


def test_handle_cancel() -> None:
    fake = FakeSnowflake()
    fake.register("CALL slow()", [{"N": 1}], polls_before_ready=5)
    with make_client(fake) as client:
        handle = client.submit("CALL slow()")
        handle.cancel()  # should not raise


# ---------------------------------------------------------------------------
# Type round-trips (native -> wire -> coerced native)
# ---------------------------------------------------------------------------

TZ = timezone(timedelta(hours=5, minutes=30))

ROUND_TRIP = {
    "an_int": 42,
    "a_float": 1.5,
    "a_str": "hello",
    "a_bool": True,
    "a_false": False,
    "a_decimal": Decimal("12.34"),
    "a_date": date(2023, 1, 1),
    "a_time": time(23, 1, 59, 456789),
    "a_ntz": datetime(2023, 1, 1, 12, 30, 45),
    "a_tz": datetime(2023, 1, 1, 12, 30, 45, tzinfo=TZ),
    "a_variant_obj": {"k": "v", "n": 1},
    "a_variant_arr": [1, 2, 3],
    "a_binary": b"\x01\x02\xff",
}


@pytest.mark.parametrize("name, value", list(ROUND_TRIP.items()))
def test_value_round_trips(name: str, value: object) -> None:
    fake = FakeSnowflake()
    fake.register("SELECT v", [{"V": value}])
    with make_client(fake) as client:
        assert client.query_scalar("SELECT v") == value


@pytest.mark.parametrize(
    "value",
    [
        Decimal("12.34"),
        Decimal("100"),
        Decimal("100.0"),
        Decimal("1E+2"),
        Decimal("-0.5"),
    ],
)
def test_decimal_values_round_trip(value: Decimal) -> None:
    # Scientific-notation Decimals (Decimal("1E+2")) must encode as plain digits,
    # not "1E+2", or the decoder's int()/Decimal() parse blows up.
    fake = FakeSnowflake()
    fake.register("SELECT d", [{"D": value}])
    with make_client(fake) as client:
        assert client.query_scalar("SELECT d") == value


def test_null_in_typed_column() -> None:
    fake = FakeSnowflake()
    fake.register("SELECT n FROM t", [{"N": 1}, {"N": None}])
    with make_client(fake) as client:
        assert client.query("SELECT n FROM t") == [{"N": 1}, {"N": None}]


def test_timestamp_ltz_round_trips() -> None:
    fake = FakeSnowflake()
    aware = datetime(2023, 6, 1, 8, 0, 0, tzinfo=timezone.utc)
    fake.register(
        "SELECT ts",
        [{"TS": aware}],
        columns=[{"name": "TS", "type": "timestamp_ltz"}],
    )
    with make_client(fake) as client:
        assert client.query_scalar("SELECT ts") == aware


@pytest.mark.regression
def test_regression_pre_epoch_subsecond_timestamps() -> None:
    """Pre-1970 sub-second instants must round-trip.

    The encoder derives wire seconds/micros from a timedelta, whose fraction is
    always positive; the decoder reads the fraction as signed. A naive split
    produced ``-1.500000`` for -0.5s, decoding one second early. The fix carries
    the sign into the fraction (``-0.500000``).
    """
    ntz = datetime(1969, 12, 31, 23, 59, 59, 500000)
    ltz = datetime(1969, 12, 31, 23, 59, 59, 500000, tzinfo=timezone.utc)
    tz = datetime(1969, 12, 31, 23, 59, 59, 500000, tzinfo=TZ)
    fake = FakeSnowflake()
    fake.register(
        "SELECT ts_ntz, ts_ltz, ts_tz",
        [{"NTZ": ntz, "LTZ": ltz, "TZ": tz}],
        columns=[
            {"name": "NTZ", "type": "timestamp_ntz"},
            {"name": "LTZ", "type": "timestamp_ltz"},
            {"name": "TZ", "type": "timestamp_tz"},
        ],
    )
    with make_client(fake) as client:
        assert client.query_one("SELECT ts_ntz, ts_ltz, ts_tz") == {
            "NTZ": ntz,
            "LTZ": ltz,
            "TZ": tz,
        }


# ---------------------------------------------------------------------------
# Async client through FakeSnowflake
# ---------------------------------------------------------------------------


async def test_async_query() -> None:
    fake = FakeSnowflake()
    fake.register("SELECT id FROM t", [{"ID": 1}, {"ID": 2}])
    client = make_async_client(fake)
    assert isinstance(client, AsyncSnowflakeClient)
    assert await client.query("SELECT id FROM t") == [{"ID": 1}, {"ID": 2}]
    await client.aclose()


async def test_async_submit_and_result() -> None:
    fake = FakeSnowflake()
    fake.register("CALL proc()", [{"N": 5}], polls_before_ready=1)
    async with make_async_client(fake) as client:
        handle = await client.submit("CALL proc()")
        assert await handle.result() == [{"N": 5}]


async def test_async_multi_partition() -> None:
    fake = FakeSnowflake()
    rows = [{"N": n} for n in range(5)]
    fake.register("SELECT n FROM big", rows, partitions=2)
    async with make_async_client(fake) as client:
        assert await client.query("SELECT n FROM big") == rows


# ---------------------------------------------------------------------------
# Auto-registered pytest fixtures
# ---------------------------------------------------------------------------


def test_fixtures_sync(
    fake_snowflake: FakeSnowflake, snowflake_client: SnowflakeClient
) -> None:
    fake_snowflake.register("SELECT 1", [{"N": 1}])
    assert snowflake_client.query_scalar("SELECT 1") == 1


async def test_fixtures_async(
    fake_snowflake: FakeSnowflake, async_snowflake_client: AsyncSnowflakeClient
) -> None:
    fake_snowflake.register("SELECT 2", [{"N": 2}])
    assert await async_snowflake_client.query_scalar("SELECT 2") == 2
