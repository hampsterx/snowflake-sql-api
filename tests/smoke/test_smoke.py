"""Live smoke tests against a real Snowflake account.

Opt-in and dev-only: skipped unless ``SNOWFLAKE_ACCOUNT``, ``SNOWFLAKE_USER``,
and ``SNOWFLAKE_PRIVATE_KEY_PATH`` are set. These assert *shapes, types, and
counts* against synthetic data (``SELECT 1``, ``GENERATOR``) - never business
data, and no expected output containing real rows is committed.

Run with::

    SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PRIVATE_KEY_PATH=... \
        SNOWFLAKE_WAREHOUSE=... pytest tests/smoke -v
"""

from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import pytest

from snowflake_sql_api import AsyncSnowflakeClient, SnowflakeClient

REQUIRED = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH")

pytestmark = [
    pytest.mark.smoke,
    pytest.mark.skipif(
        not all(os.environ.get(name) for name in REQUIRED),
        reason="live Snowflake credentials not set (SNOWFLAKE_ACCOUNT/USER/PRIVATE_KEY_PATH)",
    ),
]

# Forces a result big enough to span more than one partition on most warehouses;
# the assertion only checks the count, so it holds whether or not it splits.
PARTITION_ROWS = 300_000

TYPE_PROBE = """
SELECT
    1::INT                                        AS i,
    1.5::FLOAT                                    AS f,
    'x'::VARCHAR                                  AS s,
    TRUE                                          AS b,
    TO_DATE('2023-01-01')                         AS d,
    TO_TIMESTAMP_NTZ('2023-01-01 12:00:00')       AS ts_ntz,
    TO_TIMESTAMP_TZ('2023-01-01 12:00:00 +05:00') AS ts_tz,
    PARSE_JSON('{"a": 1}')                        AS v,
    TO_DECIMAL(1.50, 10, 2)                       AS dec
"""


def test_select_one_sync() -> None:
    with SnowflakeClient.from_env() as client:
        assert client.query_scalar("SELECT 1") == 1


def test_type_matrix_against_live_wire() -> None:
    with SnowflakeClient.from_env() as client:
        row = client.query_one(TYPE_PROBE)
    assert row is not None
    assert isinstance(row["I"], int)
    assert isinstance(row["F"], float)
    assert isinstance(row["S"], str)
    assert isinstance(row["B"], bool)
    assert isinstance(row["D"], date)
    assert isinstance(row["TS_NTZ"], datetime) and row["TS_NTZ"].tzinfo is None
    assert isinstance(row["TS_TZ"], datetime) and row["TS_TZ"].tzinfo is not None
    assert isinstance(row["V"], dict)
    assert isinstance(row["DEC"], Decimal)


def test_temporal_bind_roundtrip() -> None:
    """Round-trip bound temporal values through the real wire.

    Exercises the epoch-based bind encoders (the bind format the offline tests
    can assert the shape of, but only a live call validates Snowflake accepts).
    """
    dt_tz = datetime(2023, 6, 15, 12, 30, 45, tzinfo=timezone(timedelta(hours=5)))
    with SnowflakeClient.from_env() as client:
        row = client.query_one(
            "SELECT ?::DATE AS d, ?::TIMESTAMP_NTZ AS ts, "
            "?::TIME AS t, ?::TIMESTAMP_TZ AS tz, ?::NUMBER(10,2) AS n",
            [
                date(2023, 1, 1),
                datetime(2023, 6, 15, 12, 30, 45),
                time(1, 2, 3),
                dt_tz,
                Decimal("1.50"),
            ],
        )
    assert row is not None
    assert row["D"] == date(2023, 1, 1)
    assert row["TS"] == datetime(2023, 6, 15, 12, 30, 45)
    assert row["T"] == time(1, 2, 3)
    assert row["TZ"] == dt_tz  # same instant (aware == compares the moment)
    assert row["N"] == Decimal("1.50")


def test_multi_partition_count_sync() -> None:
    sql = (
        "SELECT seq4() AS n, randstr(120, random()) AS s "
        f"FROM TABLE(GENERATOR(ROWCOUNT => {PARTITION_ROWS}))"
    )
    with SnowflakeClient.from_env() as client:
        rows = client.query(sql)
    assert len(rows) == PARTITION_ROWS


@pytest.mark.asyncio
async def test_select_one_and_partitions_async() -> None:
    sql = (
        "SELECT seq4() AS n, randstr(120, random()) AS s "
        f"FROM TABLE(GENERATOR(ROWCOUNT => {PARTITION_ROWS}))"
    )
    async with AsyncSnowflakeClient.from_env() as client:
        assert await client.query_scalar("SELECT 1") == 1
        rows = await client.query(sql)
    assert len(rows) == PARTITION_ROWS
