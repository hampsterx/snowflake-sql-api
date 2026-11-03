"""Opt-in probe: write helpers against a conventionally named table.

``insert_many`` and ``insert_batches`` quote table and column names exactly as
given. A table created with unquoted identifiers is stored in uppercase, so the
documented examples pass uppercase names. This probe creates such a table,
inserts through both helpers with uppercase names, checks the row count, and
checks that the lowercase spelling is rejected (it names no existing object).

Requires SNOWFLAKE_LIVE_DML=1, keypair credentials, and explicit DATABASE,
SCHEMA and WAREHOUSE in a disposable test namespace. Creates one table, then
drops it. Run with pytest tests/smoke/test_identifier_case.py -v
"""

from __future__ import annotations

import inspect
import os
from uuid import uuid4

import pytest

from snowflake_sql_api import AsyncSnowflakeClient, SnowflakeClient
from snowflake_sql_api.exceptions import SnowflakeError

REQUIRED = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_WAREHOUSE",
)
pytestmark = [
    pytest.mark.smoke,
    pytest.mark.skipif(
        os.environ.get("SNOWFLAKE_LIVE_DML") != "1"
        or not all(os.environ.get(name) for name in REQUIRED),
        reason="requires SNOWFLAKE_LIVE_DML=1, credentials and explicit disposable namespace",
    ),
]


def connection() -> dict:
    return {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key_path": os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        "private_key_passphrase": os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        "database": os.environ["SNOWFLAKE_DATABASE"],
        "schema": os.environ["SNOWFLAKE_SCHEMA"],
        "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        "role": os.environ.get("SNOWFLAKE_ROLE"),
        "host": os.environ.get("SNOWFLAKE_HOST"),
        "parameters": {"QUERY_TAG": "identifier-case-smoke"},
    }


async def value(result):
    return await result if inspect.isawaitable(result) else result


@pytest.fixture
def table():
    # Unquoted DDL, as most applications write it: Snowflake stores the table
    # and its columns in uppercase.
    name = "IDCASE_PROBE_" + uuid4().hex.upper()
    with SnowflakeClient(**connection()) as control:
        control.execute(f"CREATE TABLE {name.lower()} (id NUMBER, label VARCHAR)")
        try:
            yield name, control
        finally:
            control.execute(f"DROP TABLE IF EXISTS {name}")


async def test_uppercase_names_insert_into_conventional_table(async_mode, table):
    name, control = table
    cls = AsyncSnowflakeClient if async_mode else SnowflakeClient
    client = cls(**connection())
    try:
        many = await value(client.insert_many(name, ["ID", "LABEL"], [(1, "many")]))
        batches = await value(
            client.insert_batches(
                name, ["ID", "LABEL"], ((i, f"batch {i}") for i in range(2, 7)), max_rows=2
            )
        )
    finally:
        await value(client.aclose() if async_mode else client.close())
    assert many == 1
    assert batches.input_rows == 5
    assert batches.completed_batches == 3
    assert control.query_scalar(f"SELECT COUNT(*) FROM {name}") == 6


async def test_lowercase_names_do_not_resolve_to_conventional_table(async_mode, table):
    name, _ = table
    cls = AsyncSnowflakeClient if async_mode else SnowflakeClient
    client = cls(**connection())
    try:
        with pytest.raises(SnowflakeError):
            await value(client.insert_many(name.lower(), ["id", "label"], [(1, "x")]))
        with pytest.raises(SnowflakeError):
            await value(
                client.insert_batches(name.lower(), ["id", "label"], [(1, "x")])
            )
    finally:
        await value(client.aclose() if async_mode else client.close())
