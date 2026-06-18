"""Shared constants and helpers for the HTTP-mocked test suites.

The SQL API envelope builders (``ok_body`` / ``running_body``) live in the
shipped ``snowflake_sql_api.testing`` module so the test suite and the public
testing helper share one source of truth; they are re-exported here for the
existing respx-based tests.
"""

from __future__ import annotations

from snowflake_sql_api.testing import ok_body, running_body

__all__ = [
    "ACCOUNT",
    "USER",
    "HOST",
    "BASE_URL",
    "STATEMENTS_URL",
    "statement_url",
    "ok_body",
    "running_body",
]

ACCOUNT = "xy12345.ap-southeast-2"
USER = "test_user"
HOST = "xy12345.ap-southeast-2.snowflakecomputing.com"
BASE_URL = f"https://{HOST}"
STATEMENTS_URL = f"{BASE_URL}/api/v2/statements"


def statement_url(handle: str) -> str:
    return f"{STATEMENTS_URL}/{handle}"
