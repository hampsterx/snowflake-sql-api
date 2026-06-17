"""Tests for identifier quoting (a SQL-injection boundary)."""

from __future__ import annotations

import pytest

from snowflake_sql_api.escaping import quote_identifier, quote_name
from snowflake_sql_api.exceptions import SnowflakeConfigError


def test_plain_identifier_is_double_quoted() -> None:
    assert quote_identifier("users") == '"users"'


def test_case_is_preserved() -> None:
    assert quote_identifier("MyTable") == '"MyTable"'


def test_embedded_quote_is_doubled() -> None:
    assert quote_identifier('foo"bar') == '"foo""bar"'


def test_injection_attempt_is_neutralized() -> None:
    # A classic break-out attempt stays inside one quoted identifier.
    hostile = 'x"; DROP TABLE users; --'
    quoted = quote_identifier(hostile)
    assert quoted == '"x""; DROP TABLE users; --"'
    # The only unescaped quotes are the outer pair.
    assert quoted.count('"') % 2 == 0
    assert quoted.startswith('"') and quoted.endswith('"')


def test_empty_identifier_rejected() -> None:
    with pytest.raises(SnowflakeConfigError):
        quote_identifier("")


def test_nul_rejected() -> None:
    with pytest.raises(SnowflakeConfigError):
        quote_identifier("a\x00b")


def test_qualified_name_quoted_per_segment() -> None:
    assert quote_name("my_db.my_schema.my_table") == '"my_db"."my_schema"."my_table"'


def test_single_segment_name() -> None:
    assert quote_name("t") == '"t"'


def test_qualified_name_with_empty_segment_rejected() -> None:
    with pytest.raises(SnowflakeConfigError):
        quote_name("db..table")


def test_non_str_identifier_rejected() -> None:
    with pytest.raises(SnowflakeConfigError):
        quote_identifier(123)  # type: ignore[arg-type]


def test_non_str_name_rejected() -> None:
    with pytest.raises(SnowflakeConfigError):
        quote_name(123)  # type: ignore[arg-type]
