"""Identifier quoting.

Values are sent through the SQL API's server-side bindings, never interpolated
(see :mod:`snowflake_sql_api.bindings`). Identifiers - table, column, schema,
and procedure names - cannot be bound, so the helpers that build SQL around
them quote and validate here. This is a security boundary; see the README and
AGENTS.md "Security" notes.
"""

from __future__ import annotations

from .exceptions import SnowflakeConfigError

__all__ = ["quote_identifier", "quote_name"]


def quote_identifier(name: str) -> str:
    """Return ``name`` as a safely double-quoted Snowflake identifier.

    Wraps the name in double quotes and doubles any embedded quote, which is
    Snowflake's escape for a quoted identifier (``foo"bar`` -> ``"foo""bar"``).
    The result is always a single, fully-quoted, case-exact identifier, so a
    caller-supplied table/column name cannot break out into injectable SQL.

    Rejects empty names and names containing a NUL character (never valid in a
    Snowflake identifier) rather than emitting unsafe SQL.
    """
    if not isinstance(name, str):
        raise SnowflakeConfigError(
            f"identifier must be a str, got {type(name).__name__}"
        )
    if name == "":
        raise SnowflakeConfigError("identifier must not be empty")
    if "\x00" in name:
        raise SnowflakeConfigError("identifier must not contain a NUL character")
    return '"' + name.replace('"', '""') + '"'


def quote_name(name: str) -> str:
    """Quote a possibly-qualified name (``db.schema.table``) part by part.

    Splits on ``.`` and quotes each segment with :func:`quote_identifier`, so
    ``my_db.my_schema.my_table`` becomes ``"my_db"."my_schema"."my_table"``.
    Each segment must be non-empty; a leading/trailing/double dot is rejected.

    A segment that itself contains a literal dot is not expressible here - pass
    it to :func:`quote_identifier` directly.
    """
    if not isinstance(name, str):
        raise SnowflakeConfigError(f"name must be a str, got {type(name).__name__}")
    return ".".join(quote_identifier(part) for part in name.split("."))
