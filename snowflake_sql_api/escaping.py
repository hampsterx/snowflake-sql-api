"""Identifier quoting.

Values are sent through the SQL API's server-side bindings, never interpolated
(see :mod:`snowflake_sql_api.bindings`). Identifiers - table, column, schema,
and procedure names - cannot be bound, so the helpers that build SQL around
them quote and validate here. This is a security boundary; see the README and
AGENTS.md "Security" notes.

Scaffold only: strict quoting/validation lands in Phase 2.
"""

from __future__ import annotations

__all__ = ["quote_identifier", "quote_name"]


def quote_identifier(name: str) -> str:
    """Return ``name`` as a safely double-quoted Snowflake identifier.

    Rejects anything that cannot be a legal quoted identifier rather than
    silently producing injectable SQL. Implemented in Phase 2.
    """
    raise NotImplementedError


def quote_name(name: str) -> str:
    """Quote a possibly-qualified name (``db.schema.table``) part by part. (Phase 2.)"""
    raise NotImplementedError
