"""Command-line interface: ``snowflake-sql-api query ...``.

A thin wrapper over :class:`~snowflake_sql_api.client.SnowflakeClient`, reading
connection settings from the environment (``SNOWFLAKE_*``). This is the basic
JSON-output ``query`` command; richer output formats (``--format
table|csv|json|jsonl``, file input, spinner) arrive with the v0.2.0 toolkit.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Optional, Sequence

from . import __version__
from .exceptions import SnowflakeError

__all__ = ["main", "build_parser"]


def build_parser() -> argparse.ArgumentParser:
    """Construct the top-level argument parser."""
    parser = argparse.ArgumentParser(
        prog="snowflake-sql-api",
        description="Run SQL against Snowflake's SQL API v2.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    query = sub.add_parser("query", help="Run a SQL statement and print the result.")
    query.add_argument("sql", help="SQL statement to execute.")
    return parser


def _json_default(value: Any) -> str:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _run_query(sql: str) -> int:
    from .client import SnowflakeClient

    try:
        with SnowflakeClient.from_env() as client:
            rows = client.query(sql)
    except SnowflakeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(rows, default=_json_default, indent=2))
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point. Returns a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 2
    if args.command == "query":
        return _run_query(args.sql)
    return 0  # pragma: no cover - argparse only yields None or a known subcommand


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
