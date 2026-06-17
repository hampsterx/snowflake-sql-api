"""Command-line interface: ``snowflake-sql-api query ...``.

Thin wrapper over :class:`~snowflake_sql_api.client.SnowflakeClient`, reading
connection settings from the environment. A basic ``query`` command lands in
Phase 2; the full output formats (``--format table|csv|json|jsonl``, file
input, spinner) arrive with the v0.2.0 toolkit (Phase 8).

Scaffold only.
"""

from __future__ import annotations

import argparse
from typing import Optional, Sequence

from . import __version__

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


def main(argv: Optional[Sequence[str]] = None) -> int:
    """CLI entry point. Returns a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 2

    if args.command == "query":
        # Wired to SnowflakeClient in Phase 2.
        parser.error("the 'query' command is not implemented yet (lands in Phase 2)")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
