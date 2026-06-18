"""Unit tests for the ``snowflake-sql-api`` CLI (``cli.py``).

Drive ``build_parser()`` / ``main()`` with ``argv`` injection and a stubbed
client (``from_env`` monkeypatched to a :class:`FakeSnowflake`-backed client), so
no network and no real account. Covers the exit-code contract documented in
``docs/cli.md`` plus the ``_json_default`` serializer.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timezone
from decimal import Decimal

import pytest

from snowflake_sql_api import cli
from snowflake_sql_api.client import SnowflakeClient
from snowflake_sql_api.testing import FakeSnowflake, make_client


@pytest.fixture
def stub_from_env(monkeypatch: pytest.MonkeyPatch) -> FakeSnowflake:
    """Point ``SnowflakeClient.from_env`` at a fresh FakeSnowflake-backed client.

    Returns the fake so a test can register results before invoking the CLI.
    """
    fake = FakeSnowflake()

    def _from_env() -> SnowflakeClient:
        return make_client(fake)

    monkeypatch.setattr(SnowflakeClient, "from_env", staticmethod(_from_env))
    return fake


# ---------------------------------------------------------------------------
# build_parser / argv plumbing
# ---------------------------------------------------------------------------


def test_build_parser_parses_query() -> None:
    args = cli.build_parser().parse_args(["query", "SELECT 1"])
    assert args.command == "query"
    assert args.sql == "SELECT 1"


def test_version(capsys: pytest.CaptureFixture[str]) -> None:
    # argparse's `version` action prints then raises SystemExit(0).
    from snowflake_sql_api import __version__

    with pytest.raises(SystemExit) as excinfo:
        cli.main(["--version"])
    assert excinfo.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_no_subcommand_prints_help_and_exits_2(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert cli.main([]) == 2
    out = capsys.readouterr().out
    assert "usage:" in out
    assert "query" in out


# ---------------------------------------------------------------------------
# query command (stubbed client)
# ---------------------------------------------------------------------------


def test_query_happy_path_prints_json(
    stub_from_env: FakeSnowflake, capsys: pytest.CaptureFixture[str]
) -> None:
    stub_from_env.register(
        "SELECT id, name FROM users",
        [{"ID": 1, "NAME": "alice"}, {"ID": 2, "NAME": "bob"}],
    )
    assert cli.main(["query", "SELECT id, name FROM users"]) == 0
    out = capsys.readouterr().out
    assert json.loads(out) == [
        {"ID": 1, "NAME": "alice"},
        {"ID": 2, "NAME": "bob"},
    ]


def test_query_serializes_non_json_native_values(
    stub_from_env: FakeSnowflake, capsys: pytest.CaptureFixture[str]
) -> None:
    # Decimal/timestamp/binary travel through _json_default on the way to stdout.
    stub_from_env.register(
        "SELECT amount, ts, blob",
        [
            {
                "AMOUNT": Decimal("10.50"),
                "TS": datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
                "BLOB": b"\x00\xff",
            }
        ],
    )
    assert cli.main(["query", "SELECT amount, ts, blob"]) == 0
    row = json.loads(capsys.readouterr().out)[0]
    assert row["AMOUNT"] == "10.50"
    assert row["TS"].startswith("2024-01-02T03:04:05")
    assert row["BLOB"] == "00ff"


def test_query_snowflake_error_exits_1(
    stub_from_env: FakeSnowflake, capsys: pytest.CaptureFixture[str]
) -> None:
    stub_from_env.register_error(
        "SELECT * FROM nope", "Object 'NOPE' does not exist", code="002003"
    )
    assert cli.main(["query", "SELECT * FROM nope"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("error: ")
    assert "does not exist" in captured.err


# ---------------------------------------------------------------------------
# _json_default serializer (direct unit coverage, incl. the TypeError branch)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("1.25"), "1.25"),
        (datetime(2024, 6, 1, 12, 30, 0), "2024-06-01T12:30:00"),
        (date(2024, 6, 1), "2024-06-01"),
        (time(9, 15, 30), "09:15:30"),
        (b"\xde\xad", "dead"),
        (bytearray(b"\xbe\xef"), "beef"),
    ],
)
def test_json_default_serializes(value: object, expected: str) -> None:
    assert cli._json_default(value) == expected


def test_json_default_rejects_unsupported() -> None:
    with pytest.raises(TypeError, match="cannot serialize"):
        cli._json_default(object())
