"""Run the complete pre-release live suite; skipped probes fail the gate."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SMOKE_FILES = tuple(
    ROOT / "tests" / "smoke" / name
    for name in (
        "test_smoke.py",
        "test_submissions.py",
        "test_identifier_case.py",
        "test_multi_statement.py",
    )
)
REQUIRED = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_WAREHOUSE",
)


class CompleteSmoke:
    def __init__(self) -> None:
        self.selected: set[str] = set()
        self.files: set[Path] = set()
        self.passed: set[str] = set()
        self.incomplete = False

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        self.selected = {item.nodeid for item in session.items}
        self.files = {item.path.resolve() for item in session.items}

    def pytest_deselected(self, items: list[pytest.Item]) -> None:
        self.incomplete = True

    def pytest_collectreport(self, report: pytest.CollectReport) -> None:
        self.incomplete |= report.skipped

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        self.incomplete |= report.skipped or hasattr(report, "wasxfail")
        if report.when == "call" and report.passed:
            self.passed.add(report.nodeid)

    def pytest_sessionfinish(self, session: pytest.Session) -> None:
        if (
            self.incomplete
            or not set(SMOKE_FILES) <= self.files
            or not self.selected
            or self.passed != self.selected
        ):
            print("Live smoke incomplete: every probe in all required files must pass.")
            session.exitstatus = pytest.ExitCode.TESTS_FAILED


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--junitxml", type=Path, help="write private probe observations to JUnit XML"
    )
    options = parser.parse_args()
    missing = [name for name in REQUIRED if not os.environ.get(name, "").strip()]
    if os.environ.get("SNOWFLAKE_LIVE_DML") != "1":
        missing.append("SNOWFLAKE_LIVE_DML=1")
    if missing:
        print("Live smoke requires: " + ", ".join(missing), file=sys.stderr)
        return 2
    if os.environ.get("PYTEST_ADDOPTS", "").strip():
        print("Unset PYTEST_ADDOPTS to run the complete live smoke.", file=sys.stderr)
        return 2
    evidence = (
        ["-o", "junit_family=legacy", f"--junitxml={options.junitxml.resolve()}"]
        if options.junitxml is not None
        else []
    )
    return int(
        pytest.main(
            [
                "-o",
                "addopts=",
                "--strict-markers",
                "-v",
                "-rs",
                *evidence,
                *map(str, SMOKE_FILES),
            ],
            plugins=[CompleteSmoke()],
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
