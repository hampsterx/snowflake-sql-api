#!/usr/bin/env python3
"""Report the cold-import time and source size of ``snowflake_sql_api``.

Manual / CI-informational tool, not a gate (the hard regression guard lives in
``tests/test_import_footprint.py``). The whole point of this client is a small
install and a fast cold start; this prints both so a regression is visible.

Run from the repo (the script locates the package relative to itself, so no
install is required)::

    python scripts/benchmark.py            # default 5 import runs
    python scripts/benchmark.py --runs 10
"""

from __future__ import annotations

import argparse
import pathlib
import statistics
import subprocess
import sys

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
_PACKAGE_DIR = _REPO_ROOT / "snowflake_sql_api"

_TIMING_SCRIPT = """
import time
start = time.perf_counter()
import snowflake_sql_api  # noqa: F401
print(time.perf_counter() - start)
"""


def cold_import_seconds(runs: int) -> list:
    """Time ``import snowflake_sql_api`` in a fresh subprocess, ``runs`` times.

    Runs with cwd at the repo root so the package imports from the source tree
    even without an editable install.
    """
    timings = []
    for _ in range(runs):
        result = subprocess.run(
            [sys.executable, "-c", _TIMING_SCRIPT],
            capture_output=True,
            text=True,
            check=True,
            cwd=_REPO_ROOT,
        )
        timings.append(float(result.stdout.strip()))
    return timings


def source_size_bytes() -> int:
    """Sum the on-disk size of the package's source files (excludes bytecode)."""
    return sum(
        path.stat().st_size
        for path in _PACKAGE_DIR.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runs", type=int, default=5, help="import timing samples (default: 5)"
    )
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be >= 1")

    timings = cold_import_seconds(args.runs)
    size_kb = source_size_bytes() / 1024

    print(f"cold import (n={args.runs}):")
    print(f"  median: {statistics.median(timings) * 1000:.1f} ms")
    print(f"  min:    {min(timings) * 1000:.1f} ms")
    print(f"  max:    {max(timings) * 1000:.1f} ms")
    print(f"source size: {size_kb:.1f} KiB (package only, excludes deps)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
