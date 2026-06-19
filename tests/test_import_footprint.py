"""Guard the install footprint: a bare ``import snowflake_sql_api`` stays light.

The package's whole value proposition is a small install and a fast cold start
(serverless / Lambda). Two regressions would silently erode that:

1. A heavy optional dependency (pandas / pyarrow) getting imported at module top
   instead of behind its extra. The ``[pandas]`` feature must stay opt-in.
2. Import time ballooning for any reason.

Both checks run in a **fresh subprocess** so the parent pytest process's
already-imported modules can't mask a regression (pandas may well be imported by
the time these tests run). The leak check installs a recording meta-path finder
that stubs the forbidden names, so it fires deterministically whether or not the
real packages are installed: a module-top ``import pandas`` is caught even when
it is guarded by ``try/except ImportError`` and even in an env without pandas.
"""

from __future__ import annotations

import subprocess
import sys

# Heavy optional deps that a bare import must never pull in. pandas is the
# ``[pandas]`` extra; pyarrow/numpy ride in with it. If any is imported at module
# top, something bypassed the optional-import guard.
FORBIDDEN_ON_BARE_IMPORT = ("pandas", "pyarrow", "numpy")

# Generous ceiling: the real cold import (httpx + PyJWT + cryptography, all pure
# Python or fast C imports) is well under a second. This is a coarse backstop
# against an order-of-magnitude regression, not a precise benchmark, so the bound
# is deliberately loose and we assert on the best of several samples to stay
# non-flaky on shared CI runners.
IMPORT_BUDGET_SECONDS = 2.5
TIMING_SAMPLES = 3

# Subprocess: record any *attempt* to import a forbidden module during a bare
# `import snowflake_sql_api`. The recorder returns a stub module for forbidden
# names, so the import succeeds (and is recorded) regardless of whether the real
# dependency is installed -- making the guard deterministic across environments.
_LEAK_SCRIPT = """
import importlib.abc
import importlib.machinery
import sys
import types

FORBIDDEN = {forbidden!r}
attempted = []


class _Recorder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def find_spec(self, name, path, target=None):
        if name.split(".")[0] in FORBIDDEN:
            attempted.append(name.split(".")[0])
            return importlib.machinery.ModuleSpec(name, self)
        return None

    def create_module(self, spec):
        return types.ModuleType(spec.name)

    def exec_module(self, module):
        pass


sys.meta_path.insert(0, _Recorder())
import snowflake_sql_api  # noqa: F401
print(",".join(sorted(set(attempted))))
""".format(
    forbidden=FORBIDDEN_ON_BARE_IMPORT
)

_TIMING_SCRIPT = """
import time
start = time.perf_counter()
import snowflake_sql_api  # noqa: F401
print(time.perf_counter() - start)
"""


def _run(script: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True
    )


def test_bare_import_pulls_no_heavy_deps() -> None:
    result = _run(_LEAK_SCRIPT)
    assert (
        result.returncode == 0
    ), f"`import snowflake_sql_api` failed in a clean subprocess:\n{result.stderr}"
    attempted = [name for name in result.stdout.strip().split(",") if name]
    assert not attempted, (
        f"bare `import snowflake_sql_api` tried to import heavy deps: {attempted}. "
        "Keep optional dependencies behind their extra (lazy import)."
    )


def test_bare_import_is_fast() -> None:
    samples = []
    for _ in range(TIMING_SAMPLES):
        result = _run(_TIMING_SCRIPT)
        assert result.returncode == 0, result.stderr
        samples.append(float(result.stdout.strip()))
    best = min(samples)
    assert best < IMPORT_BUDGET_SECONDS, (
        f"fastest of {TIMING_SAMPLES} imports was {best:.3f}s, over the "
        f"{IMPORT_BUDGET_SECONDS}s budget. A heavy import likely crept into the "
        "module-load path."
    )
