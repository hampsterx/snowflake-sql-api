"""Exercise the release gate in subprocesses with synthetic tests, never Snowflake."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PRIVATE_KEY_PATH",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_LIVE_DML",
)
FILES = ("test_smoke.py", "test_submissions.py", "test_identifier_case.py")


@pytest.fixture
def smoke_tree(tmp_path, monkeypatch):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    shutil.copyfile(ROOT / "scripts" / "live_smoke.py", scripts / "live_smoke.py")
    smoke = tmp_path / "tests" / "smoke"
    smoke.mkdir(parents=True)
    for name in FILES:
        (smoke / name).write_text("def test_probe():\n    assert True\n")
    # No credentials are read or used; even the accepted path runs synthetic tests.
    for key in list(os.environ):
        if key.startswith(("SNOWFLAKE_", "PYTEST_")):
            monkeypatch.delenv(key)
    for key in REQUIRED:
        monkeypatch.setenv(key, "1" if key == "SNOWFLAKE_LIVE_DML" else "synthetic")
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    return tmp_path


def run_smoke(tree):
    return subprocess.run(
        [sys.executable, str(tree / "scripts" / "live_smoke.py")],
        cwd=tree.parent,  # The runner must resolve its suite independently of cwd.
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.parametrize("missing", REQUIRED)
def test_live_smoke_requires_explicit_environment(smoke_tree, monkeypatch, missing):
    monkeypatch.delenv(missing)
    result = run_smoke(smoke_tree)
    assert result.returncode == 2
    assert missing in result.stderr
    assert "test session starts" not in result.stdout


@pytest.mark.parametrize("value", ["0", "true", "yes", ""])
def test_live_smoke_requires_dml_one(smoke_tree, monkeypatch, value):
    monkeypatch.setenv("SNOWFLAKE_LIVE_DML", value)
    result = run_smoke(smoke_tree)
    assert result.returncode == 2
    assert "SNOWFLAKE_LIVE_DML=1" in result.stderr


@pytest.mark.parametrize("missing", REQUIRED)
def test_live_smoke_rejects_blank_values(smoke_tree, monkeypatch, missing):
    monkeypatch.setenv(missing, "   ")
    result = run_smoke(smoke_tree)
    assert result.returncode == 2
    assert missing in result.stderr
    assert "test session starts" not in result.stdout


def test_live_smoke_refuses_ambient_selection(smoke_tree, monkeypatch):
    monkeypatch.setenv("PYTEST_ADDOPTS", "-k nothing")
    result = run_smoke(smoke_tree)
    assert result.returncode == 2
    assert "PYTEST_ADDOPTS" in result.stderr


def test_live_smoke_all_pass(smoke_tree):
    (smoke_tree / "pytest.ini").write_text("[pytest]\naddopts = -k nothing\n")
    result = run_smoke(smoke_tree)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "3 passed" in result.stdout


@pytest.mark.parametrize("name", FILES)
@pytest.mark.parametrize(
    "source",
    [
        "import pytest\npytest.skip('synthetic', allow_module_level=True)\n",
        "import pytest\n@pytest.mark.skip(reason='synthetic')\ndef test_probe(): pass\n",
        "import pytest\ndef test_probe(): pytest.skip('synthetic')\n",
        "import pytest\n@pytest.mark.xfail\ndef test_probe(): assert False\n",
        "import pytest\n@pytest.mark.xfail\ndef test_probe(): pass\n",
        "def test_probe(): assert False\n",
        "# No probes collected.\n",
    ],
)
def test_live_smoke_incomplete_file_fails(smoke_tree, name, source):
    (smoke_tree / "tests" / "smoke" / name).write_text(source)
    result = run_smoke(smoke_tree)
    assert result.returncode != 0, result.stdout + result.stderr


def test_live_smoke_deselection_fails(smoke_tree):
    (smoke_tree / "tests" / "smoke" / "test_smoke.py").write_text(
        "def test_a(): pass\ndef test_b(): pass\n"
    )
    (smoke_tree / "conftest.py").write_text(
        "def pytest_collection_modifyitems(config, items):\n"
        "    config.hook.pytest_deselected(items=[items.pop(0)])\n"
    )
    result = run_smoke(smoke_tree)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "incomplete" in result.stdout
