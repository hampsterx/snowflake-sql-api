# Releasing

`snowflake-sql-api` is released by pushing a git tag. There is no version to
hand-edit and no manual upload step.

## Version source

The version comes from the git tag via [`hatch-vcs`](https://github.com/ofek/hatch-vcs).
At build time the resolved version is written to `snowflake_sql_api/_version.py`
(gitignored). **Do not hand-edit a version anywhere** (not in `__init__.py`, not
in `pyproject.toml`); a feature PR that edits a version string is wrong.

Tag convention: `vX.Y.Z` (PEP 440 pre-releases are fine too, e.g. `v0.2.0rc1`).

## Cutting a release

From the clean commit you intend to tag, install the development dependencies
(`pip install -e '.[dev]'`) and complete the [pre-tag live smoke](#pre-tag-live-smoke)
before creating or pushing the tag. A skipped smoke is not release evidence.

```bash
# from a clean master that has the commits you want to ship
git fetch origin
git switch master && git pull --ff-only

# after configuring the disposable namespace described below
python scripts/live_smoke.py
# continue only if the gate exits 0; tag the same commit that passed
git tag v0.1.0
git push origin v0.1.0
```

Pushing the tag triggers `.github/workflows/publish.yml`, which:

1. Runs the lint + test matrix (same checks as CI).
2. Builds the sdist and wheel (`python -m build`); the version reflects the tag.
3. Runs `twine check dist/*`.
4. Publishes to PyPI via OIDC trusted publishing (no API token).
5. Creates a GitHub Release for the tag with notes auto-generated from the merged
   PRs since the previous tag.

A GitHub Release is part of every release, not an optional extra: the workflow
creates one automatically (step 5). After the run, edit the release on GitHub to
add a short highlights section if the auto-generated PR list needs framing. For
the very first release (no prior tag to diff against), write the notes by hand.

## Pre-tag live smoke

Use a disposable Snowflake test namespace. The suite spends warehouse time and
creates and drops uniquely named tables and a stored procedure; the selected role
needs `CREATE TABLE` and `CREATE PROCEDURE` on the schema, plus access to the
warehouse and namespace.

Use a test identity whose sessions have
`QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE`. The lowercase rejection probe skips
when this setting is true, which fails the combined gate.

Set these variables in your shell before running the gate:

- `tests/smoke/test_smoke.py`: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`,
  `SNOWFLAKE_PRIVATE_KEY_PATH`; a warehouse for the synthetic `GENERATOR` probes
  (explicit or the user's default).
- `tests/smoke/test_submissions.py`: the same credentials,
  `SNOWFLAKE_LIVE_DML=1`, and explicit `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`,
  `SNOWFLAKE_WAREHOUSE` for the disposable namespace.
- `tests/smoke/test_identifier_case.py`: the same requirements as
  `test_submissions.py`.

The combined gate requires all six named credential/namespace variables, plus
`SNOWFLAKE_LIVE_DML=1`, even if the user has defaults. All three files' opt-in
checks require the key path. `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`, `SNOWFLAKE_ROLE`
and `SNOWFLAKE_HOST` are optional for every file. Only `test_smoke.py` uses
`from_env()`: it also accepts `SNOWFLAKE_PRIVATE_KEY` (inline PEM, taking precedence
over the path); the two writing files read the key from the path directly. Use
credentials for the intended test identity and keep keys out of git.

From a checkout with the development dependencies installed, run:

```bash
python scripts/live_smoke.py
```

The runner locates all three files relative to its checkout and refuses missing
variables or a nonempty `PYTEST_ADDOPTS` before starting pytest. It clears pytest's
configured `addopts` and accepts no test-selection arguments. Success means exit
code 0 with every collected probe passing in all three files, including the sync
and async cases. A skip during collection, setup or execution, an expected failure
(or unexpected pass), a deselected test, or a file with no collected probes makes
the gate fail. Test and collection failures also remain failures. Resolve the
reported issue and rerun the whole gate before tagging; retain its output with the
release evidence. Ordinary `pytest tests/smoke` still allows opt-in skips and does
not enforce this release gate.

## One-time prerequisites

Trusted publishing must be configured before the first real release:

1. **PyPI trusted publisher**: on the PyPI project, add a GitHub publisher for
   `hampsterx/snowflake-sql-api`, workflow `publish.yml`, environment `pypi`.
   (For the very first upload, create the project via a TestPyPI dry-run or a
   pending publisher.)
2. **GitHub environment**: create an environment named `pypi` in the repo
   settings. It can be empty; it exists to gate the publish job and bind the
   trusted-publisher claim.

## TestPyPI dry-run (optional)

To rehearse without touching the real index, build locally and upload to
TestPyPI with a token:

```bash
python -m build
twine check dist/*
twine upload --repository testpypi dist/*
```

## At v0.1.0

When cutting the first `v0.1.0`, flip the `Development Status` classifier in
`pyproject.toml` from `3 - Alpha` to `4 - Beta` in the same commit that precedes
the tag.

## Fixing a bad tag

```bash
git tag -d v0.1.0
git push origin :refs/tags/v0.1.0
```

Re-tag only if nothing was published yet; PyPI does not allow re-uploading a
version that already exists.
