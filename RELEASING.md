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

```bash
# from a clean master that has the commits you want to ship
git fetch origin
git switch master && git pull --ff-only

git tag v0.1.0
git push origin v0.1.0
```

Pushing the tag triggers `.github/workflows/publish.yml`, which:

1. Runs the lint + test matrix (same checks as CI).
2. Builds the sdist and wheel (`python -m build`); the version reflects the tag.
3. Runs `twine check dist/*`.
4. Publishes to PyPI via OIDC trusted publishing (no API token).

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
