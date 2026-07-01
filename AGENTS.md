# AGENTS.md

Canonical agent and contributor guidance for `snowflake-sql-api`. Read by Claude
Code (via `CLAUDE.md`), Codex CLI, and humans.

## Project Overview

`snowflake-sql-api` is a lightweight, pure-Python client for Snowflake's
[SQL API v2](https://docs.snowflake.com/en/developer-guide/sql-api/index) (the
`POST /api/v2/statements` REST endpoint). It exists as a small, fast alternative
to the official `snowflake-connector-python`, which is ~75 MB installed (150 MB
with pandas/pyarrow) and ships a C extension that slows container builds and
inflates cold starts (its import alone is ~1.5 s versus ~0.3 s for this client).
This client is pure Python over `httpx`, so it is small and quick to cold-start,
which matters in serverless / Lambda environments.

It offers both **synchronous and asynchronous** clients, keypair (JWT) auth, type
coercion, multi-partition result handling, a CLI, and optional pandas / typed-row
helpers.

> Clean-room reimplementation inspired by and API-compatible with the (now
> unmaintained) [`pps-19012/snowflake-rest`](https://github.com/pps-19012/snowflake-rest).
> No upstream code is copied; the design and API shape are credited to its author.
> Licensed MIT.

## Architecture

Module layout under `snowflake_sql_api/` (clean separation, `py.typed`):

| Module | Responsibility |
|--------|----------------|
| `auth.py` | Keypair JWT (RS256) generation, token cache, account-locator normalization (strips region/cloud suffix) |
| `transport.py` | `httpx` sync + async request execution against `/api/v2/statements`, retry wiring |
| `types.py` | Result-set type coercion (VARIANT, Decimal, datetime/date/time) |
| `escaping.py`, `bindings.py` | Parameter binding, bind-value escaping, identifier quoting |
| `pagination.py` | Multi-partition result fetching (the SQL API splits large results into partitions) |
| `exceptions.py` | Typed exception hierarchy |
| `client.py` | Synchronous `SnowflakeClient` (query helpers, DML, batch insert, async-submit handle) |
| `aclient.py` | Asynchronous client (same surface, `await`-based) |
| `row_mapping.py` | Optional dataclass / Pydantic row mapping |
| `cli.py` | Command-line interface (`snowflake-sql-api query ...`) |
| `testing.py` | Shipped test helper: `FakeSnowflake` (httpx.MockTransport), `make_client`/`make_async_client`, pytest fixtures (pytest11) |

### Dependencies

- **Core** (always installed): `httpx`, `PyJWT`, `cryptography`.
- **Extras**: `[pandas]` (DataFrame output), `[pydantic]` (typed row mapping),
  `[dev]` (test/lint/build tooling).

Heavy features sit behind extras so the default install stays small. Optional
imports are guarded: using a feature without its extra raises a clear error rather
than failing at import time.

## Development Commands

```bash
# Install in editable mode with dev tooling, then wire the git hooks
pip install -e '.[dev]'
pre-commit install

# Run tests with coverage (NOT `pytest --cov`; see Known Quirks)
coverage run -m pytest && coverage report

# Run only the known-bug regression tests
pytest -k regression

# Lint / format / type-check (or run all hooks at once)
ruff check snowflake_sql_api tests
black --check snowflake_sql_api tests
mypy snowflake_sql_api
pre-commit run --all-files

# Build a distribution (version comes from the git tag via hatch-vcs)
python -m build
```

### Live tests

Tests under `tests/smoke/` hit a real Snowflake account and need keypair
credentials via environment variables (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`,
`SNOWFLAKE_PRIVATE_KEY_PATH`). They are skipped when those are unset. Never commit
private keys, `.gitignore` excludes `*.pem` / `*.p8` / `*private_key*`.

## Design Principles

- **Pure Python, small footprint.** No compiled extensions. New dependencies need
  a strong justification, the size/cold-start advantage is the whole point.
- **Generic and vendor-neutral.** No assumptions about a specific account, region,
  role, warehouse, timezone, or schema. Accept these as parameters or environment
  configuration; never hardcode them.
- **Sync and async parity.** A feature added to the sync client should have an
  async equivalent (or a clear reason it does not).
- **Correctness over surface.** Type coercion and partition handling are
  correctness-critical, prefer well-tested core behavior to breadth.

## Known Quirks

Behaviour that looks wrong but is intentional. Do not "fix" these without reading
the linked regression test first.

- **Account locator: claim vs host** (`auth.py`). The JWT claim account
  (`iss`/`sub`) strips the region/cloud suffix and uppercases
  (`xy12345.ap-southeast-2` -> `XY12345`); the API host keeps the full account
  (`xy12345.ap-southeast-2.snowflakecomputing.com`). Conflating them breaks JWT
  validation. `normalize_account_locator` vs `account_hostname`. Regression:
  `test_regression_bug1`.
- **`result(poll=False)` raises on 202** (`client.py` / `aclient.py`
  `_collect`). A still-running async statement must raise `ResultNotReady`, never
  return its in-progress HTTP 202 body as if it were a result set. Regression:
  `test_regression_bug3`.
- **Fetch every partition, in order** (`pagination.py`). `query` returns
  partition 0 (inline) plus partitions 1..N (fetched by index). Stopping at
  partition 0 silently truncates large results. Regression:
  `test_regression_bug4`.
- **`on_query` streaming hook is deferred** to the v0.2.0 toolkit
  (`query_stream`, Phase 8). The hook fires for `query`/`execute`/`submit` today;
  there is no streaming path yet, so no regression test until the feature lands
  (this is spike bug #2, intentionally not yet covered).
- **No PEP 604 unions at runtime** (py3.9 floor). ruff's `UP` (pyupgrade) rule is
  omitted on purpose: it would rewrite `Optional[...]` / `Union[...]` to
  `X | None`, which raises at import time on 3.9 for typing generics (PEP 604 on
  generics is 3.10+). Keep `from __future__ import annotations` plus
  `Optional`/`Union`.
- **mypy `python_version = "3.10"` vs the 3.9-3.13 matrix.** 3.10 is the lowest
  this mypy accepts; true 3.9 runtime compatibility is enforced by the pytest
  matrix, which imports every module under 3.9.
- **Coverage uses `coverage run`, not `pytest --cov`.** The package ships a pytest
  plugin via the `pytest11` entry point, so `snowflake_sql_api.testing` (and the
  whole package) is imported at plugin-load time, before pytest-cov starts
  tracing. `pytest --cov` then reports import-time lines as uncovered (~20 points
  lost). `coverage run -m pytest` starts tracing first. The 89% gate lives in
  `pyproject.toml` `[tool.coverage.report] fail_under`.

## Testing

- Unit tests mock the HTTP layer; no network access required for the default suite.
- Mock the client in your own tests with the shipped `snowflake_sql_api.testing`
  helper (`FakeSnowflake` + `make_client`/`make_async_client`, or the
  auto-registered `fake_snowflake` / `snowflake_client` / `async_snowflake_client`
  fixtures). No respx. See `docs/testing.md`.
- Each fixed bug gets a named regression test (`test_regression_*`) so it cannot
  silently return.
- Target coverage: >= 89%, enforced across Python 3.9-3.13. Run with
  `coverage run -m pytest && coverage report` (see Known Quirks).

## Common Mistakes

- Hand-editing a version string. The version comes from the git tag (hatch-vcs);
  `_version.py` is generated and gitignored. A feature PR must not touch it. See
  `RELEASING.md`.
- Running `pytest --cov` and reacting to the false coverage drop. Use
  `coverage run -m pytest`.
- Adding a runtime dependency without strong justification. The small
  install / fast cold start is the whole point; new optional features go behind
  an extra.
- Rewriting `Optional[...]` to `X | None` (breaks the 3.9 runtime).
- Conflating the JWT claim account with the API host (see Known Quirks).
- Forgetting the async counterpart of a sync change (sync/async parity).
- Forgetting a `test_regression_*` for a fixed bug.

## Before Finishing

1. `pre-commit run --all-files` is clean (ruff, black, mypy, yaml/toml, private-key).
2. `coverage run -m pytest && coverage report` passes and coverage holds >= 89%.
3. Sync/async parity: any client change has its counterpart, or a stated reason.
4. Fixed bugs have a `test_regression_*`; public API changes are in the
   README / `docs/`.

## Security

Report vulnerabilities privately, see [SECURITY.md](SECURITY.md). Never commit
private keys (`.gitignore` and a `detect-private-key` pre-commit hook guard
this).

## Contributing

Before opening a PR:

- [ ] Tests pass (`coverage run -m pytest && coverage report`) and coverage holds.
- [ ] Formatted (`black`) and linted (`ruff`), type hints on public APIs (`mypy`).
- [ ] No hardcoded account/region/role/warehouse values; configuration is generic.
- [ ] Public API changes documented in the README.
- [ ] New behavior covered by tests; fixed bugs have a regression test.
