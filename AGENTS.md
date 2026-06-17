# AGENTS.md

Canonical agent and contributor guidance for `snowflake-sql-api`. Read by Claude
Code (via `CLAUDE.md`), Codex CLI, and humans.

## Project Overview

`snowflake-sql-api` is a lightweight, pure-Python client for Snowflake's
[SQL API v2](https://docs.snowflake.com/en/developer-guide/sql-api/index) (the
`POST /api/v2/statements` REST endpoint). It exists as a small, fast alternative
to the official `snowflake-connector-python`, which is ~75 MB installed (150 MB
with pandas/pyarrow), has an 8-20 s cold start, and ships a C extension that slows
container builds. This client is pure Python over `httpx`, so it is small and
quick to cold-start, which matters in serverless / Lambda environments.

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

### Dependencies

- **Core** (always installed): `httpx`, `PyJWT`, `cryptography`.
- **Extras**: `[pandas]` (DataFrame output), `[pydantic]` (typed row mapping),
  `[dev]` (test/lint/build tooling).

Heavy features sit behind extras so the default install stays small. Optional
imports are guarded: using a feature without its extra raises a clear error rather
than failing at import time.

## Development Commands

```bash
# Install in editable mode with dev tooling
pip install -e '.[dev]'

# Run tests with coverage
pytest --cov=snowflake_sql_api --cov-report=term-missing

# Run only the known-bug regression tests
pytest -k regression

# Lint / format / type-check
ruff check snowflake_sql_api tests
black --check snowflake_sql_api tests
mypy snowflake_sql_api

# Build a distribution
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

## Testing

- Unit tests mock the HTTP layer; no network access required for the default suite.
- Each fixed bug gets a named regression test (`test_regression_*`) so it cannot
  silently return.
- Target coverage: >= 89%, enforced in CI across Python 3.9-3.13.

## Contributing

Before opening a PR:

- [ ] Tests pass (`pytest --cov`) and coverage holds.
- [ ] Formatted (`black`) and linted (`ruff`), type hints on public APIs (`mypy`).
- [ ] No hardcoded account/region/role/warehouse values; configuration is generic.
- [ ] Public API changes documented in the README.
- [ ] New behavior covered by tests; fixed bugs have a regression test.
