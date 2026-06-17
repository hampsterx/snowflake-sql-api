# Contributing

Thanks for your interest in `snowflake-sql-api`. This is a small, focused
library; contributions that keep it small and correct are very welcome.

## Development setup

```bash
git clone https://github.com/hampsterx/snowflake-sql-api
cd snowflake-sql-api
python -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
```

## Before opening a PR

```bash
ruff check snowflake_sql_api tests
black --check snowflake_sql_api tests
mypy snowflake_sql_api
pytest --cov=snowflake_sql_api --cov-report=term-missing
```

All four must pass. CI runs the same checks across Python 3.9-3.13.

Checklist:

- [ ] Tests pass and coverage holds (target >= 89%).
- [ ] Formatted (`black`) and linted (`ruff`); public APIs have type hints (`mypy`).
- [ ] No hardcoded account/region/role/warehouse values - configuration is generic.
- [ ] Public API changes are documented in the README.
- [ ] New behavior is covered by tests; each fixed bug gets a named regression
      test (`test_regression_*`) so it cannot silently return.

## Design principles

- **Pure Python, small footprint.** No compiled extensions. A new dependency
  needs a strong justification - the size and cold-start advantage over the
  official connector is the whole point.
- **Generic and vendor-neutral.** No assumptions about a specific account,
  region, role, warehouse, timezone, or schema. Accept these as parameters or
  environment configuration; never hardcode them.
- **Sync and async parity.** A feature added to the sync client should have an
  async equivalent, or a clear reason it does not.
- **Correctness over surface.** Type coercion and partition handling are
  correctness-critical; prefer well-tested core behavior to breadth.

## Live (smoke) tests

Tests under `tests/smoke/` hit a real Snowflake account and need keypair
credentials via environment variables (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`,
`SNOWFLAKE_PRIVATE_KEY_PATH`). They are skipped when those are unset. Never
commit private keys or expected output containing real business data.

## License

By contributing, you agree that your contributions are licensed under the MIT
License.
