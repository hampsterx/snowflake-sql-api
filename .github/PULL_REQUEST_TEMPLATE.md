## Summary

What changed and why. Write for the reviewer and for downstream consumers, not
for yourself: lead with the user-visible behaviour, not the implementation
narrative.

## Backwards compatibility

This is a library; downstream code depends on it. State the impact:

- [ ] No public API change, or
- [ ] Public API changed (describe it, and the migration for callers)

Do not add compatibility shims for behaviour that never shipped; just change the
code.

## Test plan

- [ ] `coverage run -m pytest && coverage report` passes (coverage holds >= 89%)
- [ ] `ruff check`, `black --check`, and `mypy` pass (or `pre-commit run --all-files`)
- [ ] New behaviour is covered by tests; each fixed bug has a `test_regression_*`
- [ ] Public API changes documented in the README / `docs/`
