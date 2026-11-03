"""Tests for the retry backoff policy."""

from __future__ import annotations

import random
from dataclasses import FrozenInstanceError

import pytest

from snowflake_sql_api.retry import DEFAULT_RETRY_POLICY, RetryPolicy


def test_ceiling_is_exponential() -> None:
    p = RetryPolicy(base_backoff=0.5, multiplier=2.0, max_backoff=100.0)
    assert p.ceiling_for(0) == 0.5
    assert p.ceiling_for(1) == 1.0
    assert p.ceiling_for(2) == 2.0
    assert p.ceiling_for(3) == 4.0


def test_ceiling_is_capped() -> None:
    p = RetryPolicy(base_backoff=1.0, multiplier=10.0, max_backoff=5.0)
    assert p.ceiling_for(0) == 1.0
    assert p.ceiling_for(1) == 5.0  # 10.0 capped to 5.0
    assert p.ceiling_for(5) == 5.0


@pytest.mark.parametrize("method", ["ceiling_for", "backoff_for", "delay_for"])
def test_negative_attempt_rejected(method: str) -> None:
    with pytest.raises(ValueError):
        getattr(DEFAULT_RETRY_POLICY, method)(-1)


def test_backoff_without_jitter_is_exact() -> None:
    p = RetryPolicy(base_backoff=0.5, multiplier=2.0, jitter=False)
    assert p.backoff_for(2) == 2.0


def test_backoff_with_jitter_stays_within_bounds() -> None:
    p = RetryPolicy(base_backoff=0.5, multiplier=2.0, jitter=True, max_backoff=8.0)
    random.seed(1234)
    for attempt in range(6):
        ceiling = p.ceiling_for(attempt)
        for _ in range(50):
            value = p.backoff_for(attempt)
            assert 0.0 <= value <= ceiling


def test_should_retry_status() -> None:
    p = RetryPolicy()
    assert p.should_retry_status(429)
    assert p.should_retry_status(503)
    assert not p.should_retry_status(200)
    assert not p.should_retry_status(422)


def test_policy_is_frozen() -> None:
    with pytest.raises(FrozenInstanceError):
        DEFAULT_RETRY_POLICY.max_attempts = 99  # type: ignore[misc]


def test_regression_429_has_longer_default_schedule() -> None:
    p = RetryPolicy(jitter=False)
    assert [p.delay_for(i, status_code=429) for i in range(2)] == [5.0, 8.0]
    assert [p.delay_for(i, status_code=503) for i in range(2)] == [0.5, 1.0]
    assert p.max_attempts == 3
    assert RetryPolicy(base_backoff=0).delay_for(0, status_code=429) == 0
    assert (
        RetryPolicy(rate_limit_backoff_multiplier=1, jitter=False).delay_for(
            0, status_code=429
        )
        == 0.5
    )


@pytest.mark.parametrize(
    "header, expected",
    [("12", 12.0), (" 7 ", 7.0), ("0", 0.0), ("999", 60.0), ("9" * 5000, 60.0)],
)
def test_retry_after_seconds_override_jitter(
    header: str, expected: float, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        random, "uniform", lambda *_: pytest.fail("server delay must not be jittered")
    )
    assert RetryPolicy().delay_for(0, status_code=429, retry_after=header) == expected


@pytest.mark.parametrize(
    "header",
    [
        "",
        "-1",
        "1.5",
        "+3",
        "NaN",
        "inf",
        "١٢",
        "later",
        "5, 10",
        "Wed, 99 Nov 2026 00:00:00 GMT",
    ],
)
def test_invalid_retry_after_uses_status_backoff(header: str) -> None:
    assert (
        RetryPolicy(jitter=False).delay_for(0, status_code=429, retry_after=header) == 5
    )


@pytest.mark.parametrize(
    "header, expected",
    [
        ("Wed, 04 Nov 2026 00:00:12 GMT", 12),
        ("Wednesday, 04-Nov-26 00:00:12 GMT", 12),
        ("Wed Nov  4 00:00:12 2026", 12),
        ("Tue, 03 Nov 2026 00:00:00 GMT", 0),
        ("Thu, 05 Nov 2026 00:00:00 GMT", 60),
    ],
)
def test_retry_after_http_date(
    header: str, expected: float, monkeypatch: pytest.MonkeyPatch
) -> None:
    from datetime import datetime, timezone

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 4, tzinfo=timezone.utc)

    monkeypatch.setattr("snowflake_sql_api.retry.datetime", Clock)
    assert RetryPolicy().delay_for(0, retry_after=header) == expected


def test_retry_after_cap_is_independent_of_backoff_cap() -> None:
    p = RetryPolicy(max_backoff=0, max_retry_after=15)
    assert p.delay_for(0, retry_after="14") == 14
    assert p.delay_for(0, retry_after="16") == 15
    assert RetryPolicy(max_retry_after=0).delay_for(0, retry_after="5") == 0


def test_429_uses_full_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    bounds = []

    def uniform(low, high):
        bounds.append((low, high))
        return high / 2

    monkeypatch.setattr(random, "uniform", uniform)
    assert RetryPolicy().delay_for(1, status_code=429) == 4
    assert bounds == [(0, 8)]


@pytest.mark.parametrize("field", ["rate_limit_backoff_multiplier", "max_retry_after"])
@pytest.mark.parametrize("value", [-1, float("nan"), float("inf")])
def test_new_policy_limits_must_be_finite_and_nonnegative(
    field: str, value: float
) -> None:
    with pytest.raises(ValueError, match=field):
        RetryPolicy(**{field: value})


def test_existing_positional_policy_constructor() -> None:
    p = RetryPolicy(4, 0.25, 6, 3, False, frozenset({503}))
    assert p.backoff_for(1) == 0.75
    assert p.retry_statuses == frozenset({503})


def test_legacy_backoff_hook_keeps_its_signature() -> None:
    class CustomPolicy(RetryPolicy):
        def backoff_for(self, attempt: int) -> float:
            return 3.0

    assert CustomPolicy().delay_for(0, status_code=503) == 3.0
    assert CustomPolicy().delay_for(0) == 3.0
