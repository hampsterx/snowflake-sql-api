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


def test_negative_attempt_rejected() -> None:
    with pytest.raises(ValueError):
        DEFAULT_RETRY_POLICY.ceiling_for(-1)


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
