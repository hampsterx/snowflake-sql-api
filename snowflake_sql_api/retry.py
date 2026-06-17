"""Retry policy for transport operations.

Defines which conditions are retryable (connect/read timeout, HTTP 429, 5xx,
status-poll, partition fetch) and the backoff schedule. Retries reuse the
statement ``requestId`` so a retried DML submit cannot double-apply
(idempotency); the transport owns that wiring.

Scaffold only: the retry decision and backoff loop land in Phase 2.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import FrozenSet

__all__ = ["RetryPolicy", "DEFAULT_RETRY_POLICY"]


@dataclass(frozen=True)
class RetryPolicy:
    """Configuration for retrying transport requests.

    ``max_attempts`` counts the initial try, so ``3`` means up to two retries.
    Backoff is exponential: ``base_backoff * (multiplier ** attempt)``, capped
    at ``max_backoff``, with optional full jitter.
    """

    max_attempts: int = 3
    base_backoff: float = 0.5
    max_backoff: float = 8.0
    multiplier: float = 2.0
    jitter: bool = True
    retry_statuses: FrozenSet[int] = field(
        default_factory=lambda: frozenset({429, 500, 502, 503, 504})
    )

    def backoff_for(self, attempt: int) -> float:
        """Return the delay in seconds before the given 0-based retry attempt. (Phase 2.)"""
        raise NotImplementedError


#: Default policy used by clients that do not pass their own.
DEFAULT_RETRY_POLICY = RetryPolicy()
