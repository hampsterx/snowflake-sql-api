"""Retry policy for transport operations.

Defines which conditions are retryable (connect/read timeout, HTTP 429, 5xx,
status-poll, partition fetch) and the backoff schedule. Retries reuse the
statement ``requestId`` and add ``retry=true`` after the initial attempt.
Snowflake uses retained statement history to resolve an identical submission;
this is not an unlimited deduplication or exactly-once guarantee. The transport
owns this wiring.
"""

from __future__ import annotations

import random
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
        # Submissions retain their requestId and use retry=true after a timeout.
        default_factory=lambda: frozenset({408, 429, 500, 502, 503, 504})
    )

    def ceiling_for(self, attempt: int) -> float:
        """Return the un-jittered backoff ceiling for a 0-based retry ``attempt``.

        Exponential and capped: ``min(base_backoff * multiplier**attempt,
        max_backoff)``. ``attempt`` 0 is the delay before the first retry.
        """
        if attempt < 0:
            raise ValueError("attempt must be >= 0")
        return min(self.base_backoff * (self.multiplier**attempt), self.max_backoff)

    def backoff_for(self, attempt: int) -> float:
        """Return the delay in seconds before a 0-based retry ``attempt``.

        With ``jitter`` (the default) this is "full jitter": a uniform random
        value in ``[0, ceiling]``, which de-correlates retries from concurrent
        clients. Without jitter it is the ceiling exactly.
        """
        ceiling = self.ceiling_for(attempt)
        if self.jitter:
            return random.uniform(0.0, ceiling)
        return ceiling

    def should_retry_status(self, status_code: int) -> bool:
        """Whether an HTTP ``status_code`` is in the retryable set."""
        return status_code in self.retry_statuses


#: Default policy used by clients that do not pass their own.
DEFAULT_RETRY_POLICY = RetryPolicy()
