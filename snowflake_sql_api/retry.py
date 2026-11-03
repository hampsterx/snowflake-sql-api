"""Retry policy for transport operations.

Defines which conditions are retryable (connect/read timeout, HTTP 429, 5xx,
status-poll, partition fetch) and the backoff schedule. Retries reuse the
statement ``requestId`` and add ``retry=true`` after the initial attempt.
Snowflake uses retained statement history to resolve an identical submission;
this is not an unlimited deduplication or exactly-once guarantee. The transport
owns this wiring.
"""

from __future__ import annotations

import math
import random
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import FrozenSet, Optional

__all__ = ["RetryPolicy", "DEFAULT_RETRY_POLICY"]


@dataclass(frozen=True)
class RetryPolicy:
    """Configuration for retrying transport requests.

    ``max_attempts`` counts the initial try, so ``3`` means up to two retries.
    Backoff is exponential: ``base_backoff * (multiplier ** attempt)``, capped
    at ``max_backoff``, with optional full jitter. HTTP 429 multiplies the base
    by ``rate_limit_backoff_multiplier`` before applying that cap. Valid
    ``Retry-After`` values override the schedule without jitter, capped by
    ``max_retry_after`` seconds per wait (not an overall request deadline).
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

    rate_limit_backoff_multiplier: float = 10.0
    max_retry_after: float = 60.0

    def __post_init__(self) -> None:
        for name in ("rate_limit_backoff_multiplier", "max_retry_after"):
            value = getattr(self, name)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and >= 0")

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

    def delay_for(
        self,
        attempt: int,
        *,
        status_code: Optional[int] = None,
        retry_after: Optional[str] = None,
    ) -> float:
        """Return a retry delay using the preceding retryable response.

        ``attempt`` is 0 before the first retry. Delay-seconds and HTTP dates
        take precedence over the schedule; past dates give zero delay. Invalid
        headers fall back to backoff. Callers must check retryability first.
        The existing ``backoff_for`` hook still handles ordinary backoff.
        """
        if attempt < 0:
            raise ValueError("attempt must be >= 0")
        if retry_after is not None:
            value = retry_after.strip(" \t")
            if re.fullmatch(r"[0-9]+", value):
                # Convert directly to float: huge valid integers become infinity
                # and are capped without hitting Python's integer digit limit.
                return min(float(value), self.max_retry_after)
            try:
                retry_at = parsedate_to_datetime(value)
                if retry_at.tzinfo is None:
                    # The obsolete HTTP asctime form has no explicit zone.
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                delay = (retry_at - datetime.now(timezone.utc)).total_seconds()
            except (ValueError, TypeError, OverflowError):
                pass
            else:
                return min(max(0.0, delay), self.max_retry_after)
        if status_code != 429:
            return self.backoff_for(attempt)
        ceiling = min(
            self.base_backoff
            * self.rate_limit_backoff_multiplier
            * (self.multiplier**attempt),
            self.max_backoff,
        )
        return random.uniform(0.0, ceiling) if self.jitter else ceiling

    def should_retry_status(self, status_code: int) -> bool:
        """Whether an HTTP ``status_code`` is in the retryable set."""
        return status_code in self.retry_statuses


#: Default policy used by clients that do not pass their own.
DEFAULT_RETRY_POLICY = RetryPolicy()
