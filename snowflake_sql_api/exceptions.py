"""Typed exception hierarchy for ``snowflake-sql-api``.

All exceptions derive from :class:`SnowflakeError`, so a caller can catch the
whole family with a single ``except`` clause. The transport layer maps SQL API
HTTP responses onto these types; see :mod:`snowflake_sql_api.transport`.
"""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from .batches import BatchOutcome, BatchPhase, BatchWriteResult, FailedBatch

__all__ = [
    "SnowflakeError",
    "BatchWriteError",
    "SnowflakeConfigError",
    "SnowflakeAuthError",
    "SnowflakeRequestError",
    "SnowflakeForbiddenError",
    "SnowflakeProgrammingError",
    "SnowflakeTimeoutError",
    "SnowflakeRetryError",
    "ResultNotReady",
]


class SnowflakeError(Exception):
    """Base class for every error raised by this library.

    ``submission_attempts`` counts actual parent POST attempts, or is None for
    errors outside statement submission (including GET requests).
    """

    def __init__(
        self, *args: object, submission_attempts: Optional[int] = None
    ) -> None:
        super().__init__(*args)
        self.submission_attempts = submission_attempts


class BatchWriteError(SnowflakeError):
    """A batch import stopped; inspect confirmed progress and reconcile unknown writes."""

    def __init__(
        self,
        message: str,
        *,
        completed: BatchWriteResult,
        next_row_index: int,
        consumed_rows: int,
        failed_batch: Optional[FailedBatch],
        phase: BatchPhase,
        outcome: BatchOutcome,
    ) -> None:
        super().__init__(message)
        self.completed = completed
        self.next_row_index = next_row_index
        self.consumed_rows = consumed_rows
        self.failed_batch = failed_batch
        self.phase = phase
        self.outcome = outcome

    def __reduce__(self) -> tuple[Any, ...]:
        fields = {
            key: getattr(self, key)
            for key in (
                "completed",
                "next_row_index",
                "consumed_rows",
                "failed_batch",
                "phase",
                "outcome",
            )
        }
        return partial(type(self), **fields), self.args, self.__dict__


class SnowflakeConfigError(SnowflakeError):
    """Client constructed with invalid or missing configuration.

    Raised before any network call (e.g. no account, unreadable private key,
    a feature used without its optional extra installed).
    """


class SnowflakeAuthError(SnowflakeError):
    """Snowflake rejected authentication (HTTP 401).

    Carries the server envelope. Local JWT signing errors propagate with their
    original types instead of being converted to this exception.
    """

    def __init__(
        self,
        *args: object,
        status_code: Optional[int] = None,
        code: Optional[str] = None,
        sql_state: Optional[str] = None,
        statement_handle: Optional[str] = None,
        submission_attempts: Optional[int] = None,
    ) -> None:
        super().__init__(*args, submission_attempts=submission_attempts)
        self.status_code = status_code
        self.code = code
        self.sql_state = sql_state
        self.statement_handle = statement_handle


class SnowflakeRequestError(SnowflakeError):
    """A request to the SQL API failed at the HTTP/protocol level.

    Carries the Snowflake error envelope fields when present so callers can
    branch on ``code``/``sql_state`` without re-parsing the response body.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        code: Optional[str] = None,
        sql_state: Optional[str] = None,
        request_id: Optional[str] = None,
        statement_handle: Optional[str] = None,
        submission_attempts: Optional[int] = None,
    ) -> None:
        super().__init__(message, submission_attempts=submission_attempts)
        self.status_code = status_code
        self.code = code
        self.sql_state = sql_state
        self.request_id = request_id
        self.statement_handle = statement_handle


class SnowflakeForbiddenError(SnowflakeRequestError):
    """Snowflake refused an authenticated request (HTTP 403).

    The JWT was accepted but the request is not allowed, so retrying with the
    same identity will not help. A subclass of :class:`SnowflakeRequestError`,
    so existing handlers for that type still catch it.
    """


class SnowflakeProgrammingError(SnowflakeRequestError):
    """The submitted SQL failed to compile or execute (HTTP 422).

    The statement reached Snowflake but was rejected (syntax error, missing
    object, constraint violation, and similar). Distinct from transport faults,
    which are :class:`SnowflakeRequestError`.
    """


class SnowflakeTimeoutError(SnowflakeError):
    """An HTTP request or a statement exceeded its timeout."""


class SnowflakeRetryError(SnowflakeError):
    """Retries were exhausted without a successful response.

    The originating error is attached as ``__cause__``.
    """


class ResultNotReady(SnowflakeError):
    """An asynchronously submitted statement is still running.

    Raised by ``QueryHandle.result(poll=False)`` when Snowflake answers a
    results fetch with HTTP 202 (regression bug #3: do not return a misleading
    202 payload as if it were a result set).
    """

    def __init__(
        self,
        message: str = "Statement is still running",
        *,
        statement_handle: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.statement_handle = statement_handle
