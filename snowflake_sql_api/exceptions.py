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
    from .multi_statement import (
        MultiStatementOutcome,
        MultiStatementPhase,
        StatementResult,
    )

__all__ = [
    "SnowflakeError",
    "BatchWriteError",
    "SnowflakeMultiStatementError",
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

    request_id: Optional[str] = None

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
    """Invalid configuration or argument, or an unsupported result shape.

    Raised locally, for example for no account, an unreadable private key, an
    unbindable parameter value, or invalid write-helper columns, rows or limits.
    Most are raised before any request is sent. Duplicate column names are
    detected from completed result metadata, after the statement has run. In
    ``insert_batches``, a row error found during iteration arrives as the cause
    of a ``BatchWriteError``.
    """


class SnowflakeMultiStatementError(SnowflakeError):
    """A batch failed to submit, execute, or download. Inspect outcome before retrying."""

    def __init__(
        self,
        message: str,
        *,
        statement_handle: Optional[str],
        expected_count: int,
        known_statement_handles: tuple[str, ...] = (),
        completed_results: tuple[StatementResult, ...] = (),
        failed_index: Optional[int] = None,
        phase: MultiStatementPhase,
        outcome: MultiStatementOutcome,
        submission_attempts: Optional[int] = None,
        request_id: Optional[str] = None,
        status_code: Optional[int] = None,
        code: Optional[str] = None,
        sql_state: Optional[str] = None,
    ) -> None:
        super().__init__(message, submission_attempts=submission_attempts)
        self.statement_handle = statement_handle
        self.expected_count = expected_count
        self.known_statement_handles = known_statement_handles
        self.completed_results = completed_results
        self.failed_index = failed_index
        self.phase = phase
        self.outcome = outcome
        self.request_id = request_id
        self.status_code = status_code
        self.code = code
        self.sql_state = sql_state

    def __reduce__(self) -> tuple[Any, ...]:
        return partial(type(self), **self.__dict__), self.args


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
        statement_handles: Optional[tuple[str, ...]] = None,
    ) -> None:
        super().__init__(message, submission_attempts=submission_attempts)
        self.status_code = status_code
        self.code = code
        self.sql_state = sql_state
        self.request_id = request_id
        self.statement_handle = statement_handle
        self.statement_handles = statement_handles


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
    """An HTTP request or a statement exceeded its timeout.

    When the client gives up polling a statement it started itself, it asks
    Snowflake to cancel that statement before raising. ``cancel_error`` holds
    the exception that best-effort cancel raised, or is None when the cancel
    request was accepted or not attempted. It is not the cause of the timeout.
    """

    cancel_error: Optional[BaseException] = None


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
