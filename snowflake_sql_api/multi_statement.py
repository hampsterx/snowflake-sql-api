"""Request-local SQL batches, ordered results and recovery metadata."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from time import monotonic
from typing import TYPE_CHECKING, Any, Literal

from .exceptions import (
    SnowflakeConfigError,
    SnowflakeMultiStatementError,
    SnowflakeRequestError,
)
from .transport import StatementResponse
from .types import ColumnMeta, Row

if TYPE_CHECKING:
    from ._client_base import _ClientBase

MultiStatementPhase = Literal["submission", "execution", "result_fetch"]
MultiStatementOutcome = Literal["not_executed", "failed", "unknown", "succeeded"]


@dataclass(frozen=True)
class StatementResult:
    """One statement's zero-based position, handle, metadata and coerced rows."""

    index: int
    statement_handle: str
    columns: tuple[ColumnMeta, ...]
    rows: list[Row]


@dataclass(frozen=True)
class MultiStatementResult:
    """A parent handle and an immutable sequence of results in input order."""

    statement_handle: str
    results: tuple[StatementResult, ...]


def _statements(statements: Sequence[str]) -> tuple[str, ...]:
    if isinstance(statements, (str, bytes)) or not isinstance(statements, Sequence):
        raise SnowflakeConfigError(
            "statements must be a nonempty sequence of SQL strings"
        )
    if not statements or any(
        not isinstance(s, str) or not s.strip() for s in statements
    ):
        raise SnowflakeConfigError("statements must contain nonblank SQL strings")
    return tuple(s.strip() for s in statements)


def _transaction_statements(statements: Sequence[str]) -> tuple[str, ...]:
    body = _statements(statements)
    for sql in body:
        prefix = sql
        while True:
            prefix = prefix.lstrip(" \t\r\n\v\f")
            if prefix.startswith("--"):
                end = re.search(r"[\r\n]", prefix)
                prefix = prefix[end.end() :] if end else ""
            elif prefix.startswith("/*"):
                end_index = prefix.find("*/", 2)
                if end_index < 0 or "/*" in prefix[2:end_index]:
                    raise SnowflakeConfigError("unsupported leading block comment")
                prefix = prefix[end_index + 2 :]
            else:
                break
        keyword = re.match(r"[A-Za-z]+\b", prefix)
        if keyword is None or keyword[0].upper() not in {
            "INSERT",
            "UPDATE",
            "DELETE",
            "MERGE",
            "SELECT",
        }:
            raise SnowflakeConfigError(
                "transaction body supports only INSERT, UPDATE, DELETE, MERGE and SELECT"
            )
    return ("BEGIN TRANSACTION", *body, "COMMIT")


def _multi_payload(
    client: _ClientBase, statements: Sequence[str]
) -> tuple[dict[str, Any], int]:
    entries = _statements(statements)
    sql = "\n;\n".join(entries)
    if any(key.upper() == "AUTOCOMMIT" for key in client._parameters):
        raise SnowflakeConfigError(
            "AUTOCOMMIT is not a supported SQL API request parameter"
        )
    payload = client._build_payload(sql, None)
    parameters = {
        key: value
        for key, value in payload.get("parameters", {}).items()
        if key.upper() != "MULTI_STATEMENT_COUNT"
    }
    parameters["MULTI_STATEMENT_COUNT"] = str(len(entries))
    payload["parameters"] = parameters
    return payload, len(entries)


class _MultiStatementState:
    """Shared bookkeeping; concrete handles own every network operation."""

    def __init__(self, statement_handle: str, expected_count: int) -> None:
        if (
            isinstance(expected_count, bool)
            or not isinstance(expected_count, int)
            or expected_count <= 0
        ):
            raise SnowflakeConfigError("expected_count must be a positive integer")
        self.statement_handle = statement_handle
        self.expected_count = expected_count
        self.submission_attempts: int | None = None
        self._response: StatementResponse | None = None
        self._known_handles: tuple[str, ...] = ()
        self._succeeded = False

    def _discover(self, response: StatementResponse) -> tuple[str, ...]:
        handles = response.statement_handles
        if self.expected_count == 1 and "statementHandles" not in response.body:
            handles = (self.statement_handle,)
        if handles is None or len(handles) != self.expected_count:
            raise SnowflakeRequestError(
                "successful parent has missing or malformed statementHandles"
            )
        self._known_handles = handles
        self._succeeded = True
        return handles

    def _error(
        self,
        cause: Exception,
        phase: MultiStatementPhase,
        completed: Sequence[StatementResult] = (),
        failed_index: int | None = None,
    ) -> SnowflakeMultiStatementError:
        reported = getattr(cause, "statement_handles", None)
        if phase != "result_fetch" and reported is not None:
            self._known_handles = tuple(reported)
        attempts = getattr(cause, "submission_attempts", None)
        if attempts is not None:
            self.submission_attempts = attempts
        outcome: MultiStatementOutcome = "unknown"
        status = getattr(cause, "status_code", None)
        if self._succeeded:
            outcome = "succeeded"
        elif self.submission_attempts == 0:
            outcome = "not_executed"
        elif self.submission_attempts == 1:
            if phase == "submission" and status in (400, 401, 403):
                outcome = "not_executed"
            elif status == 422:
                outcome = "failed"
        if status == 422 and phase == "submission":
            phase = "execution"
        return SnowflakeMultiStatementError(
            str(cause),
            statement_handle=self.statement_handle or None,
            expected_count=self.expected_count,
            known_statement_handles=self._known_handles,
            completed_results=tuple(completed),
            failed_index=failed_index,
            phase=phase,
            outcome=outcome,
            submission_attempts=self.submission_attempts,
            status_code=status,
            code=getattr(cause, "code", None),
            sql_state=getattr(cause, "sql_state", None),
        )


def _deadline(timeout: float | None, default: float) -> float:
    return monotonic() + (default if timeout is None else timeout)


def _remaining(deadline: float) -> float:
    return max(0.0, deadline - monotonic())
