"""Bounded INSERT planning and immutable progress shared by both clients."""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, replace
from typing import Any, Callable, Literal

from .bindings import to_bindings
from .escaping import quote_identifier, quote_name
from .exceptions import (
    BatchWriteError,
    SnowflakeAuthError,
    SnowflakeConfigError,
    SnowflakeProgrammingError,
    SnowflakeRequestError,
)

__all__ = ["BatchSummary", "BatchWriteResult", "FailedBatch"]

BatchPhase = Literal["input", "validation", "submission", "execution"]
BatchOutcome = Literal["not_executed", "failed", "unknown", "succeeded"]


@dataclass(frozen=True)
class BatchSummary:
    """One confirmed INSERT, with zero-based batch and source offsets."""

    index: int
    start_row_index: int
    row_count: int
    rows_affected: int


@dataclass(frozen=True)
class BatchWriteResult:
    """Confirmed progress only; never retains input records or request bodies."""

    batches: tuple[BatchSummary, ...] = ()

    @property
    def input_rows(self) -> int:
        return sum(batch.row_count for batch in self.batches)

    @property
    def rows_affected(self) -> int:
        return sum(batch.rows_affected for batch in self.batches)

    @property
    def completed_batches(self) -> int:
        return len(self.batches)


@dataclass(frozen=True)
class FailedBatch:
    """Prepared INSERT identity; an unknown outcome requires reconciliation."""

    index: int
    start_row_index: int
    row_count: int
    request_id: str
    statement_handle: str | None = None
    submission_attempts: int = 0


def _encode(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


@dataclass
class _PreparedBatch:
    sql: str
    params: list[Any]
    content: bytes
    row_count: int


class _BatchPlanner:
    def __init__(
        self,
        table: str,
        columns: Sequence[str],
        rows: Iterable[Sequence[Any]],
        max_rows: int,
        max_bytes: int,
        build_payload: Callable[[str, Any], dict[str, Any]],
    ) -> None:
        if not columns:
            raise SnowflakeConfigError("insert_batches requires at least one column")
        for name, limit in (("max_rows", max_rows), ("max_bytes", max_bytes)):
            if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
                raise SnowflakeConfigError(f"{name} must be a positive integer")
        if max_rows > 200_000:
            raise SnowflakeConfigError("max_rows must be <= 200000")
        cols = ", ".join(quote_identifier(c) for c in columns)
        self.prefix = f"INSERT INTO {quote_name(table)} ({cols}) VALUES "
        self.placeholder = "(" + ", ".join(["?"] * len(columns)) + ")"
        self.width = len(columns)
        self.max_rows, self.max_bytes = max_rows, max_bytes
        self.source = rows
        self.iterator: Iterator[Sequence[Any]] | None = None
        self.build_payload = build_payload
        self.context: bytes | None = None
        self.single_overhead = 0
        self.pending: tuple[list[Any], list[bytes]] | None = None
        self.exhausted = False
        self.consumed_rows = 0
        self.confirmed_rows = 0
        self.completed: list[BatchSummary] = []

    def result(self) -> BatchWriteResult:
        return BatchWriteResult(tuple(self.completed))

    def error(
        self,
        cause: Exception,
        phase: BatchPhase,
        outcome: BatchOutcome = "not_executed",
        failed_batch: FailedBatch | None = None,
    ) -> BatchWriteError:
        return BatchWriteError(
            f"batch write stopped during {phase}: {cause}",
            completed=self.result(),
            next_row_index=self.confirmed_rows,
            consumed_rows=self.consumed_rows,
            failed_batch=failed_batch,
            phase=phase,
            outcome=outcome,
        )

    def _record(self) -> tuple[list[Any], list[bytes]] | None:
        if self.exhausted:
            return None
        try:
            if self.iterator is None:
                self.iterator = iter(self.source)
        except Exception as exc:
            raise self.error(exc, "input") from exc
        try:
            row = next(self.iterator)
        except StopIteration:
            self.exhausted = True
            return None
        except Exception as exc:
            raise self.error(exc, "input") from exc
        self.consumed_rows += 1
        try:
            if not isinstance(row, Sequence) or isinstance(
                row, (str, bytes, bytearray)
            ):
                raise SnowflakeConfigError("each row must be a positional sequence")
            if len(row) != self.width:
                raise SnowflakeConfigError(
                    f"each row must have {self.width} values, got {len(row)}"
                )
            values = list(row)
            return values, [_encode(value) for value in to_bindings(values).values()]
        except Exception as exc:
            raise self.error(exc, "validation") from exc

    @staticmethod
    def _fragment(values: list[bytes], offset: int) -> bytes:
        return b",".join(
            _encode(str(offset + i)) + b":" + value for i, value in enumerate(values, 1)
        )

    def _body(self, sql: str, bindings: bytes) -> bytes:
        assert self.context is not None
        return (
            b'{"statement":'
            + _encode(sql)
            + self.context
            + b',"bindings":{'
            + bindings
            + b"}}"
        )

    def next_batch(self) -> _PreparedBatch | None:
        # Configuration is validated before this point; input errors discard the
        # unsent buffer. Only a validated singleton can trigger a byte-boundary flush.
        params: list[Any] = []
        fragments: list[bytes] = []
        row_count = 0
        size = 0
        while row_count < self.max_rows:
            record = self.pending
            self.pending = None
            if record is None:
                record = self._record()
            if record is None:
                break
            values, encoded = record
            try:
                if self.context is None:
                    context = self.build_payload("", None)
                    del context["statement"]
                    self.context = _encode(context)[1:-1]
                    if self.context:
                        self.context = b"," + self.context
                    self.single_overhead = len(
                        self._body(self.prefix + self.placeholder, b"")
                    )
                first = self._fragment(encoded, 0)
                single_size = self.single_overhead + len(first)
                if single_size > self.max_bytes:
                    raise SnowflakeConfigError(
                        f"row {self.consumed_rows - 1} exceeds max_bytes={self.max_bytes}"
                    )
                fragment = (
                    first if row_count == 0 else self._fragment(encoded, len(params))
                )
                candidate = (
                    single_size
                    if row_count == 0
                    else size + 2 + len(self.placeholder) + 1 + len(fragment)
                )
                if candidate > self.max_bytes:
                    self.pending = record
                    break
                fragments.append(fragment)
                params.extend(values)
                row_count += 1
                size = candidate
            except Exception as exc:
                raise self.error(exc, "validation") from exc
        if not row_count:
            return None
        sql = self.prefix + ", ".join([self.placeholder] * row_count)
        content = self._body(sql, b",".join(fragments))
        if len(content) != size or len(content) > self.max_bytes:
            budget_error = SnowflakeConfigError(
                "batch body exceeds its measured byte budget"
            )
            raise self.error(budget_error, "validation") from budget_error
        return _PreparedBatch(sql, params, content, row_count)

    def confirm(self, batch: _PreparedBatch, rows_affected: int) -> None:
        self.completed.append(
            BatchSummary(
                len(self.completed), self.confirmed_rows, batch.row_count, rows_affected
            )
        )
        self.confirmed_rows += batch.row_count


class _BatchExecution:
    """Parent POST context survives polling and conservative error classification."""

    def __init__(self, planner: _BatchPlanner, batch: _PreparedBatch) -> None:
        self.planner = planner
        self.failed = FailedBatch(
            len(planner.completed),
            planner.confirmed_rows,
            batch.row_count,
            str(uuid.uuid4()),
        )
        self.phase: BatchPhase = "submission"
        self.succeeded = False

    def received(self, handle: str | None, attempts: int | None, running: bool) -> None:
        self.failed = replace(
            self.failed, statement_handle=handle, submission_attempts=attempts or 0
        )
        if not running or handle is not None:
            self.phase = "execution"
        self.succeeded = not running

    def error(self, cause: Exception) -> BatchWriteError:
        attempts = getattr(cause, "submission_attempts", None)
        handle = getattr(cause, "statement_handle", None) or getattr(
            cause.__cause__, "statement_handle", None
        )
        self.failed = replace(
            self.failed,
            submission_attempts=(
                self.failed.submission_attempts if attempts is None else attempts
            ),
            statement_handle=handle or self.failed.statement_handle,
        )
        outcome: BatchOutcome = "succeeded" if self.succeeded else "unknown"
        if not self.succeeded and self.failed.submission_attempts <= 1:
            if isinstance(cause, SnowflakeProgrammingError):
                self.phase, outcome = "execution", "failed"
            elif self.phase == "submission" and (
                attempts == 0
                or isinstance(cause, SnowflakeAuthError)
                and cause.status_code == 401
                or isinstance(cause, SnowflakeRequestError)
                and cause.status_code in (400, 401, 403)
            ):
                outcome = "not_executed"
        if isinstance(cause, SnowflakeProgrammingError):
            self.phase = "execution"
        return self.planner.error(cause, self.phase, outcome, self.failed)
