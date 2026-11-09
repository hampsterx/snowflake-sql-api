"""Synchronous Snowflake SQL API client.

The primary entry point. Wraps auth + transport + type coercion + pagination
into query helpers (``query``/``query_one``/``query_scalar``/``query_column``),
DML (``execute``, ``insert_many``), and async-submit handles (``submit`` ->
:class:`QueryHandle`). Mirrors :class:`~snowflake_sql_api.aclient.AsyncSnowflakeClient`.
"""

from __future__ import annotations

import time
import uuid
from contextlib import closing
from time import monotonic
from typing import (
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Sequence,
    TextIO,
    Tuple,
)

import httpx

from ._client_base import OnQuery, Params, _ClientBase
from .batches import BatchWriteResult, _BatchExecution, _BatchPlanner
from .exceptions import (
    ResultNotReady,
    SnowflakeConfigError,
    SnowflakeError,
    SnowflakeRequestError,
    SnowflakeTimeoutError,
)
from .exports import (
    CSVWriter,
    validate_batch_size,
    validate_export_columns,
    write_jsonl,
)
from .multi_statement import (
    MultiStatementPhase,
    MultiStatementResult,
    StatementResult,
    _deadline,
    _multi_payload,
    _MultiStatementState,
    _remaining,
    _transaction_statements,
)
from .pagination import fetch_all_partitions, iter_result_rows
from .retry import DEFAULT_RETRY_POLICY, RetryPolicy
from .transport import StatementResponse, Transport
from .types import ColumnMeta, Row, _validate_column_names, coerce_rows

__all__ = ["SnowflakeClient", "QueryHandle", "MultiStatementHandle"]

#: Fallback wait, in seconds, when polling a long-running statement to completion.
DEFAULT_POLL_TIMEOUT = 600.0


def _submission_id(request_id: str | uuid.UUID | None, *, retry: bool) -> str:
    """Allocate or canonicalize an identity before hooks, binding or HTTP."""
    if request_id is None and not retry:
        return str(uuid.uuid4())
    if isinstance(request_id, uuid.UUID):
        return str(request_id)
    if not isinstance(request_id, str):
        raise SnowflakeConfigError("request_id must be a UUID string or UUID object")
    try:
        return str(uuid.UUID(request_id))
    except ValueError as exc:
        raise SnowflakeConfigError("request_id must be a valid UUID") from exc


def _affected_count(value: Any, strict: bool) -> int:
    if strict and (isinstance(value, bool) or not isinstance(value, (int, str))):
        raise ValueError("rows-affected count must be an integer")
    count = int(value)
    if strict and count < 0:
        raise ValueError("rows-affected count must be nonnegative")
    return count


def _rows_affected(body: Dict[str, Any], *, strict: bool = False) -> int:
    """Extract the rows-affected count from a DML response."""
    stats = body.get("stats")
    if isinstance(stats, dict):
        total = 0
        found = False
        for key in ("numRowsInserted", "numRowsUpdated", "numRowsDeleted"):
            if key in stats:
                total += _affected_count(stats[key], strict)
                found = True
        if found:
            return total
    data = body.get("data")
    if data and data[0]:
        try:
            return _affected_count(data[0][0], strict)
        except (ValueError, TypeError):
            if strict:
                raise
    if strict:
        raise ValueError("response has no rows-affected count")
    return 0


def _record_cancel_failure(
    error: SnowflakeTimeoutError, handle: str, exc: Exception
) -> None:
    """Attach a failed best-effort cancel to the timeout without replacing it."""
    error.cancel_error = exc
    error.add_note(f"cancelling {handle} after the timeout failed: {exc!r}")


def _columns_from(body: Dict[str, Any]) -> List[ColumnMeta]:
    metadata = body.get("resultSetMetaData") or {}
    columns = [ColumnMeta.from_row_type(rt) for rt in metadata.get("rowType", [])]
    _validate_column_names(columns)
    return columns


class QueryHandle:
    """Handle to an asynchronously submitted statement.

    Returned by :meth:`SnowflakeClient.submit`. ``result(poll=False)`` raises
    :class:`~snowflake_sql_api.exceptions.ResultNotReady` when the statement is
    still running (HTTP 202) instead of returning a misleading 202 body
    (regression bug #3).
    """

    def __init__(
        self,
        client: SnowflakeClient,
        statement_handle: str,
        *,
        request_id: Optional[str] = None,
    ) -> None:
        self._client = client
        self.statement_handle = statement_handle
        self.request_id = request_id

    def result(
        self, *, poll: bool = True, timeout: Optional[float] = None
    ) -> List[Row]:
        """Return the rows, optionally polling until the statement finishes.

        With ``poll=False`` and the statement still running, raises
        :class:`~snowflake_sql_api.exceptions.ResultNotReady` rather than
        returning the in-progress 202 payload. A polling timeout raises
        :class:`~snowflake_sql_api.exceptions.SnowflakeTimeoutError` and leaves
        the statement running; call :meth:`cancel` to stop it.
        """
        response = self._client._transport.get_statement(self.statement_handle)
        columns, raw_rows = self._client._collect(
            response, timeout=timeout, cancel_on_timeout=False, poll=poll
        )
        return coerce_rows(raw_rows, columns)

    def status(self) -> str:
        """Return ``"RUNNING"`` or ``"SUCCESS"`` without fetching results."""
        response = self._client._transport.get_statement(self.statement_handle)
        return "RUNNING" if response.is_running else "SUCCESS"

    def cancel(self) -> None:
        """Cancel the running statement."""
        self._client._transport.cancel(self.statement_handle)


class MultiStatementHandle(_MultiStatementState):
    """A caller-owned parent handle; attaching and retrieving never submit SQL."""

    def __init__(
        self, client: SnowflakeClient, statement_handle: str, *, expected_count: int
    ) -> None:
        if not isinstance(statement_handle, str) or not statement_handle.strip():
            raise SnowflakeConfigError("a nonempty parent statement handle is required")
        super().__init__(statement_handle, expected_count)
        self._client = client

    def result(
        self, *, poll: bool = True, timeout: Optional[float] = None
    ) -> MultiStatementResult:
        """Collect ordered child results using one combined polling budget."""
        return self._result(poll=poll, timeout=timeout, cancel_on_timeout=False)

    def _result(
        self, *, poll: bool, timeout: Optional[float], cancel_on_timeout: bool
    ) -> MultiStatementResult:
        deadline = _deadline(timeout, DEFAULT_POLL_TIMEOUT)
        completed: List[StatementResult] = []
        phase: MultiStatementPhase = "execution"
        failed_index = None
        try:
            response = self._response
            if response is None:
                response = self._client._transport.get_statement(self.statement_handle)
            if response.is_running:
                if not poll:
                    raise ResultNotReady(statement_handle=self.statement_handle)
                response = self._client._poll(
                    self.statement_handle,
                    _remaining(deadline),
                    cancel_on_timeout=cancel_on_timeout,
                )
            handles = self._discover(response)
            self._response = response
            phase = "result_fetch"
            for index, handle in enumerate(handles):
                failed_index = index
                child = (
                    response
                    if self.expected_count == 1 and handle == self.statement_handle
                    else self._client._transport.get_statement(handle)
                )
                columns, raw_rows = self._client._collect(
                    child,
                    timeout=_remaining(deadline),
                    cancel_on_timeout=False,
                    poll=poll,
                )
                completed.append(
                    StatementResult(
                        index, handle, tuple(columns), coerce_rows(raw_rows, columns)
                    )
                )
            return MultiStatementResult(self.statement_handle, tuple(completed))
        except ResultNotReady:
            raise
        except Exception as exc:
            raise self._error(exc, phase, completed, failed_index) from exc

    def status(self) -> str:
        """Check only the parent request's execution status."""
        response = self._client._transport.get_statement(self.statement_handle)
        return "RUNNING" if response.is_running else "SUCCESS"

    def cancel(self) -> None:
        """Request parent cancellation; committed effects may remain."""
        self._client._transport.cancel(self.statement_handle)


class SnowflakeClient(_ClientBase):
    """Synchronous client for the Snowflake SQL API.

    Construct with keypair credentials directly, or via :meth:`from_env`.
    No account/region/role/warehouse defaults are baked in: all session context
    is caller-supplied (vendor-neutral by design).
    """

    def __init__(
        self,
        account: str,
        user: str,
        *,
        private_key: Optional[bytes] = None,
        private_key_path: Optional[str] = None,
        private_key_passphrase: Optional[str] = None,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        timezone: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        host: Optional[str] = None,
        timeout: float = 60.0,
        statement_timeout: Optional[int] = None,
        poll_interval: float = 1.0,
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
        user_agent: Optional[str] = None,
        on_query: Optional[OnQuery] = None,
        http_client: Optional[httpx.Client] = None,
    ) -> None:
        super().__init__(
            account,
            user,
            private_key=private_key,
            private_key_path=private_key_path,
            private_key_passphrase=private_key_passphrase,
            role=role,
            warehouse=warehouse,
            database=database,
            schema=schema,
            timezone=timezone,
            parameters=parameters,
            host=host,
            statement_timeout=statement_timeout,
            poll_interval=poll_interval,
            on_query=on_query,
        )
        self._transport = Transport(
            self._host,
            self._auth,
            timeout=timeout,
            retry_policy=retry_policy,
            user_agent=user_agent,
            client=http_client,
        )

    @classmethod
    def from_env(cls) -> SnowflakeClient:
        """Construct a client from ``SNOWFLAKE_*`` environment variables.

        Reads ``SNOWFLAKE_ACCOUNT``, ``SNOWFLAKE_USER`` (required) and the
        optional ``SNOWFLAKE_PRIVATE_KEY`` / ``SNOWFLAKE_PRIVATE_KEY_PATH`` /
        ``SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`` / ``SNOWFLAKE_ROLE`` /
        ``SNOWFLAKE_WAREHOUSE`` / ``SNOWFLAKE_DATABASE`` / ``SNOWFLAKE_SCHEMA`` /
        ``SNOWFLAKE_HOST``.
        """
        return super().from_env()

    # -- internals --------------------------------------------------------

    def _run(
        self, sql: str, params: Params, *, async_exec: bool = False
    ) -> StatementResponse:
        self._notify(sql, params)
        request_id = str(uuid.uuid4())
        payload = self._build_payload(sql, params)
        return self._transport.submit(
            payload, request_id=request_id, async_exec=async_exec
        )

    def _poll(
        self, handle: str, timeout: Optional[float], *, cancel_on_timeout: bool
    ) -> StatementResponse:
        """Poll ``handle`` until it finishes or the wait runs out.

        ``cancel_on_timeout`` is True when the client started the statement on
        its own behalf: nobody else holds the handle, so a timeout would leave
        it running server-side with no owner. A caller-held handle is not
        cancelled, since its owner may poll it again.

        A ``KeyboardInterrupt`` (or any other non-``Exception`` raised while
        polling) propagates without a cancel: an HTTP call made while the
        process is being interrupted can hang, so the statement keeps running
        until its server-side ``timeout``.
        """
        wait = DEFAULT_POLL_TIMEOUT if timeout is None else timeout
        deadline = monotonic() + wait
        while True:
            response = self._transport.get_statement(handle)
            if not response.is_running:
                return response
            if monotonic() >= deadline:
                error = SnowflakeTimeoutError(
                    f"statement {handle} did not finish within {wait}s"
                )
                if cancel_on_timeout:
                    try:
                        self._transport.cancel(handle)
                    except Exception as exc:
                        _record_cancel_failure(error, handle, exc)
                    else:
                        error.add_note(f"cancellation requested for {handle}")
                raise error
            time.sleep(self._poll_interval)

    def _prepare_result(
        self,
        response: StatementResponse,
        *,
        timeout: Optional[float],
        cancel_on_timeout: bool,
        poll: bool = True,
    ) -> Tuple[List[ColumnMeta], str, Dict[str, Any]]:
        if response.is_running:
            if not poll:
                raise ResultNotReady(statement_handle=response.statement_handle)
            handle = response.statement_handle
            if handle is None:
                raise SnowflakeRequestError("running statement has no handle")
            response = self._poll(handle, timeout, cancel_on_timeout=cancel_on_timeout)
        body = response.body
        columns = _columns_from(body)
        handle = response.statement_handle
        if handle is None:
            raise SnowflakeRequestError("completed statement has no handle")
        return columns, handle, body

    def _collect(
        self,
        response: StatementResponse,
        *,
        timeout: Optional[float],
        cancel_on_timeout: bool,
        poll: bool = True,
    ) -> Tuple[List[ColumnMeta], List[List[Any]]]:
        columns, handle, body = self._prepare_result(
            response, timeout=timeout, cancel_on_timeout=cancel_on_timeout, poll=poll
        )
        return columns, fetch_all_partitions(self._transport, handle, body)

    def _stream_result(
        self, sql: str, params: Params
    ) -> Tuple[List[ColumnMeta], Generator[Row, None, None]]:
        columns, handle, body = self._prepare_result(
            self._run(sql, params),
            timeout=self._statement_timeout,
            cancel_on_timeout=True,
        )
        return columns, iter_result_rows(self._transport, handle, body, columns)

    # -- query surface ----------------------------------------------------

    def query(self, sql: str, params: Params = None) -> List[Row]:
        """Run ``sql`` and return all rows (fetching every partition)."""
        response = self._run(sql, params)
        columns, raw_rows = self._collect(
            response, timeout=self._statement_timeout, cancel_on_timeout=True
        )
        return coerce_rows(raw_rows, columns)

    def query_stream(
        self, sql: str, params: Params = None
    ) -> Generator[Row, None, None]:
        """Yield converted rows, submitting on first iteration without prefetch.

        Memory scales with the largest decoded server partition. Keep the client
        open while iterating; close a retained stream when stopping early.
        """
        _, rows = self._stream_result(sql, params)
        yield from rows

    def query_batches(
        self, sql: str, params: Params = None, *, batch_size: int = 1000
    ) -> Generator[List[Row], None, None]:
        """Yield batches of at most batch_size rows across partition boundaries."""
        validate_batch_size(batch_size)
        with closing(self.query_stream(sql, params)) as rows:
            batch: List[Row] = []
            for row in rows:
                batch.append(row)
                if len(batch) == batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch

    def export_csv(self, sql: str, output: TextIO, params: Params = None) -> int:
        """Stream CSV with a header to an open text sink; return the data-row count.

        The caller owns the sink. Failures propagate and may leave partial output.
        """
        columns, rows = self._stream_result(sql, params)
        with closing(rows):
            writer = CSVWriter(output, columns)
            count = 0
            for row in rows:
                writer.write(row)
                count += 1
        return count

    def export_jsonl(self, sql: str, output: TextIO, params: Params = None) -> int:
        """Stream JSON objects to an open text sink; return the row count.

        The caller owns the sink. Failures propagate and may leave partial output.
        """
        columns, rows = self._stream_result(sql, params)
        with closing(rows):
            validate_export_columns(columns)
            count = 0
            for row in rows:
                write_jsonl(output, row)
                count += 1
        return count

    def query_one(self, sql: str, params: Params = None) -> Optional[Row]:
        """Return the first row, or ``None`` if the result is empty."""
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def query_scalar(self, sql: str, params: Params = None) -> Any:
        """Return the first column of the first row (or ``None``)."""
        row = self.query_one(sql, params)
        if not row:
            return None
        return next(iter(row.values()), None)

    def query_column(self, sql: str, params: Params = None) -> List[Any]:
        """Return the first column across all rows."""
        rows = self.query(sql, params)
        return [next(iter(row.values()), None) for row in rows]

    # -- DML --------------------------------------------------------------

    def execute(self, sql: str, params: Params = None) -> int:
        """Run a DML/DDL statement and return the rows affected."""
        response = self._run(sql, params)
        if response.is_running:
            handle = response.statement_handle
            if handle is None:
                raise SnowflakeRequestError("running statement has no handle")
            response = self._poll(
                handle, self._statement_timeout, cancel_on_timeout=True
            )
        return _rows_affected(response.body)

    def insert_many(
        self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]
    ) -> int:
        """Batch-insert ``rows`` into ``table`` via server-side bindings.

        Builds a single multi-row ``INSERT ... VALUES`` with positional
        bindings; identifiers are quoted, values are bound (never interpolated).

        Table and column names are quoted exactly as given, so they are
        case-sensitive: a table created as ``CREATE TABLE events (id INT)`` is
        stored as ``EVENTS`` / ``ID`` and must be addressed as ``"EVENTS"`` and
        ``["ID"]``. Lowercase names only match objects created with quoted
        lowercase identifiers. This assumes Snowflake's default
        ``QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE``; with it ``TRUE``, quoted
        names resolve as uppercase.
        """
        statement = self._insert_statement(table, columns, rows)
        if statement is None:
            return 0
        sql, flat = statement
        return self.execute(sql, flat)

    def insert_batches(
        self,
        table: str,
        columns: Sequence[str],
        rows: Iterable[Sequence[Any]],
        *,
        max_rows: int = 1000,
        max_bytes: int = 1_000_000,
    ) -> BatchWriteResult:
        """Insert a single-pass source sequentially within row and wire-byte budgets.

        Independent statements can commit before a later failure. BatchWriteError
        reports confirmed progress and uncertain outcomes; see docs/batch-writes.md.
        Input is an ordinary iterable, consumed on the calling thread.

        Table and column names are quoted exactly as given, so they are
        case-sensitive: a table created as ``CREATE TABLE events (id INT)`` is
        stored as ``EVENTS`` / ``ID`` and must be addressed as ``"EVENTS"`` and
        ``["ID"]``. Lowercase names only match objects created with quoted
        lowercase identifiers. This assumes Snowflake's default
        ``QUOTED_IDENTIFIERS_IGNORE_CASE = FALSE``; with it ``TRUE``, quoted
        names resolve as uppercase.
        """
        planner = _BatchPlanner(
            table, columns, rows, max_rows, max_bytes, self._build_payload
        )
        while (batch := planner.next_batch()) is not None:
            try:
                self._notify(batch.sql, batch.params)
            except Exception as exc:
                raise planner.error(exc, "validation") from exc
            execution = _BatchExecution(planner, batch)
            try:
                response = self._transport.submit(
                    {}, request_id=execution.failed.request_id, content=batch.content
                )
                execution.received(
                    response.statement_handle,
                    response.submission_attempts,
                    response.is_running,
                )
                if response.is_running:
                    handle = response.statement_handle
                    if handle is None:
                        raise SnowflakeRequestError("running statement has no handle")
                    response = self._poll(
                        handle, self._statement_timeout, cancel_on_timeout=True
                    )
                    execution.succeeded = True
                affected = _rows_affected(response.body, strict=True)
            except Exception as exc:
                raise execution.error(exc) from exc
            planner.confirm(batch, affected)
            del batch
        return planner.result()

    # -- multi-statement requests ------------------------------------------

    def submit_statements(self, statements: Sequence[str]) -> MultiStatementHandle:
        """Submit trusted SQL entries together, without bindings, and return their parent."""
        return self._submit_statements(statements, async_exec=True)

    def execute_statements(self, statements: Sequence[str]) -> MultiStatementResult:
        """Execute one request and collect each result; ordinary batches are not atomic."""
        handle = self._submit_statements(statements, async_exec=False)
        return handle._result(
            poll=True, timeout=self._statement_timeout, cancel_on_timeout=True
        )

    def execute_transaction(self, statements: Sequence[str]) -> MultiStatementResult:
        """Wrap supported trusted DML/SELECT entries in one BEGIN/body/COMMIT request.

        Rollback after a server-side failure requires live verification for the
        target environment. A timeout does not establish whether COMMIT ran.
        """
        return self.execute_statements(_transaction_statements(statements))

    def _submit_statements(
        self, statements: Sequence[str], *, async_exec: bool
    ) -> MultiStatementHandle:
        payload, count = _multi_payload(self, statements)
        state = _MultiStatementState("", count)
        state.request_id = str(uuid.uuid4())
        self._notify(payload["statement"], None)
        try:
            response = self._transport.submit(
                payload, request_id=state.request_id, async_exec=async_exec
            )
            state.submission_attempts = response.submission_attempts
            parent = response.statement_handle
            if not isinstance(parent, str) or not parent.strip():
                raise SnowflakeRequestError(
                    "multi-statement submit returned no parent handle"
                )
            handle = MultiStatementHandle(self, parent, expected_count=count)
            handle.request_id = state.request_id
            handle.submission_attempts = response.submission_attempts
            handle._response = response
            return handle
        except Exception as exc:
            state.statement_handle = getattr(exc, "statement_handle", None) or ""
            raise state._error(exc, "submission") from exc

    # -- async submit -----------------------------------------------------

    def submit(
        self,
        sql: str,
        params: Params = None,
        *,
        request_id: str | uuid.UUID | None = None,
    ) -> QueryHandle:
        """Submit once with a caller-owned UUID, or generate a fresh UUID4.

        Persist the ID and identical inputs before calling for restart recovery.
        Use :meth:`resubmit` once a previous attempt might have reached Snowflake.
        """
        return self._submit_handle(sql, params, request_id=request_id, retry=False)

    def resubmit(
        self, sql: str, params: Params = None, *, request_id: str | uuid.UUID
    ) -> QueryHandle:
        """Replay identical saved inputs with the same UUID and ``retry=true``.

        The caller owns durable storage, payload consistency and reconciliation
        when Snowflake's retained history cannot resolve an uncertain outcome.
        """
        return self._submit_handle(sql, params, request_id=request_id, retry=True)

    def _submit_handle(
        self,
        sql: str,
        params: Params,
        *,
        request_id: str | uuid.UUID | None,
        retry: bool,
    ) -> QueryHandle:
        canonical_id = _submission_id(request_id, retry=retry)
        self._notify(sql, params)
        try:
            payload = self._build_payload(sql, params)
            response = self._transport.submit(
                payload, request_id=canonical_id, async_exec=True, retry=retry
            )
            handle = response.statement_handle
            if handle is None:
                raise SnowflakeRequestError("async submit returned no statement handle")
            return QueryHandle(self, handle, request_id=canonical_id)
        except SnowflakeError as exc:
            exc.request_id = canonical_id
            raise

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        """Close the underlying transport."""
        self._transport.close()

    def __enter__(self) -> SnowflakeClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
