"""Asynchronous Snowflake SQL API client.

Same surface as :class:`~snowflake_sql_api.client.SnowflakeClient`, with
``await``-based methods and an async context manager. Sync/async parity is a
design principle: a feature on one side should exist on the other.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from time import monotonic
from typing import (
    Any,
    AsyncGenerator,
    Dict,
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
from .client import (
    DEFAULT_POLL_TIMEOUT,
    _columns_from,
    _record_cancel_failure,
    _rows_affected,
    _submission_id,
)
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
from .pagination import fetch_all_partitions_async, iter_result_rows_async
from .retry import DEFAULT_RETRY_POLICY, RetryPolicy
from .transport import AsyncTransport, StatementResponse
from .types import ColumnMeta, Row, coerce_rows

__all__ = ["AsyncSnowflakeClient", "AsyncQueryHandle", "AsyncMultiStatementHandle"]

#: Seconds a best-effort cancel may take after the polling task is cancelled.
CANCEL_ON_TASK_CANCEL_TIMEOUT = 5.0


# Cancel requests abandoned at the deadline but not yet finished. The event
# loop holds only weak references to tasks, so these keep each one alive until
# its own cancellation completes.
_abandoned_cancels: set[asyncio.Task[Any]] = set()


def _settle_cancel(task: asyncio.Task[Any]) -> None:
    _abandoned_cancels.discard(task)
    if not task.cancelled():
        task.exception()  # retrieved: a failed best-effort cancel is not reported


async def _cancel_after_task_cancel(transport: AsyncTransport, handle: str) -> None:
    """Best-effort cancel of ``handle`` while a ``CancelledError`` propagates.

    The request runs in its own task, so ``asyncio.wait`` neither cancels it
    nor is held up by it. A further cancellation of the waiting task is
    absorbed until the request finishes, since returning early would let the
    caller close the HTTP client the request (or its retry) still needs. The
    whole wait is bounded by :data:`CANCEL_ON_TASK_CANCEL_TIMEOUT`: past it the
    request is cancelled and abandoned without waiting for its cleanup, which
    can itself stall. Every failure is swallowed, since the caller re-raises
    the original ``CancelledError``.
    """
    task = asyncio.ensure_future(transport.cancel(handle))
    task.add_done_callback(_settle_cancel)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + CANCEL_ON_TASK_CANCEL_TIMEOUT
    while not task.done():
        remaining = deadline - loop.time()
        if remaining <= 0:
            _abandoned_cancels.add(task)
            task.cancel()
            return
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait({task}, timeout=remaining)


class AsyncQueryHandle:
    """Async handle to a submitted statement (see :class:`~snowflake_sql_api.client.QueryHandle`)."""

    def __init__(
        self,
        client: AsyncSnowflakeClient,
        statement_handle: str,
        *,
        request_id: Optional[str] = None,
    ) -> None:
        self._client = client
        self.statement_handle = statement_handle
        self.request_id = request_id

    async def result(
        self, *, poll: bool = True, timeout: Optional[float] = None
    ) -> List[Row]:
        """Return the rows, optionally polling until the statement finishes.

        With ``poll=False`` and the statement still running, raises
        :class:`~snowflake_sql_api.exceptions.ResultNotReady` (regression bug #3).
        A polling timeout leaves the statement running; await :meth:`cancel`
        to stop it.
        """
        response = await self._client._transport.get_statement(self.statement_handle)
        columns, raw_rows = await self._client._collect(
            response, timeout=timeout, cancel_on_timeout=False, poll=poll
        )
        return coerce_rows(raw_rows, columns)

    async def status(self) -> str:
        """Return ``"RUNNING"`` or ``"SUCCESS"`` without fetching results."""
        response = await self._client._transport.get_statement(self.statement_handle)
        return "RUNNING" if response.is_running else "SUCCESS"

    async def cancel(self) -> None:
        """Cancel the running statement."""
        await self._client._transport.cancel(self.statement_handle)


class AsyncMultiStatementHandle(_MultiStatementState):
    """A caller-owned parent handle; attaching and retrieving never submit SQL."""

    def __init__(
        self,
        client: AsyncSnowflakeClient,
        statement_handle: str,
        *,
        expected_count: int,
    ) -> None:
        if not isinstance(statement_handle, str) or not statement_handle.strip():
            raise SnowflakeConfigError("a nonempty parent statement handle is required")
        super().__init__(statement_handle, expected_count)
        self._client = client

    async def result(
        self, *, poll: bool = True, timeout: Optional[float] = None
    ) -> MultiStatementResult:
        """Collect ordered child results using one combined polling budget."""
        return await self._result(poll=poll, timeout=timeout, cancel_on_timeout=False)

    async def _result(
        self, *, poll: bool, timeout: Optional[float], cancel_on_timeout: bool
    ) -> MultiStatementResult:
        deadline = _deadline(timeout, DEFAULT_POLL_TIMEOUT)
        completed: List[StatementResult] = []
        phase: MultiStatementPhase = "execution"
        failed_index = None
        try:
            response = self._response
            if response is None:
                response = await self._client._transport.get_statement(
                    self.statement_handle
                )
            if response.is_running:
                if not poll:
                    raise ResultNotReady(statement_handle=self.statement_handle)
                response = await self._client._poll(
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
                    else await self._client._transport.get_statement(handle)
                )
                columns, raw_rows = await self._client._collect(
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

    async def status(self) -> str:
        """Check only the parent request's execution status."""
        response = await self._client._transport.get_statement(self.statement_handle)
        return "RUNNING" if response.is_running else "SUCCESS"

    async def cancel(self) -> None:
        """Request parent cancellation; committed effects may remain."""
        await self._client._transport.cancel(self.statement_handle)


class AsyncSnowflakeClient(_ClientBase):
    """Asynchronous client for the Snowflake SQL API."""

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
        http_client: Optional[httpx.AsyncClient] = None,
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
        self._transport = AsyncTransport(
            self._host,
            self._auth,
            timeout=timeout,
            retry_policy=retry_policy,
            user_agent=user_agent,
            client=http_client,
        )

    @classmethod
    def from_env(cls) -> AsyncSnowflakeClient:
        """Construct a client from ``SNOWFLAKE_*`` environment variables."""
        return super().from_env()

    # -- internals --------------------------------------------------------

    async def _run(
        self, sql: str, params: Params, *, async_exec: bool = False
    ) -> StatementResponse:
        self._notify(sql, params)
        request_id = str(uuid.uuid4())
        payload = self._build_payload(sql, params)
        return await self._transport.submit(
            payload, request_id=request_id, async_exec=async_exec
        )

    async def _poll(
        self, handle: str, timeout: Optional[float], *, cancel_on_timeout: bool
    ) -> StatementResponse:
        """Poll ``handle``; see :meth:`SnowflakeClient._poll` for cancellation.

        Unlike the sync client's ``KeyboardInterrupt``, a task cancellation
        while a client-owned poll waits also cancels the statement: a bounded,
        best-effort request (see :func:`_cancel_after_task_cancel`), after which
        the ``CancelledError`` is re-raised unchanged.
        """
        wait = DEFAULT_POLL_TIMEOUT if timeout is None else timeout
        deadline = monotonic() + wait
        try:
            while True:
                response = await self._transport.get_statement(handle)
                if not response.is_running:
                    return response
                if monotonic() >= deadline:
                    break
                await asyncio.sleep(self._poll_interval)
            error = SnowflakeTimeoutError(
                f"statement {handle} did not finish within {wait}s"
            )
            if cancel_on_timeout:
                try:
                    await self._transport.cancel(handle)
                except Exception as exc:
                    _record_cancel_failure(error, handle, exc)
                else:
                    error.add_note(f"cancellation requested for {handle}")
        except asyncio.CancelledError:
            # Also reached when the task is cancelled during the timeout cancel
            # above: that request was cut off, so it is sent again here.
            if cancel_on_timeout:
                await _cancel_after_task_cancel(self._transport, handle)
            raise
        raise error

    async def _prepare_result(
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
            response = await self._poll(
                handle, timeout, cancel_on_timeout=cancel_on_timeout
            )
        body = response.body
        columns = _columns_from(body)
        handle = response.statement_handle
        if handle is None:
            raise SnowflakeRequestError("completed statement has no handle")
        return columns, handle, body

    async def _collect(
        self,
        response: StatementResponse,
        *,
        timeout: Optional[float],
        cancel_on_timeout: bool,
        poll: bool = True,
    ) -> Tuple[List[ColumnMeta], List[List[Any]]]:
        columns, handle, body = await self._prepare_result(
            response, timeout=timeout, cancel_on_timeout=cancel_on_timeout, poll=poll
        )
        return columns, await fetch_all_partitions_async(self._transport, handle, body)

    async def _stream_result(
        self, sql: str, params: Params
    ) -> Tuple[List[ColumnMeta], AsyncGenerator[Row, None]]:
        columns, handle, body = await self._prepare_result(
            await self._run(sql, params),
            timeout=self._statement_timeout,
            cancel_on_timeout=True,
        )
        return columns, iter_result_rows_async(self._transport, handle, body, columns)

    # -- query surface ----------------------------------------------------

    async def query(self, sql: str, params: Params = None) -> List[Row]:
        """Run ``sql`` and return all rows (fetching every partition)."""
        response = await self._run(sql, params)
        columns, raw_rows = await self._collect(
            response, timeout=self._statement_timeout, cancel_on_timeout=True
        )
        return coerce_rows(raw_rows, columns)

    async def query_stream(
        self, sql: str, params: Params = None
    ) -> AsyncGenerator[Row, None]:
        """Yield converted rows on demand; call directly with async for.

        Submission starts on first iteration. Keep the client open while
        consuming; await aclose() on a retained stream when stopping early.
        """
        _, rows = await self._stream_result(sql, params)
        try:
            async for row in rows:
                yield row
        finally:
            await rows.aclose()

    async def query_batches(
        self, sql: str, params: Params = None, *, batch_size: int = 1000
    ) -> AsyncGenerator[List[Row], None]:
        """Yield batches of at most batch_size rows across partition boundaries."""
        validate_batch_size(batch_size)
        rows = self.query_stream(sql, params)
        try:
            batch: List[Row] = []
            async for row in rows:
                batch.append(row)
                if len(batch) == batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch
        finally:
            await rows.aclose()

    async def export_csv(self, sql: str, output: TextIO, params: Params = None) -> int:
        """Await query I/O and write CSV to a caller-owned synchronous text sink.

        Return the data-row count. Failures may leave partial output.
        """
        columns, rows = await self._stream_result(sql, params)
        try:
            writer = CSVWriter(output, columns)
            count = 0
            async for row in rows:
                writer.write(row)
                count += 1
        finally:
            await rows.aclose()
        return count

    async def export_jsonl(
        self, sql: str, output: TextIO, params: Params = None
    ) -> int:
        """Await query I/O and write JSONL to a caller-owned synchronous text sink.

        Return the row count. Failures may leave partial output.
        """
        columns, rows = await self._stream_result(sql, params)
        try:
            validate_export_columns(columns)
            count = 0
            async for row in rows:
                write_jsonl(output, row)
                count += 1
        finally:
            await rows.aclose()
        return count

    async def query_one(self, sql: str, params: Params = None) -> Optional[Row]:
        """Return the first row, or ``None`` if the result is empty."""
        rows = await self.query(sql, params)
        return rows[0] if rows else None

    async def query_scalar(self, sql: str, params: Params = None) -> Any:
        """Return the first column of the first row (or ``None``)."""
        row = await self.query_one(sql, params)
        if not row:
            return None
        return next(iter(row.values()), None)

    async def query_column(self, sql: str, params: Params = None) -> List[Any]:
        """Return the first column across all rows."""
        rows = await self.query(sql, params)
        return [next(iter(row.values()), None) for row in rows]

    # -- DML --------------------------------------------------------------

    async def execute(self, sql: str, params: Params = None) -> int:
        """Run a DML/DDL statement and return the rows affected."""
        response = await self._run(sql, params)
        if response.is_running:
            handle = response.statement_handle
            if handle is None:
                raise SnowflakeRequestError("running statement has no handle")
            response = await self._poll(
                handle, self._statement_timeout, cancel_on_timeout=True
            )
        return _rows_affected(response.body)

    async def insert_many(
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
        return await self.execute(sql, flat)

    # -- multi-statement requests ------------------------------------------

    async def submit_statements(
        self, statements: Sequence[str]
    ) -> AsyncMultiStatementHandle:
        """Submit trusted SQL entries together, without bindings, and return their parent."""
        return await self._submit_statements(statements, async_exec=True)

    async def execute_statements(
        self, statements: Sequence[str]
    ) -> MultiStatementResult:
        """Execute one request and collect each result; ordinary batches are not atomic."""
        handle = await self._submit_statements(statements, async_exec=False)
        return await handle._result(
            poll=True, timeout=self._statement_timeout, cancel_on_timeout=True
        )

    async def execute_transaction(
        self, statements: Sequence[str]
    ) -> MultiStatementResult:
        """Wrap supported trusted DML/SELECT entries in one BEGIN/body/COMMIT request.

        Rollback after a server-side failure requires live verification for the
        target environment. A timeout does not establish whether COMMIT ran.
        """
        return await self.execute_statements(_transaction_statements(statements))

    async def _submit_statements(
        self, statements: Sequence[str], *, async_exec: bool
    ) -> AsyncMultiStatementHandle:
        payload, count = _multi_payload(self, statements)
        state = _MultiStatementState("", count)
        state.request_id = str(uuid.uuid4())
        self._notify(payload["statement"], None)
        try:
            response = await self._transport.submit(
                payload, request_id=state.request_id, async_exec=async_exec
            )
            state.submission_attempts = response.submission_attempts
            parent = response.statement_handle
            if not isinstance(parent, str) or not parent.strip():
                raise SnowflakeRequestError(
                    "multi-statement submit returned no parent handle"
                )
            handle = AsyncMultiStatementHandle(self, parent, expected_count=count)
            handle.request_id = state.request_id
            handle.submission_attempts = response.submission_attempts
            handle._response = response
            return handle
        except Exception as exc:
            state.statement_handle = getattr(exc, "statement_handle", None) or ""
            raise state._error(exc, "submission") from exc

    # -- async submit -----------------------------------------------------

    async def insert_batches(
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
                response = await self._transport.submit(
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
                    response = await self._poll(
                        handle, self._statement_timeout, cancel_on_timeout=True
                    )
                    execution.succeeded = True
                affected = _rows_affected(response.body, strict=True)
            except Exception as exc:
                raise execution.error(exc) from exc
            planner.confirm(batch, affected)
            del batch
        return planner.result()

    async def submit(
        self,
        sql: str,
        params: Params = None,
        *,
        request_id: str | uuid.UUID | None = None,
    ) -> AsyncQueryHandle:
        """Submit once with a caller-owned UUID, or generate a fresh UUID4.

        Persist the ID and identical inputs before calling for restart recovery.
        Use :meth:`resubmit` once a previous attempt might have reached Snowflake.
        """
        return await self._submit_handle(
            sql, params, request_id=request_id, retry=False
        )

    async def resubmit(
        self, sql: str, params: Params = None, *, request_id: str | uuid.UUID
    ) -> AsyncQueryHandle:
        """Replay identical saved inputs with the same UUID and ``retry=true``.

        The caller owns durable storage, payload consistency and reconciliation
        when Snowflake's retained history cannot resolve an uncertain outcome.
        """
        return await self._submit_handle(sql, params, request_id=request_id, retry=True)

    async def _submit_handle(
        self,
        sql: str,
        params: Params,
        *,
        request_id: str | uuid.UUID | None,
        retry: bool,
    ) -> AsyncQueryHandle:
        canonical_id = _submission_id(request_id, retry=retry)
        self._notify(sql, params)
        try:
            payload = self._build_payload(sql, params)
            response = await self._transport.submit(
                payload, request_id=canonical_id, async_exec=True, retry=retry
            )
            handle = response.statement_handle
            if handle is None:
                raise SnowflakeRequestError("async submit returned no statement handle")
            return AsyncQueryHandle(self, handle, request_id=canonical_id)
        except SnowflakeError as exc:
            exc.request_id = canonical_id
            raise

    # -- lifecycle --------------------------------------------------------

    async def aclose(self) -> None:
        """Close the underlying async transport."""
        await self._transport.aclose()

    async def __aenter__(self) -> AsyncSnowflakeClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
