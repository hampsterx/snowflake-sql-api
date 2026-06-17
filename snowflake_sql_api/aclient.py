"""Asynchronous Snowflake SQL API client.

Same surface as :class:`~snowflake_sql_api.client.SnowflakeClient`, with
``await``-based methods and an async context manager. Sync/async parity is a
design principle: a feature on one side should exist on the other.
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .auth import KeypairAuthenticator, account_hostname
from .bindings import to_bindings
from .client import DEFAULT_POLL_TIMEOUT, _columns_from, _rows_affected
from .escaping import quote_identifier, quote_name
from .exceptions import (
    ResultNotReady,
    SnowflakeConfigError,
    SnowflakeRequestError,
    SnowflakeTimeoutError,
)
from .pagination import fetch_all_partitions_async
from .retry import DEFAULT_RETRY_POLICY, RetryPolicy
from .transport import AsyncTransport, StatementResponse
from .types import ColumnMeta, Row, coerce_rows

__all__ = ["AsyncSnowflakeClient", "AsyncQueryHandle"]

#: Positional bind parameters for a statement.
Params = Optional[Sequence[Any]]

#: Callback fired for every executed statement (logging / instrumentation hook).
OnQuery = Callable[[str, "Params"], None]


class AsyncQueryHandle:
    """Async handle to a submitted statement (see :class:`~snowflake_sql_api.client.QueryHandle`)."""

    def __init__(self, client: AsyncSnowflakeClient, statement_handle: str) -> None:
        self._client = client
        self.statement_handle = statement_handle

    async def result(
        self, *, poll: bool = True, timeout: Optional[float] = None
    ) -> List[Row]:
        """Return the rows, optionally polling until the statement finishes.

        With ``poll=False`` and the statement still running, raises
        :class:`~snowflake_sql_api.exceptions.ResultNotReady` (regression bug #3).
        """
        response = await self._client._transport.get_statement(self.statement_handle)
        columns, raw_rows = await self._client._collect(
            response, timeout=timeout, poll=poll
        )
        return coerce_rows(raw_rows, columns)

    async def status(self) -> str:
        """Return ``"RUNNING"`` or ``"SUCCESS"`` without fetching results."""
        response = await self._client._transport.get_statement(self.statement_handle)
        return "RUNNING" if response.is_running else "SUCCESS"

    async def cancel(self) -> None:
        """Cancel the running statement."""
        await self._client._transport.cancel(self.statement_handle)


class AsyncSnowflakeClient:
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
    ) -> None:
        if private_key is None and private_key_path is None:
            raise SnowflakeConfigError("a private_key or private_key_path is required")
        key_bytes = (
            private_key
            if private_key is not None
            else Path(private_key_path).read_bytes()  # type: ignore[arg-type]
        )
        self.account = account
        self.user = user
        self.role = role
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.timezone = timezone
        self._parameters = dict(parameters) if parameters else {}
        self._statement_timeout = statement_timeout
        self._poll_interval = poll_interval
        self.on_query = on_query

        self._auth = KeypairAuthenticator(
            account, user, key_bytes, private_key_passphrase=private_key_passphrase
        )
        self._transport = AsyncTransport(
            host or account_hostname(account),
            self._auth,
            timeout=timeout,
            retry_policy=retry_policy,
            user_agent=user_agent,
        )

    @classmethod
    def from_env(cls) -> AsyncSnowflakeClient:
        """Construct a client from ``SNOWFLAKE_*`` environment variables."""
        account = os.environ.get("SNOWFLAKE_ACCOUNT")
        user = os.environ.get("SNOWFLAKE_USER")
        if not account or not user:
            raise SnowflakeConfigError(
                "SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER are required"
            )
        key_pem = os.environ.get("SNOWFLAKE_PRIVATE_KEY")
        return cls(
            account=account,
            user=user,
            private_key=key_pem.encode() if key_pem else None,
            private_key_path=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
            private_key_passphrase=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
            role=os.environ.get("SNOWFLAKE_ROLE"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            database=os.environ.get("SNOWFLAKE_DATABASE"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA"),
            host=os.environ.get("SNOWFLAKE_HOST"),
        )

    # -- internals --------------------------------------------------------

    def _build_payload(self, sql: str, params: Params) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"statement": sql}
        if self._statement_timeout is not None:
            payload["timeout"] = self._statement_timeout
        if self.database:
            payload["database"] = self.database
        if self.schema:
            payload["schema"] = self.schema
        if self.warehouse:
            payload["warehouse"] = self.warehouse
        if self.role:
            payload["role"] = self.role
        parameters = dict(self._parameters)
        if self.timezone:
            parameters["TIMEZONE"] = self.timezone
        if parameters:
            payload["parameters"] = parameters
        if params:
            payload["bindings"] = to_bindings(params)
        return payload

    def _notify(self, sql: str, params: Params) -> None:
        if self.on_query is not None:
            self.on_query(sql, params)

    async def _run(
        self, sql: str, params: Params, *, async_exec: bool = False
    ) -> StatementResponse:
        self._notify(sql, params)
        request_id = str(uuid.uuid4())
        payload = self._build_payload(sql, params)
        return await self._transport.submit(
            payload, request_id=request_id, async_exec=async_exec
        )

    async def _poll(self, handle: str, timeout: Optional[float]) -> StatementResponse:
        wait = DEFAULT_POLL_TIMEOUT if timeout is None else timeout
        deadline = time.time() + wait
        while True:
            response = await self._transport.get_statement(handle)
            if not response.is_running:
                return response
            if time.time() >= deadline:
                raise SnowflakeTimeoutError(
                    f"statement {handle} did not finish within {wait}s"
                )
            await asyncio.sleep(self._poll_interval)

    async def _collect(
        self,
        response: StatementResponse,
        *,
        timeout: Optional[float],
        poll: bool = True,
    ) -> Tuple[List[ColumnMeta], List[List[Any]]]:
        if response.is_running:
            if not poll:
                raise ResultNotReady(statement_handle=response.statement_handle)
            handle = response.statement_handle
            if handle is None:
                raise SnowflakeRequestError("running statement has no handle")
            response = await self._poll(handle, timeout)
        body = response.body
        columns = _columns_from(body)
        handle = response.statement_handle
        if handle is None:
            raise SnowflakeRequestError("completed statement has no handle")
        raw_rows = await fetch_all_partitions_async(self._transport, handle, body)
        return columns, raw_rows

    # -- query surface ----------------------------------------------------

    async def query(self, sql: str, params: Params = None) -> List[Row]:
        """Run ``sql`` and return all rows (fetching every partition)."""
        response = await self._run(sql, params)
        columns, raw_rows = await self._collect(
            response, timeout=self._statement_timeout
        )
        return coerce_rows(raw_rows, columns)

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
            response = await self._poll(handle, self._statement_timeout)
        return _rows_affected(response.body)

    async def insert_many(
        self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]
    ) -> int:
        """Batch-insert ``rows`` into ``table`` via server-side bindings."""
        if not columns:
            raise SnowflakeConfigError("insert_many requires at least one column")
        if not rows:
            return 0
        width = len(columns)
        flat: List[Any] = []
        for row in rows:
            if len(row) != width:
                raise SnowflakeConfigError(
                    f"each row must have {width} values, got {len(row)}"
                )
            flat.extend(row)
        cols_sql = ", ".join(quote_identifier(c) for c in columns)
        placeholder = "(" + ", ".join(["?"] * width) + ")"
        values_sql = ", ".join([placeholder] * len(rows))
        sql = f"INSERT INTO {quote_name(table)} ({cols_sql}) VALUES {values_sql}"
        return await self.execute(sql, flat)

    # -- async submit -----------------------------------------------------

    async def submit(self, sql: str, params: Params = None) -> AsyncQueryHandle:
        """Submit a long-running statement and return an :class:`AsyncQueryHandle`."""
        response = await self._run(sql, params, async_exec=True)
        handle = response.statement_handle
        if handle is None:
            raise SnowflakeRequestError("async submit returned no statement handle")
        return AsyncQueryHandle(self, handle)

    # -- lifecycle --------------------------------------------------------

    async def aclose(self) -> None:
        """Close the underlying async transport."""
        await self._transport.aclose()

    async def __aenter__(self) -> AsyncSnowflakeClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
