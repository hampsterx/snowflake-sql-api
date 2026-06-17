"""Synchronous Snowflake SQL API client.

The primary entry point. Wraps auth + transport + type coercion + pagination
into query helpers (``query``/``query_one``/``query_scalar``/``query_column``),
DML (``execute``, ``insert_many``), and async-submit handles (``submit`` ->
:class:`QueryHandle`). Mirrors :class:`~snowflake_sql_api.aclient.AsyncSnowflakeClient`.
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from .auth import KeypairAuthenticator, account_hostname
from .bindings import to_bindings
from .escaping import quote_identifier, quote_name
from .exceptions import (
    ResultNotReady,
    SnowflakeConfigError,
    SnowflakeRequestError,
    SnowflakeTimeoutError,
)
from .pagination import fetch_all_partitions
from .retry import DEFAULT_RETRY_POLICY, RetryPolicy
from .transport import StatementResponse, Transport
from .types import ColumnMeta, Row, coerce_rows

__all__ = ["SnowflakeClient", "QueryHandle"]

#: Positional bind parameters for a statement.
Params = Optional[Sequence[Any]]

#: Callback fired for every executed statement (logging / instrumentation hook).
OnQuery = Callable[[str, "Params"], None]

#: Fallback wait, in seconds, when polling a long-running statement to completion.
DEFAULT_POLL_TIMEOUT = 600.0


def _rows_affected(body: Dict[str, Any]) -> int:
    """Extract the rows-affected count from a DML response."""
    stats = body.get("stats")
    if isinstance(stats, dict):
        total = 0
        found = False
        for key in ("numRowsInserted", "numRowsUpdated", "numRowsDeleted"):
            if key in stats:
                total += int(stats[key])
                found = True
        if found:
            return total
    data = body.get("data")
    if data and data[0]:
        try:
            return int(data[0][0])
        except (ValueError, TypeError):
            pass
    return 0


def _columns_from(body: Dict[str, Any]) -> List[ColumnMeta]:
    metadata = body.get("resultSetMetaData") or {}
    return [ColumnMeta.from_row_type(rt) for rt in metadata.get("rowType", [])]


class QueryHandle:
    """Handle to an asynchronously submitted statement.

    Returned by :meth:`SnowflakeClient.submit`. ``result(poll=False)`` raises
    :class:`~snowflake_sql_api.exceptions.ResultNotReady` when the statement is
    still running (HTTP 202) instead of returning a misleading 202 body
    (regression bug #3).
    """

    def __init__(self, client: SnowflakeClient, statement_handle: str) -> None:
        self._client = client
        self.statement_handle = statement_handle

    def result(
        self, *, poll: bool = True, timeout: Optional[float] = None
    ) -> List[Row]:
        """Return the rows, optionally polling until the statement finishes.

        With ``poll=False`` and the statement still running, raises
        :class:`~snowflake_sql_api.exceptions.ResultNotReady` rather than
        returning the in-progress 202 payload.
        """
        response = self._client._transport.get_statement(self.statement_handle)
        columns, raw_rows = self._client._collect(response, timeout=timeout, poll=poll)
        return coerce_rows(raw_rows, columns)

    def status(self) -> str:
        """Return ``"RUNNING"`` or ``"SUCCESS"`` without fetching results."""
        response = self._client._transport.get_statement(self.statement_handle)
        return "RUNNING" if response.is_running else "SUCCESS"

    def cancel(self) -> None:
        """Cancel the running statement."""
        self._client._transport.cancel(self.statement_handle)


class SnowflakeClient:
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
        self._transport = Transport(
            host or account_hostname(account),
            self._auth,
            timeout=timeout,
            retry_policy=retry_policy,
            user_agent=user_agent,
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

    def _run(
        self, sql: str, params: Params, *, async_exec: bool = False
    ) -> StatementResponse:
        self._notify(sql, params)
        request_id = str(uuid.uuid4())
        payload = self._build_payload(sql, params)
        return self._transport.submit(
            payload, request_id=request_id, async_exec=async_exec
        )

    def _poll(self, handle: str, timeout: Optional[float]) -> StatementResponse:
        wait = DEFAULT_POLL_TIMEOUT if timeout is None else timeout
        deadline = time.time() + wait
        while True:
            response = self._transport.get_statement(handle)
            if not response.is_running:
                return response
            if time.time() >= deadline:
                raise SnowflakeTimeoutError(
                    f"statement {handle} did not finish within {wait}s"
                )
            time.sleep(self._poll_interval)

    def _collect(
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
            response = self._poll(handle, timeout)
        body = response.body
        columns = _columns_from(body)
        handle = response.statement_handle
        if handle is None:
            raise SnowflakeRequestError("completed statement has no handle")
        raw_rows = fetch_all_partitions(self._transport, handle, body)
        return columns, raw_rows

    # -- query surface ----------------------------------------------------

    def query(self, sql: str, params: Params = None) -> List[Row]:
        """Run ``sql`` and return all rows (fetching every partition)."""
        response = self._run(sql, params)
        columns, raw_rows = self._collect(response, timeout=self._statement_timeout)
        return coerce_rows(raw_rows, columns)

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
            response = self._poll(handle, self._statement_timeout)
        return _rows_affected(response.body)

    def insert_many(
        self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]
    ) -> int:
        """Batch-insert ``rows`` into ``table`` via server-side bindings.

        Builds a single multi-row ``INSERT ... VALUES`` with positional
        bindings; identifiers are quoted, values are bound (never interpolated).
        """
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
        return self.execute(sql, flat)

    # -- async submit -----------------------------------------------------

    def submit(self, sql: str, params: Params = None) -> QueryHandle:
        """Submit a long-running statement and return a :class:`QueryHandle`."""
        response = self._run(sql, params, async_exec=True)
        handle = response.statement_handle
        if handle is None:
            raise SnowflakeRequestError("async submit returned no statement handle")
        return QueryHandle(self, handle)

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        """Close the underlying transport."""
        self._transport.close()

    def __enter__(self) -> SnowflakeClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
