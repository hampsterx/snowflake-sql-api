"""Synchronous Snowflake SQL API client.

The primary entry point. Wraps auth + transport + type coercion + pagination
into query helpers (``query``/``query_one``/``query_scalar``/``query_column``),
DML (``execute``, ``insert_many``), and async-submit handles (``submit`` ->
:class:`QueryHandle`). Mirrors :class:`~snowflake_sql_api.aclient.AsyncSnowflakeClient`.

Scaffold only: the query surface lands in Phase 2.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Sequence

from .retry import DEFAULT_RETRY_POLICY, RetryPolicy
from .types import Row

__all__ = ["SnowflakeClient", "QueryHandle"]

#: Positional bind parameters for a statement.
Params = Optional[Sequence[Any]]

#: Callback fired for every executed statement (logging / instrumentation hook).
OnQuery = Callable[[str, "Params"], None]


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
    ) -> list[Row]:
        """Return the rows, optionally polling until the statement finishes. (Phase 2.)"""
        raise NotImplementedError

    def status(self) -> str:
        """Return the current execution status without fetching results. (Phase 2.)"""
        raise NotImplementedError

    def cancel(self) -> None:
        """Cancel the running statement. (Phase 2.)"""
        raise NotImplementedError


class SnowflakeClient:
    """Synchronous client for the Snowflake SQL API.

    Construct with keypair credentials directly, or via :meth:`from_env`.
    No account/region/role/warehouse defaults are baked in: all session context
    is caller-supplied (vendor-neutral by design).

    Scaffold only: every method below is implemented in Phase 2.
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
        host: Optional[str] = None,
        timeout: float = 60.0,
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
        on_query: Optional[OnQuery] = None,
    ) -> None:
        self.account = account
        self.user = user
        self.role = role
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.timezone = timezone
        self.host = host
        self.timeout = timeout
        self.retry_policy = retry_policy
        self.on_query = on_query
        self._private_key = private_key
        self._private_key_path = private_key_path
        self._private_key_passphrase = private_key_passphrase

    @classmethod
    def from_env(cls) -> SnowflakeClient:
        """Construct a client from ``SNOWFLAKE_*`` environment variables. (Phase 2.)"""
        raise NotImplementedError

    # -- query surface ----------------------------------------------------

    def query(self, sql: str, params: Params = None) -> list[Row]:
        """Run ``sql`` and return all rows (fetching every partition). (Phase 2.)"""
        raise NotImplementedError

    def query_one(self, sql: str, params: Params = None) -> Optional[Row]:
        """Return the first row, or ``None`` if the result is empty. (Phase 2.)"""
        raise NotImplementedError

    def query_scalar(self, sql: str, params: Params = None) -> Any:
        """Return the first column of the first row. (Phase 2.)"""
        raise NotImplementedError

    def query_column(self, sql: str, params: Params = None) -> list[Any]:
        """Return the first column across all rows. (Phase 2.)"""
        raise NotImplementedError

    # -- DML --------------------------------------------------------------

    def execute(self, sql: str, params: Params = None) -> int:
        """Run a DML/DDL statement and return the rows affected. (Phase 2.)"""
        raise NotImplementedError

    def insert_many(
        self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]
    ) -> int:
        """Batch-insert ``rows`` into ``table`` via server-side bindings. (Phase 2.)"""
        raise NotImplementedError

    # -- async submit -----------------------------------------------------

    def submit(self, sql: str, params: Params = None) -> QueryHandle:
        """Submit a long-running statement and return a :class:`QueryHandle`. (Phase 2.)"""
        raise NotImplementedError

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        """Close the underlying transport. (Phase 2.)"""
        raise NotImplementedError

    def __enter__(self) -> SnowflakeClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
