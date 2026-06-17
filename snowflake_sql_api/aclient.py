"""Asynchronous Snowflake SQL API client.

Same surface as :class:`~snowflake_sql_api.client.SnowflakeClient`, with
``await``-based methods and an async context manager. Sync/async parity is a
design principle: a feature on one side should exist on the other.

Scaffold only: the query surface lands in Phase 2.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Sequence

from .retry import DEFAULT_RETRY_POLICY, RetryPolicy
from .types import Row

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
    ) -> list[Row]:
        """Return the rows, optionally polling until the statement finishes. (Phase 2.)"""
        raise NotImplementedError

    async def status(self) -> str:
        """Return the current execution status without fetching results. (Phase 2.)"""
        raise NotImplementedError

    async def cancel(self) -> None:
        """Cancel the running statement. (Phase 2.)"""
        raise NotImplementedError


class AsyncSnowflakeClient:
    """Asynchronous client for the Snowflake SQL API.

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
    def from_env(cls) -> AsyncSnowflakeClient:
        """Construct a client from ``SNOWFLAKE_*`` environment variables. (Phase 2.)"""
        raise NotImplementedError

    # -- query surface ----------------------------------------------------

    async def query(self, sql: str, params: Params = None) -> list[Row]:
        """Run ``sql`` and return all rows (fetching every partition). (Phase 2.)"""
        raise NotImplementedError

    async def query_one(self, sql: str, params: Params = None) -> Optional[Row]:
        """Return the first row, or ``None`` if the result is empty. (Phase 2.)"""
        raise NotImplementedError

    async def query_scalar(self, sql: str, params: Params = None) -> Any:
        """Return the first column of the first row. (Phase 2.)"""
        raise NotImplementedError

    async def query_column(self, sql: str, params: Params = None) -> list[Any]:
        """Return the first column across all rows. (Phase 2.)"""
        raise NotImplementedError

    # -- DML --------------------------------------------------------------

    async def execute(self, sql: str, params: Params = None) -> int:
        """Run a DML/DDL statement and return the rows affected. (Phase 2.)"""
        raise NotImplementedError

    async def insert_many(
        self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]
    ) -> int:
        """Batch-insert ``rows`` into ``table`` via server-side bindings. (Phase 2.)"""
        raise NotImplementedError

    # -- async submit -----------------------------------------------------

    async def submit(self, sql: str, params: Params = None) -> AsyncQueryHandle:
        """Submit a long-running statement and return an :class:`AsyncQueryHandle`. (Phase 2.)"""
        raise NotImplementedError

    # -- lifecycle --------------------------------------------------------

    async def aclose(self) -> None:
        """Close the underlying async transport. (Phase 2.)"""
        raise NotImplementedError

    async def __aenter__(self) -> AsyncSnowflakeClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
