"""Typed exception hierarchy for ``snowflake-sql-api``.

All exceptions derive from :class:`SnowflakeError`, so a caller can catch the
whole family with a single ``except`` clause. The transport layer maps SQL API
HTTP responses onto these types; see :mod:`snowflake_sql_api.transport`.
"""

from __future__ import annotations

from typing import Optional

__all__ = [
    "SnowflakeError",
    "SnowflakeConfigError",
    "SnowflakeAuthError",
    "SnowflakeRequestError",
    "SnowflakeProgrammingError",
    "SnowflakeTimeoutError",
    "SnowflakeRetryError",
    "ResultNotReady",
]


class SnowflakeError(Exception):
    """Base class for every error raised by this library."""


class SnowflakeConfigError(SnowflakeError):
    """Client constructed with invalid or missing configuration.

    Raised before any network call (e.g. no account, unreadable private key,
    a feature used without its optional extra installed).
    """


class SnowflakeAuthError(SnowflakeError):
    """Keypair JWT generation or Snowflake authentication failed."""


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
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.sql_state = sql_state
        self.request_id = request_id
        self.statement_handle = statement_handle


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
