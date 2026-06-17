"""snowflake-sql-api: a lightweight, pure-Python client for Snowflake's SQL API v2.

Public surface:

- :class:`SnowflakeClient` / :class:`AsyncSnowflakeClient` - the sync and async
  clients (paired surface).
- :class:`QueryHandle` / :class:`AsyncQueryHandle` - handles to async-submitted
  statements.
- :class:`RetryPolicy` - transport retry configuration.
- the exception hierarchy rooted at :class:`SnowflakeError`.
"""

from __future__ import annotations

__version__ = "0.1.0.dev0"

from .aclient import AsyncQueryHandle, AsyncSnowflakeClient
from .client import QueryHandle, SnowflakeClient
from .exceptions import (
    ResultNotReady,
    SnowflakeAuthError,
    SnowflakeConfigError,
    SnowflakeError,
    SnowflakeProgrammingError,
    SnowflakeRequestError,
    SnowflakeRetryError,
    SnowflakeTimeoutError,
)
from .retry import RetryPolicy

__all__ = [
    "__version__",
    "SnowflakeClient",
    "AsyncSnowflakeClient",
    "QueryHandle",
    "AsyncQueryHandle",
    "RetryPolicy",
    "SnowflakeError",
    "SnowflakeConfigError",
    "SnowflakeAuthError",
    "SnowflakeRequestError",
    "SnowflakeProgrammingError",
    "SnowflakeTimeoutError",
    "SnowflakeRetryError",
    "ResultNotReady",
]
