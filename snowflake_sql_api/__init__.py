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

try:
    # Written at build time by hatch-vcs from the git tag (see pyproject.toml).
    from ._version import __version__
except ImportError:  # pragma: no cover - plain source checkout, never built
    # No build-generated version file (e.g. running from a raw clone). Fall back
    # to installed package metadata, then to a dev placeholder.
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as _pkg_version

    try:
        __version__ = _pkg_version("snowflake-sql-api")
    except PackageNotFoundError:
        __version__ = "0.0.0.dev0"

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
