"""HTTP transport against ``POST /api/v2/statements``.

Owns the actual ``httpx`` calls (sync and async), status-code handling (200
success, 202 long-running submit / status poll, 422 execution failure), retry
wiring, and ``requestId`` reuse for idempotent retries. Higher layers
(``client``/``aclient``) talk to the transport, not to ``httpx`` directly.

Scaffold only: request execution lands in Phase 2.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .auth import KeypairAuthenticator
from .retry import DEFAULT_RETRY_POLICY, RetryPolicy

__all__ = ["Transport", "AsyncTransport", "StatementResponse"]


class StatementResponse:
    """A decoded SQL API response (status, headers, JSON body).

    Wraps the raw HTTP result so callers branch on ``status_code`` and the
    Snowflake envelope without re-touching ``httpx``. Populated in Phase 2.
    """

    def __init__(self, status_code: int, body: Dict[str, Any]) -> None:
        self.status_code = status_code
        self.body = body


class Transport:
    """Synchronous SQL API transport."""

    def __init__(
        self,
        host: str,
        authenticator: KeypairAuthenticator,
        *,
        timeout: float = 60.0,
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
    ) -> None:
        self.host = host
        self.authenticator = authenticator
        self.timeout = timeout
        self.retry_policy = retry_policy

    def execute_statement(
        self, payload: Dict[str, Any], *, request_id: Optional[str] = None
    ) -> StatementResponse:
        """POST a statement, applying retries and idempotent ``requestId``. (Phase 2.)"""
        raise NotImplementedError

    def close(self) -> None:
        """Close the underlying HTTP client. (Phase 2.)"""
        raise NotImplementedError


class AsyncTransport:
    """Asynchronous SQL API transport (same surface, ``await``-based)."""

    def __init__(
        self,
        host: str,
        authenticator: KeypairAuthenticator,
        *,
        timeout: float = 60.0,
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
    ) -> None:
        self.host = host
        self.authenticator = authenticator
        self.timeout = timeout
        self.retry_policy = retry_policy

    async def execute_statement(
        self, payload: Dict[str, Any], *, request_id: Optional[str] = None
    ) -> StatementResponse:
        """POST a statement, applying retries and idempotent ``requestId``. (Phase 2.)"""
        raise NotImplementedError

    async def aclose(self) -> None:
        """Close the underlying async HTTP client. (Phase 2.)"""
        raise NotImplementedError
