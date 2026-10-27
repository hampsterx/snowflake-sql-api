"""HTTP transport against ``POST /api/v2/statements``.

Owns the actual ``httpx`` calls (sync and async), status-code interpretation
(200 success, 202 long-running/async, 422 execution failure, 401 auth,
403 forbidden, 429/5xx retryable), the retry loop, and ``requestId`` reuse for
idempotent retries. A retried submit re-POSTs the identical body with the same
``requestId`` and ``retry=true`` so Snowflake deduplicates instead of
double-applying DML.

Higher layers (``client``/``aclient``) talk to the transport, not to ``httpx``.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, Optional

import httpx

from .exceptions import (
    SnowflakeAuthError,
    SnowflakeForbiddenError,
    SnowflakeProgrammingError,
    SnowflakeRequestError,
    SnowflakeRetryError,
)
from .retry import DEFAULT_RETRY_POLICY, RetryPolicy

__all__ = ["Transport", "AsyncTransport", "StatementResponse"]

STATEMENTS_PATH = "/api/v2/statements"
TOKEN_TYPE = "KEYPAIR_JWT"


def _default_user_agent() -> str:
    # Imported lazily to avoid a circular import at module load time.
    from . import __version__

    return f"snowflake-sql-api/{__version__}"


class StatementResponse:
    """A decoded SQL API response: HTTP status plus the parsed JSON body."""

    __slots__ = ("status_code", "body", "submission_attempts", "_submission_handle")

    def __init__(
        self,
        status_code: int,
        body: Dict[str, Any],
        submission_attempts: Optional[int] = None,
    ) -> None:
        self.status_code = status_code
        self.body = body
        self.submission_attempts = submission_attempts
        self._submission_handle: Optional[str] = None

    @property
    def is_running(self) -> bool:
        """True for a 202 (statement still executing / submitted async)."""
        return self.status_code == 202

    @property
    def statement_handle(self) -> Optional[str]:
        """Latest known handle, including a previous attempt of this submission."""
        return self.body.get("statementHandle") or self._submission_handle


def _json_body(response: httpx.Response) -> Dict[str, Any]:
    try:
        parsed = response.json()
    except ValueError:
        return {"message": response.text[:500]}
    if isinstance(parsed, dict):
        return parsed
    # Partition fetches can return a bare array; wrap it uniformly.
    return {"data": parsed}


def _interpret(response: httpx.Response) -> StatementResponse:
    """Map an HTTP response to a :class:`StatementResponse` or raise a typed error."""
    status = response.status_code
    body = _json_body(response)
    if status in (200, 202):
        return StatementResponse(status, body)
    if status == 422:
        raise SnowflakeProgrammingError(
            body.get("message", "SQL execution error"),
            status_code=status,
            code=body.get("code"),
            sql_state=body.get("sqlState"),
            statement_handle=body.get("statementHandle"),
        )
    if status == 401:
        raise SnowflakeAuthError(
            body.get("message", "authentication failed"),
            status_code=status,
            code=body.get("code"),
            sql_state=body.get("sqlState"),
            statement_handle=body.get("statementHandle"),
        )
    if status == 403:
        raise SnowflakeForbiddenError(
            body.get("message", "request forbidden"),
            status_code=status,
            code=body.get("code"),
            sql_state=body.get("sqlState"),
            statement_handle=body.get("statementHandle"),
        )
    raise SnowflakeRequestError(
        body.get("message", f"HTTP {status}"),
        status_code=status,
        code=body.get("code"),
        sql_state=body.get("sqlState"),
        statement_handle=body.get("statementHandle"),
    )


def _retry_error(response: httpx.Response) -> SnowflakeRequestError:
    """Build (not raise) the error chained as the cause of an exhausted retry."""
    body = _json_body(response)
    return SnowflakeRequestError(
        body.get("message", f"HTTP {response.status_code}"),
        status_code=response.status_code,
        code=body.get("code"),
        sql_state=body.get("sqlState"),
        statement_handle=body.get("statementHandle"),
    )


class _TransportBase:
    """Shared URL/header/param building for the sync and async transports."""

    def __init__(
        self,
        host: str,
        authenticator: Any,
        *,
        timeout: float = 60.0,
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
        user_agent: Optional[str] = None,
    ) -> None:
        self._base_url = f"https://{host}"
        self._auth = authenticator
        self._timeout = timeout
        self._retry = retry_policy
        self._user_agent = user_agent or _default_user_agent()

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._auth.token()}",
            "X-Snowflake-Authorization-Token-Type": TOKEN_TYPE,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": self._user_agent,
        }

    def _submit_params(self, request_id: str, async_exec: bool) -> Dict[str, str]:
        params = {"requestId": request_id}
        if async_exec:
            params["async"] = "true"
        return params

    @staticmethod
    def _status_path(handle: str) -> str:
        return f"{STATEMENTS_PATH}/{handle}"


class Transport(_TransportBase):
    """Synchronous SQL API transport."""

    def __init__(
        self,
        host: str,
        authenticator: Any,
        *,
        timeout: float = 60.0,
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
        user_agent: Optional[str] = None,
        client: Optional[httpx.Client] = None,
    ) -> None:
        super().__init__(
            host,
            authenticator,
            timeout=timeout,
            retry_policy=retry_policy,
            user_agent=user_agent,
        )
        self._client = client or httpx.Client(timeout=timeout)
        self._owns_client = client is None

    def submit(
        self,
        payload: Dict[str, Any],
        *,
        request_id: str,
        async_exec: bool = False,
        content: Optional[bytes] = None,
    ) -> StatementResponse:
        """POST a statement, retrying idempotently on transient failure."""
        return self._send(
            "POST",
            STATEMENTS_PATH,
            params=self._submit_params(request_id, async_exec),
            json=payload if content is None else None,
            content=content,
            is_submit=True,
        )

    def get_statement(
        self, handle: str, *, partition: Optional[int] = None
    ) -> StatementResponse:
        """GET a statement's status (poll) or a result partition."""
        params: Dict[str, str] = {}
        if partition is not None:
            params["partition"] = str(partition)
        return self._send("GET", self._status_path(handle), params=params)

    def cancel(self, handle: str) -> StatementResponse:
        """Cancel a running statement (``POST .../{handle}/cancel``)."""
        return self._send("POST", f"{self._status_path(handle)}/cancel")

    def _send(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, str]] = None,
        json: Optional[Dict[str, Any]] = None,
        content: Optional[bytes] = None,
        is_submit: bool = False,
    ) -> StatementResponse:
        url = self._base_url + path
        attempts = 0
        known_handle: Optional[str] = None
        try:
            last_exc: Optional[Exception] = None
            for attempt in range(self._retry.max_attempts):
                if attempt > 0:
                    if is_submit and params is not None:
                        params = {**params, "retry": "true"}
                    time.sleep(self._retry.backoff_for(attempt - 1))
                try:
                    headers = self._headers()
                    attempts += 1
                    response = self._client.request(
                        method,
                        url,
                        params=params,
                        json=json,
                        content=content,
                        headers=headers,
                    )
                except httpx.TransportError as exc:
                    last_exc = exc
                    continue
                if self._retry.should_retry_status(response.status_code):
                    last_exc = _retry_error(response)
                    if is_submit and last_exc.statement_handle:
                        known_handle = last_exc.statement_handle
                    continue
                result = _interpret(response)
                result.submission_attempts = attempts if is_submit else None
                result._submission_handle = known_handle
                return result
            raise SnowflakeRetryError(
                f"retries exhausted after {self._retry.max_attempts} attempts"
            ) from last_exc
        except Exception as exc:
            # Preserve original exception identity, including HTTPX decoding errors.
            exc.submission_attempts = attempts if is_submit else None  # type: ignore[attr-defined]
            if known_handle and not getattr(exc, "statement_handle", None):
                exc.statement_handle = known_handle  # type: ignore[attr-defined]
            raise

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


class AsyncTransport(_TransportBase):
    """Asynchronous SQL API transport (same surface, ``await``-based)."""

    def __init__(
        self,
        host: str,
        authenticator: Any,
        *,
        timeout: float = 60.0,
        retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
        user_agent: Optional[str] = None,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        super().__init__(
            host,
            authenticator,
            timeout=timeout,
            retry_policy=retry_policy,
            user_agent=user_agent,
        )
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._owns_client = client is None

    async def submit(
        self,
        payload: Dict[str, Any],
        *,
        request_id: str,
        async_exec: bool = False,
        content: Optional[bytes] = None,
    ) -> StatementResponse:
        return await self._send(
            "POST",
            STATEMENTS_PATH,
            params=self._submit_params(request_id, async_exec),
            json=payload if content is None else None,
            content=content,
            is_submit=True,
        )

    async def get_statement(
        self, handle: str, *, partition: Optional[int] = None
    ) -> StatementResponse:
        params: Dict[str, str] = {}
        if partition is not None:
            params["partition"] = str(partition)
        return await self._send("GET", self._status_path(handle), params=params)

    async def cancel(self, handle: str) -> StatementResponse:
        """Cancel a running statement (``POST .../{handle}/cancel``)."""
        return await self._send("POST", f"{self._status_path(handle)}/cancel")

    async def _send(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, str]] = None,
        json: Optional[Dict[str, Any]] = None,
        content: Optional[bytes] = None,
        is_submit: bool = False,
    ) -> StatementResponse:
        url = self._base_url + path
        attempts = 0
        known_handle: Optional[str] = None
        try:
            last_exc: Optional[Exception] = None
            for attempt in range(self._retry.max_attempts):
                if attempt > 0:
                    if is_submit and params is not None:
                        params = {**params, "retry": "true"}
                    await asyncio.sleep(self._retry.backoff_for(attempt - 1))
                try:
                    headers = self._headers()
                    attempts += 1
                    response = await self._client.request(
                        method,
                        url,
                        params=params,
                        json=json,
                        content=content,
                        headers=headers,
                    )
                except httpx.TransportError as exc:
                    last_exc = exc
                    continue
                if self._retry.should_retry_status(response.status_code):
                    last_exc = _retry_error(response)
                    if is_submit and last_exc.statement_handle:
                        known_handle = last_exc.statement_handle
                    continue
                result = _interpret(response)
                result.submission_attempts = attempts if is_submit else None
                result._submission_handle = known_handle
                return result
            raise SnowflakeRetryError(
                f"retries exhausted after {self._retry.max_attempts} attempts"
            ) from last_exc
        except Exception as exc:
            # Preserve original exception identity, including HTTPX decoding errors.
            exc.submission_attempts = attempts if is_submit else None  # type: ignore[attr-defined]
            if known_handle and not getattr(exc, "statement_handle", None):
                exc.statement_handle = known_handle  # type: ignore[attr-defined]
            raise

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()
