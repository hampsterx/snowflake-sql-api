"""Shared sync/async drivers for streaming and export contract tests."""

from __future__ import annotations

import itertools
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable

import httpx
import pytest

from snowflake_sql_api import AsyncSnowflakeClient, RetryPolicy, SnowflakeClient
from snowflake_sql_api.testing import FakeSnowflake, make_async_client, make_client

from .support import ACCOUNT, USER


def expire_poll_deadlines(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make every client polling deadline pass by the first running poll.

    Both clients read ``monotonic`` from their own module, so only their
    deadlines move; the event loop's clock is untouched. Each reading lands a
    million seconds after the last, past any positive ``statement_timeout``.
    """
    clock = itertools.count(0.0, 1_000_000.0).__next__
    monkeypatch.setattr("snowflake_sql_api.client.monotonic", clock)
    monkeypatch.setattr("snowflake_sql_api.aclient.monotonic", clock)


@asynccontextmanager
async def fake_client(
    fake: FakeSnowflake, async_mode: bool, **kwargs: Any
) -> AsyncIterator[Any]:
    if async_mode:
        async with make_async_client(fake, **kwargs) as client:
            yield client
    else:
        with make_client(fake, **kwargs) as client:
            yield client


@asynccontextmanager
async def wire_client(
    handler: Callable[[httpx.Request], httpx.Response],
    private_key: bytes,
    async_mode: bool,
    **kwargs: Any,
) -> AsyncIterator[Any]:
    options = {
        "private_key": private_key,
        "poll_interval": 0,
        "retry_policy": RetryPolicy(base_backoff=0),
        **kwargs,
    }
    transport = httpx.MockTransport(handler)
    if async_mode:
        async with (
            httpx.AsyncClient(transport=transport) as http,
            AsyncSnowflakeClient(ACCOUNT, USER, http_client=http, **options) as client,
        ):
            yield client
    else:
        with (
            httpx.Client(transport=transport) as http,
            SnowflakeClient(ACCOUNT, USER, http_client=http, **options) as client,
        ):
            yield client


async def advance(stream: Any, async_mode: bool) -> Any:
    return await anext(stream) if async_mode else next(stream)


async def collect(stream: Any, async_mode: bool) -> list[Any]:
    return [row async for row in stream] if async_mode else list(stream)


async def close(stream: Any, async_mode: bool) -> None:
    if async_mode:
        await stream.aclose()
    else:
        stream.close()


async def call(client: Any, async_mode: bool, method: str, *args: Any) -> Any:
    result = getattr(client, method)(*args)
    return await result if async_mode else result
