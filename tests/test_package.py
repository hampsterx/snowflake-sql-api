"""Scaffold-level tests: package wiring, exports, and exception hierarchy.

These assert the package imports cleanly and its public surface is wired up.
Behavioral tests (auth, transport, types, pagination) land in Phase 2.
"""

from __future__ import annotations

import snowflake_sql_api as sf
from snowflake_sql_api.exceptions import (
    ResultNotReady,
    SnowflakeError,
    SnowflakeProgrammingError,
    SnowflakeRequestError,
)
from snowflake_sql_api.retry import DEFAULT_RETRY_POLICY, RetryPolicy


def test_version_is_populated() -> None:
    assert isinstance(sf.__version__, str)
    assert sf.__version__


def test_public_exports_present() -> None:
    for name in sf.__all__:
        assert hasattr(sf, name), f"missing public export: {name}"


def test_client_classes_exported() -> None:
    assert sf.SnowflakeClient is not None
    assert sf.AsyncSnowflakeClient is not None
    assert sf.QueryHandle is not None
    assert sf.AsyncQueryHandle is not None


def test_exception_hierarchy() -> None:
    # Every typed error is a SnowflakeError.
    assert issubclass(SnowflakeRequestError, SnowflakeError)
    assert issubclass(ResultNotReady, SnowflakeError)
    # A programming error is a request error (HTTP 422 specialization).
    assert issubclass(SnowflakeProgrammingError, SnowflakeRequestError)


def test_request_error_carries_envelope_fields() -> None:
    err = SnowflakeRequestError(
        "boom", status_code=422, code="000904", sql_state="42000", request_id="req-1"
    )
    assert err.status_code == 422
    assert err.code == "000904"
    assert err.sql_state == "42000"
    assert err.request_id == "req-1"


def test_result_not_ready_carries_handle() -> None:
    err = ResultNotReady(statement_handle="abc-123")
    assert err.statement_handle == "abc-123"
    assert isinstance(err, SnowflakeError)


def test_default_retry_policy_shape() -> None:
    assert isinstance(DEFAULT_RETRY_POLICY, RetryPolicy)
    assert DEFAULT_RETRY_POLICY.max_attempts == 3
    # Frozen dataclass: fields are immutable.
    assert 429 in DEFAULT_RETRY_POLICY.retry_statuses
