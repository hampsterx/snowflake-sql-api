"""Negative controls for the live rollback proof, with no Snowflake connection."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from snowflake_sql_api import SnowflakeMultiStatementError

from .smoke import test_multi_statement as smoke


@pytest.mark.asyncio
@pytest.mark.regression
@pytest.mark.parametrize(
    "fault",
    [
        None,
        "early rejection",
        "non-422",
        "missing prefix",
        "failed prefix",
        "other transaction",
    ],
)
@pytest.mark.parametrize("formatted_history", [False, True])
async def test_regression_rollback_probe_requires_execution_evidence(
    monkeypatch, async_mode, fault, formatted_history
):
    statements = []
    observations = {}

    class Client:
        def __init__(self):
            self.calls = 0

        def query(self, sql):
            return [{"value": "true"}]

        def execute_transaction(self, body):
            self.calls += 1
            if self.calls == 1:
                return SimpleNamespace(results=(None,) * 4)
            statements.extend(body)
            raise SnowflakeMultiStatementError(
                (
                    "unrelated rejection"
                    if fault == "early rejection"
                    else "Division by zero"
                ),
                statement_handle="parent",
                expected_count=5,
                phase="execution",
                outcome="failed",
                status_code=400 if fault == "non-422" else 422,
                request_id="diagnostic-id",
            )

    class Observer:
        def query(self, sql, params=None):
            if params is None:
                return [{"ID": 0, "AMOUNT": 10}, {"ID": 1, "AMOUNT": 20}]
            assert params == statements
            history = [
                {
                    "QUERY_TEXT": text,
                    "EXECUTION_STATUS": "success",
                    "TRANSACTION_ID": 42,
                }
                for text in statements
            ]
            history[-1].update(
                EXECUTION_STATUS="failed_with_error", ERROR_MESSAGE="Division by zero"
            )
            if fault == "missing prefix":
                history.pop(0)
            if fault == "failed prefix":
                history[0]["EXECUTION_STATUS"] = "failed_with_error"
            if fault == "other transaction":
                history[0]["TRANSACTION_ID"] = 43
            if formatted_history:
                for row in history:
                    row["QUERY_TEXT"] = "\n " + row["QUERY_TEXT"] + ";\r\n"
                    row["EXECUTION_STATUS"] = row["EXECUTION_STATUS"].upper()
            return history

        def execute(self, sql):
            return 1

        def query_scalar(self, sql):
            return 11

    @asynccontextmanager
    async def client(options, mode):
        assert mode == async_mode
        yield Client()

    monkeypatch.setattr(smoke, "multi_client", client)
    probe = smoke.test_transaction_commit_and_rollback_release_locks
    if fault is None:
        await probe(
            {}, ('"SYNTHETIC"', Observer()), async_mode, observations.__setitem__
        )
        assert "rollback_and_lock_release" in observations
    else:
        with pytest.raises(AssertionError):
            await probe(
                {}, ('"SYNTHETIC"', Observer()), async_mode, observations.__setitem__
            )
        assert "rollback_and_lock_release" not in observations
        assert observations["transaction_request_id"] == "diagnostic-id"
