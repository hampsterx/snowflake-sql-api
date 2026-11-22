"""Submission identity and replay wire contracts, paired across both clients.

Mocked successes prove request construction, not Snowflake deduplication. The
opt-in smoke probes independently check server execution counts.
"""

from __future__ import annotations

import inspect
import json
from contextlib import AsyncExitStack, asynccontextmanager
from decimal import Decimal
from uuid import UUID

import httpx
import pytest

from snowflake_sql_api import (
    AsyncQueryHandle,
    AsyncSnowflakeClient,
    QueryHandle,
    SnowflakeClient,
)
from snowflake_sql_api.exceptions import (
    SnowflakeAuthError,
    SnowflakeConfigError,
    SnowflakeError,
    SnowflakeForbiddenError,
    SnowflakeProgrammingError,
    SnowflakeRequestError,
    SnowflakeRetryError,
    SnowflakeTimeoutError,
)
from snowflake_sql_api.retry import RetryPolicy

from .support import ACCOUNT, USER, ok_body, running_body

SAVED_ID = "a0b1c2d3-e4f5-1678-9abc-def012345678"
SQL = "INSERT INTO jobs VALUES (?, ?)"


async def value(result):
    return await result if inspect.isawaitable(result) else result


@asynccontextmanager
async def client_for(async_mode, private_key_pem, handler, **options):
    async with AsyncExitStack() as stack:
        transport = httpx.MockTransport(handler)
        if async_mode:
            http = await stack.enter_async_context(
                httpx.AsyncClient(transport=transport)
            )
            cls = AsyncSnowflakeClient
        else:
            http = stack.enter_context(httpx.Client(transport=transport))
            cls = SnowflakeClient
        client = cls(
            ACCOUNT,
            USER,
            private_key=private_key_pem,
            http_client=http,
            poll_interval=0,
            retry_policy=RetryPolicy(max_attempts=3, base_backoff=0, max_backoff=0),
            **options,
        )
        if async_mode:
            await stack.enter_async_context(client)
        else:
            stack.enter_context(client)
        yield client


def recorder(requests, status=202, body=None):
    def respond(request):
        requests.append(request)
        return httpx.Response(
            status, json=running_body("saved-handle") if body is None else body
        )

    return respond


@pytest.mark.parametrize("explicit_none", [False, True])
async def test_generated_unique_uuid4(async_mode, private_key_pem, explicit_none):
    requests, hooks = [], []
    async with client_for(
        async_mode,
        private_key_pem,
        recorder(requests),
        on_query=lambda *a: hooks.append(a),
    ) as client:
        kwargs = {"request_id": None} if explicit_none else {}
        handles = [
            await value(client.submit(SQL, [7, "queued"], **kwargs)) for _ in range(2)
        ]
    ids = [handle.request_id for handle in handles]
    assert len(set(ids)) == 2
    assert all(UUID(identity).version == 4 for identity in ids)
    assert [request.url.params["requestId"] for request in requests] == ids
    assert all("retry" not in request.url.params for request in requests)
    assert hooks == [(SQL, [7, "queued"])] * 2


@pytest.mark.parametrize("operation", ["submit", "resubmit"])
@pytest.mark.parametrize(
    "identity",
    [
        SAVED_ID.upper(),
        SAVED_ID.replace("-", ""),
        "{" + SAVED_ID + "}",
        "urn:uuid:" + SAVED_ID,
        UUID(SAVED_ID),
        UUID(int=0),
    ],
)
async def test_canonical_caller_identity(
    async_mode, private_key_pem, operation, identity
):
    requests = []
    expected = str(identity) if isinstance(identity, UUID) else str(UUID(identity))
    async with client_for(async_mode, private_key_pem, recorder(requests)) as client:
        handle = await value(
            getattr(client, operation)(SQL, [7, "queued"], request_id=identity)
        )
    assert handle.request_id == expected
    assert handle.statement_handle == "saved-handle"
    assert isinstance(handle, AsyncQueryHandle if async_mode else QueryHandle)
    assert dict(requests[0].url.params) == {
        "requestId": expected,
        "async": "true",
        **({"retry": "true"} if operation == "resubmit" else {}),
    }


@pytest.mark.parametrize("operation", ["submit", "resubmit"])
@pytest.mark.parametrize(
    "identity", ["", "not-a-uuid", "f" * 31, 1, True, b"a" * 32, [], {}]
)
async def test_invalid_id_has_no_http_or_hook(
    async_mode, private_key_pem, operation, identity
):
    requests, hooks = [], []
    async with client_for(
        async_mode,
        private_key_pem,
        recorder(requests),
        on_query=lambda *a: hooks.append(a),
    ) as client:
        with pytest.raises(SnowflakeConfigError) as caught:
            await value(getattr(client, operation)(SQL, request_id=identity))
    assert caught.value.request_id is None
    assert requests == hooks == []


async def test_resubmit_requires_non_null_keyword(async_mode, private_key_pem):
    requests, hooks = [], []
    async with client_for(
        async_mode,
        private_key_pem,
        recorder(requests),
        on_query=lambda *a: hooks.append(a),
    ) as client:
        with pytest.raises(TypeError):
            await value(client.resubmit(SQL))
        with pytest.raises(SnowflakeConfigError) as caught:
            await value(client.resubmit(SQL, request_id=None))
    assert caught.value.request_id is None
    assert requests == hooks == []


@pytest.mark.parametrize("operation", ["submit", "resubmit"])
async def test_retry_wire_contract(async_mode, private_key_pem, operation):
    requests, hooks = [], []

    def respond(request):
        requests.append(request)
        if len(requests) == 1:
            raise httpx.ReadError("lost response", request=request)
        if len(requests) == 2:
            return httpx.Response(503, json={"message": "unavailable"})
        return httpx.Response(202, json=running_body("saved-handle"))

    async with client_for(
        async_mode,
        private_key_pem,
        respond,
        on_query=lambda *a: hooks.append(a),
        database="DB",
        schema="SCHEMA",
        warehouse="WH",
        role="ROLE",
        timezone="UTC",
        parameters={"QUERY_TAG": "synthetic"},
        statement_timeout=80,
    ) as client:
        handle = await value(
            getattr(client, operation)(SQL, [7, "queued"], request_id=SAVED_ID.upper())
        )
    assert handle.request_id == SAVED_ID
    assert len(requests) == 3
    assert all(request.url.params["requestId"] == SAVED_ID for request in requests)
    assert all(request.url.params["async"] == "true" for request in requests)
    assert [request.url.params.get("retry") for request in requests] == (
        [None, "true", "true"] if operation == "submit" else ["true"] * 3
    )
    assert len({request.content for request in requests}) == 1
    assert json.loads(requests[0].content) == {
        "statement": SQL,
        "bindings": {
            "1": {"type": "FIXED", "value": "7"},
            "2": {"type": "TEXT", "value": "queued"},
        },
        "database": "DB",
        "schema": "SCHEMA",
        "warehouse": "WH",
        "role": "ROLE",
        "parameters": {"QUERY_TAG": "synthetic", "TIMEZONE": "UTC"},
        "timeout": 80,
    }
    assert hooks == [(SQL, [7, "queued"])]


async def test_reconstructed_job_new_client_same_payload(async_mode, private_key_pem):
    saved = json.dumps(
        {
            "id": SAVED_ID,
            "sql": SQL,
            "params": [7, "queued"],
            "context": {
                "database": "DB",
                "schema": "SCHEMA",
                "warehouse": "WH",
                "role": "ROLE",
                "timezone": "UTC",
                "statement_timeout": 80,
                "parameters": {"QUERY_TAG": "synthetic"},
            },
        }
    )
    requests = []
    for operation in ["submit", "resubmit"]:
        job = json.loads(saved)
        async with client_for(
            async_mode, private_key_pem, recorder(requests), **job["context"]
        ) as client:
            handle = await value(
                getattr(client, operation)(
                    job["sql"], job["params"], request_id=job["id"]
                )
            )
        assert handle.request_id == job["id"]
    assert len(requests) == 2
    assert requests[0].content == requests[1].content
    assert (
        requests[0].url.params["requestId"]
        == requests[1].url.params["requestId"]
        == SAVED_ID
    )
    assert "retry" not in requests[0].url.params
    assert requests[1].url.params["retry"] == "true"


@pytest.mark.parametrize("operation", ["submit", "resubmit"])
@pytest.mark.parametrize(
    "status, error_type",
    [
        (400, SnowflakeRequestError),
        (401, SnowflakeAuthError),
        (403, SnowflakeForbiddenError),
        (422, SnowflakeProgrammingError),
        (503, SnowflakeRetryError),
    ],
)
async def test_submission_errors_carry_identity(
    async_mode, private_key_pem, operation, status, error_type
):
    requests = []
    envelope = {
        "message": "server message",
        "code": "123",
        "sqlState": "XYZ",
        "statementHandle": "failed-handle",
    }
    async with client_for(
        async_mode, private_key_pem, recorder(requests, status, envelope)
    ) as client:
        with pytest.raises(error_type) as caught:
            await value(getattr(client, operation)(SQL, request_id=SAVED_ID.upper()))
    exc = caught.value
    assert type(exc) is error_type
    assert exc.request_id == SAVED_ID
    assert exc.submission_attempts == (3 if status == 503 else 1)
    cause = exc.__cause__ if status == 503 else exc
    assert cause.args == ("server message",)
    assert cause.code == "123"
    assert cause.sql_state == "XYZ"
    assert cause.statement_handle == "failed-handle"


async def test_exhausted_transport_preserves_cause(async_mode, private_key_pem):
    original = httpx.ReadTimeout("response lost")
    requests = []

    def respond(request):
        requests.append(request)
        raise original

    async with client_for(async_mode, private_key_pem, respond) as client:
        with pytest.raises(SnowflakeRetryError) as caught:
            await value(client.resubmit(SQL, request_id=SAVED_ID))
    assert caught.value.request_id == SAVED_ID
    assert caught.value.__cause__ is original
    assert original.args == ("response lost",)
    assert not hasattr(original, "request_id")
    assert len(requests) == 3


@pytest.mark.parametrize("operation", ["submit", "resubmit"])
async def test_binding_failure_has_identity(async_mode, private_key_pem, operation):
    requests, hooks = [], []
    async with client_for(
        async_mode,
        private_key_pem,
        recorder(requests),
        on_query=lambda *a: hooks.append(a),
    ) as client:
        with pytest.raises(SnowflakeConfigError) as caught:
            await value(
                getattr(client, operation)(SQL, [object()], request_id=SAVED_ID)
            )
    assert caught.value.request_id == SAVED_ID
    assert len(hooks) == 1
    assert requests == []


@pytest.mark.parametrize(
    "error_type", [SnowflakeError, SnowflakeAuthError, SnowflakeTimeoutError]
)
async def test_original_exception_identity_args_cause_traceback(
    async_mode, private_key_pem, error_type, monkeypatch
):
    original = error_type("original", "second arg")
    cause = ValueError("cause")

    def broken_payload(*args):
        raise original from cause

    async with client_for(async_mode, private_key_pem, recorder([])) as client:
        monkeypatch.setattr(client, "_build_payload", broken_payload)
        with pytest.raises(error_type) as caught:
            await value(client.submit(SQL))
    assert caught.value is original
    assert original.args == ("original", "second arg")
    assert original.__cause__ is cause
    assert UUID(original.request_id).version == 4
    frames = []
    traceback = original.__traceback__
    while traceback:
        frames.append(traceback.tb_frame.f_code.co_name)
        traceback = traceback.tb_next
    assert frames.count("_submit_handle") == 1
    assert frames[-1] == "broken_payload"


async def test_regression_missing_handle_gets_submission_identity(
    async_mode, private_key_pem
):
    async with client_for(async_mode, private_key_pem, recorder([], 200, {})) as client:
        with pytest.raises(
            SnowflakeRequestError, match="no statement handle"
        ) as caught:
            await value(client.resubmit(SQL, request_id=SAVED_ID))
    assert caught.value.request_id == SAVED_ID


@pytest.mark.parametrize("error_type", [RuntimeError, SnowflakeConfigError])
async def test_callback_exception_is_not_annotated(
    async_mode, private_key_pem, error_type
):
    original = error_type("callback failed")
    requests = []

    def hook(*args):
        raise original

    async with client_for(
        async_mode, private_key_pem, recorder(requests), on_query=hook
    ) as client:
        with pytest.raises(error_type) as caught:
            await value(client.resubmit(SQL, request_id=SAVED_ID))
    assert caught.value is original
    assert getattr(original, "request_id", None) is None
    assert requests == []


async def test_handle_lifecycle_and_saved_attachment(async_mode, private_key_pem):
    requests, hooks = [], []

    def respond(request):
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, json=ok_body([], [], handle="saved-handle"))
        return httpx.Response(202, json=running_body("saved-handle"))

    async with client_for(
        async_mode, private_key_pem, respond, on_query=lambda *a: hooks.append(a)
    ) as client:
        handle = await value(client.resubmit(SQL, request_id=SAVED_ID))
        assert await value(handle.status()) == "SUCCESS"
        assert await value(handle.result()) == []
        await value(handle.cancel())
        cls = AsyncQueryHandle if async_mode else QueryHandle
        attached = cls(client, "saved-handle")
        assert attached.request_id is None
        assert await value(attached.status()) == "SUCCESS"
        assert cls(client, "saved-handle", request_id=SAVED_ID).request_id == SAVED_ID
    assert hooks == [(SQL, None)]
    assert [request.method for request in requests] == [
        "POST",
        "GET",
        "GET",
        "POST",
        "GET",
    ]
    assert all("requestId" not in request.url.params for request in requests[1:])


async def test_poll_failure_has_no_fabricated_request_id(async_mode, private_key_pem):
    def respond(request):
        if request.method == "POST":
            return httpx.Response(202, json=running_body("saved-handle"))
        return httpx.Response(401, json={"message": "expired"})

    async with client_for(async_mode, private_key_pem, respond) as client:
        handle = await value(client.submit(SQL, request_id=SAVED_ID))
        with pytest.raises(SnowflakeAuthError) as caught:
            await value(handle.result())
    assert caught.value.request_id is None
    assert handle.request_id == SAVED_ID


def test_error_constructor_compatibility():
    assert SnowflakeError("a", "b").args == ("a", "b")
    assert SnowflakeTimeoutError("timeout").request_id is None
    assert SnowflakeRequestError("error", request_id=SAVED_ID).request_id == SAVED_ID


async def test_live_adapter_discards_reply_before_worker_sees_handle(
    async_mode, private_key_pem, monkeypatch
):
    from .smoke.test_submissions import (
        LoseAsyncResponse,
        LoseSyncResponse,
        LostResponse,
    )

    requests = []
    inner = httpx.MockTransport(recorder(requests))
    observation = LostResponse(SAVED_ID)
    monkeypatch.setattr(
        httpx, "AsyncHTTPTransport" if async_mode else "HTTPTransport", lambda: inner
    )
    async with AsyncExitStack() as stack:
        if async_mode:
            http = await stack.enter_async_context(
                httpx.AsyncClient(transport=LoseAsyncResponse(observation))
            )
            cls = AsyncSnowflakeClient
        else:
            http = stack.enter_context(
                httpx.Client(transport=LoseSyncResponse(observation))
            )
            cls = SnowflakeClient
        client = cls(
            ACCOUNT,
            USER,
            private_key=private_key_pem,
            http_client=http,
            retry_policy=RetryPolicy(max_attempts=1),
        )
        with pytest.raises(SnowflakeRetryError) as caught:
            await value(client.submit(SQL, [7, "synthetic"], request_id=SAVED_ID))
        await value(client.aclose() if async_mode else client.close())
    assert caught.value.request_id == SAVED_ID
    assert caught.value.submission_attempts == 1
    assert isinstance(caught.value.__cause__, httpx.ReadError)
    assert getattr(caught.value, "statement_handle", None) is None
    assert observation.handle == "saved-handle"
    assert observation.received_at is not None
    assert observation.forwarded == len(requests) == 1


@pytest.mark.parametrize("operation", ["submit", "resubmit"])
@pytest.mark.parametrize("failure", ["binding", "decoding"])
async def test_non_library_submission_error_keeps_original_type_and_no_id(
    async_mode, private_key_pem, operation, failure
):
    requests = []
    original = httpx.DecodingError("invalid compressed response")

    def respond(request):
        requests.append(request)
        raise original

    params = [[Decimal("1.5")]] if failure == "binding" else [7, "queued"]
    error_type = TypeError if failure == "binding" else httpx.DecodingError
    async with client_for(async_mode, private_key_pem, respond) as client:
        with pytest.raises(error_type) as caught:
            await value(getattr(client, operation)(SQL, params, request_id=SAVED_ID))
    assert getattr(caught.value, "request_id", None) is None
    if failure == "binding":
        assert requests == []
    else:
        assert caught.value is original
        assert original.args == ("invalid compressed response",)
        assert original.submission_attempts == 1
        assert len(requests) == 1
        assert requests[0].url.params["requestId"] == SAVED_ID


@pytest.mark.parametrize(
    "operation",
    [
        "execute",
        "insert_many",
        "insert_batches",
        "submit",
        "resubmit",
        "execute_statements",
        "execute_transaction",
        "submit_statements",
    ],
)
async def test_regression_lost_response_then_handleless_200_is_unknown(
    async_mode, private_key_pem, operation
):
    from snowflake_sql_api import BatchWriteError, SnowflakeMultiStatementError

    requests = []

    def respond(request):
        requests.append(request)
        if len(requests) == 1:
            raise httpx.ReadError("lost response", request=request)
        return httpx.Response(200, json={})

    async with client_for(async_mode, private_key_pem, respond) as client:
        with pytest.raises(SnowflakeError) as caught:
            if operation in ("insert_many", "insert_batches"):
                result = getattr(client, operation)("events", ["id"], [[1]])
            elif operation in ("submit", "resubmit"):
                result = getattr(client, operation)(SQL, [1, "x"], request_id=SAVED_ID)
            elif operation.endswith("statements") or operation == "execute_transaction":
                result = getattr(client, operation)(["INSERT INTO events VALUES (1)"])
            else:
                result = client.execute(SQL, [1, "x"])
            await value(result)
    error = caught.value
    if isinstance(error, BatchWriteError):
        assert error.outcome == "unknown" and error.phase == "submission"
        assert error.failed_batch.submission_attempts == 2
        assert error.failed_batch.request_id == requests[0].url.params["requestId"]
        error = error.__cause__
    elif isinstance(error, SnowflakeMultiStatementError):
        assert error.outcome == "unknown" and error.phase == "submission"
        assert error.completed_results == () and error.failed_index is None
    assert error.status_code == 200 and error.submission_attempts == 2
    assert error.request_id == requests[0].url.params["requestId"]
    assert len(requests) == 2
    assert requests[1].content == requests[0].content
    assert requests[1].url.params["requestId"] == requests[0].url.params["requestId"]
    assert requests[1].url.params["retry"] == "true"
