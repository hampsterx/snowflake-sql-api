# Replayable submissions

`submit()` and `resubmit()` support an application-owned request identity across
worker lifetimes. Both send a single statement with `async=true` and return a
handle for polling. This workflow also supports DML; `execute()` and the
query/export and batch convenience methods do not accept caller request IDs.

## Identity and notification contract

```python
submit(sql, params=None, *, request_id=None)
resubmit(sql, params=None, *, request_id=required_id)
```

The async client exposes the same arguments and returns an `AsyncQueryHandle`
when awaited. IDs accept UUID objects or any string parseable by `uuid.UUID`.
The wire, returned handle and submission error use lowercase hyphenated UUID
text. Non-v4 and nil UUIDs are syntax-valid; uniqueness across logical operations
is the caller's responsibility. Every changed logical operation needs a fresh ID.

Omitting an ID or supplying `None` to `submit()` generates a UUID4. A generated
ID is available on `handle.request_id` or a submission `SnowflakeError`, but a
worker can die before either reaches it. Allocate and persist the ID yourself
for durable work. `resubmit()` requires the keyword and rejects `None`; it never
allocates a replacement. Invalid IDs raise `SnowflakeConfigError` with
`request_id=None` before hooks, binding or HTTP.

The normal first submit omits `retry`. Explicit resubmission sends `retry=true`
immediately. Subsequent transient HTTP retries retain the same body and ID and
set `retry=true`. `on_query` fires once per explicit submit/resubmit call, before
binding/HTTP, and never for individual HTTP retries, polling or attaching a known
handle. It records an attempt, not proof of a new database execution.

After ID allocation, library submission failures, including binding and
missing-handle failures, expose `error.request_id`. Exception types, original
arguments and chained causes remain intact. Callback exceptions and GET/poll
failures do not gain submission metadata; correlate polling through the handle.
The ID diagnostic adds no SQL, bind values, credentials or persistent records.
Existing server messages and underlying causes remain available as before.

## Persist before the first POST

The application supplies the durable store. The following application callbacks
represent committed storage operations, not functions supplied by this library.
The example uses integer and string inputs so a JSON round trip preserves their
binding types.

```python
from uuid import uuid4

from snowflake_sql_api import QueryHandle, SnowflakeClient

# Application code: commit this complete job record before any Snowflake HTTP.
job = {
    "request_id": str(uuid4()),
    "sql": "INSERT INTO job_events (id, label) VALUES (?, ?)",
    "params": [7, "queued"],
    "account": "myorg-myaccount",
    "user": "WORKER_USER",
    "context": {
        "database": "JOB_DB",
        "schema": "PUBLIC",
        "warehouse": "JOB_WH",
        "role": "JOB_ROLE",
        "timezone": "UTC",
        "statement_timeout": 120,
        "parameters": {"QUERY_TAG": "durable-job"},
    },
    "statement_handle": None,
}
job_key = persist_job(job)  # Must commit before submit().

with SnowflakeClient(
    account=job["account"], user=job["user"],
    private_key_path=worker_key_path, **job["context"],
) as client:
    handle = client.submit(job["sql"], job["params"], request_id=job["request_id"])
    persist_handle(job_key, handle.statement_handle)
    handle.result()
    mark_done(job_key)

# A different process loads only committed state and uses the same account/user.
job = load_job(job_key)
with SnowflakeClient(
    account=job["account"], user=job["user"],
    private_key_path=worker_key_path, **job["context"],
) as client:
    if job["statement_handle"]:
        handle = QueryHandle(
            client, job["statement_handle"], request_id=job["request_id"],
        )
    else:
        handle = client.resubmit(
            job["sql"], job["params"], request_id=job["request_id"],
        )
        persist_handle(job_key, handle.statement_handle)
    handle.result()
    mark_done(job_key)
```

Use `AsyncSnowflakeClient` and `AsyncQueryHandle` in an async worker; await
`submit()`, `resubmit()` and `handle.result()` and use `async with`. Attaching a
known handle performs no POST and is compatible with the existing two-argument
`QueryHandle(client, statement_handle)` constructor, whose `request_id` is `None`.

Preserve the account, user identity, SQL, parameter **types and values**,
database/schema/warehouse/role, timezone, statement timeout and statement
parameters. Explicit context avoids changed user/account defaults after restart.
Authentication credentials can rotate while retaining the same account/user;
never persist private keys or tokens in job records.

For Decimal, binary, date and timestamp inputs, use typed persistence and restore
the original Python type, value, precision and timezone. Turning a Decimal into
an ordinary string changes a FIXED binding into TEXT. A JSON serializer's string
fallback alone does not preserve these invariants. The library keeps no original
body across processes, cannot compare a replay to it, and does not promise that
Snowflake detects conflicting payloads. A changed operation needs a new UUID.

## Uncertainty and limits

Only the initial attempt uses `submit(request_id=...)`. Once it might have been
sent, recovery uses `resubmit()` with that same ID and identical inputs. Calling
ordinary submit again with the same ID omits the required retry flag and is not
a supported recovery recipe. If replay is uncertain, keep the same durable job
and ID. A timeout, exhausted retries or worker death does not establish failure;
never automatically start the operation again with a fresh UUID.

[Snowflake's resubmission documentation](https://docs.snowflake.com/en/developer-guide/sql-api/submitting-requests#resubmitting-a-request-to-execute-sql-statements)
describes UUID `requestId` plus `retry=true`, avoiding re-execution of statements
that already succeed. History lookup adds overhead. The documented mechanism
supplies no unlimited deduplication lifetime. Escalate unresolved old requests
for business reconciliation rather than silently changing IDs. Business
idempotency and completion of any downstream work remain application concerns.

[Snowflake's query-size guidance](https://docs.snowflake.com/en/user-guide/query-size-limits)
also constrains reliable retained retry metadata: large statements can execute
while their persisted query text is truncated. Do not assume arbitrarily large
SQL/inputs remain replayable. This client introduces no universal HTTP size limit
or guaranteed replay window.

## Live verification

**Verification status: server deduplication and restart safety are unverified
until the opt-in probes pass in the intended disposable environment.** Mocked
wire tests verify request construction only. In particular, recovery from a crash
between persisting the ID and sending the first POST needs the never-sent-ID
probe. If Snowflake rejects an unseen retry ID, stop and revise that recovery
design; never fall back automatically to a normal submit or a new ID.

Run the probes with credentials and an explicit disposable namespace:

```bash
export SNOWFLAKE_LIVE_DML=1
# Also set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_PATH,
# SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE.
# Optional: SNOWFLAKE_ROLE, SNOWFLAKE_HOST, SNOWFLAKE_PRIVATE_KEY_PASSPHRASE.
python -m pytest tests/smoke/test_submissions.py -v --junitxml=replay-results.xml
```

The paired sync/async tests create uniquely named owned tables and, for the
running case, a procedure that waits before inserting a synthetic row. Setup
happens outside the tested request. The first worker allows exactly one HTTP
attempt. A forwarding HTTP adapter consumes a real successful response, records
its handle only in the harness, and raises a transport error before the worker
sees it. That client closes before a new client recovers from saved inputs and
the UUID alone. The harness observes original completion, or RUNNING for the
running case, before replay and waits for both original and replay handles
before an independent row-count query. A second execution cannot hide behind an
unfinished handle when the count is read. Owned resources are dropped afterward.

Separate cases check an ID never sent before resubmission and an unchanged SQL
failure; corrected SQL uses a new ID. JUnit properties record probe timestamps,
the response-to-replay interval, account/user consistency, error codes and actual
row counts where applicable. A skipped probe proves none of these server
properties, and a successful short-interval probe supplies no unlimited-delay
guarantee.
