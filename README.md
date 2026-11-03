# snowflake-sql-api

[![CI](https://github.com/hampsterx/snowflake-sql-api/actions/workflows/ci.yml/badge.svg)](https://github.com/hampsterx/snowflake-sql-api/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/snowflake-sql-api)](https://pypi.org/project/snowflake-sql-api/)
[![Python versions](https://img.shields.io/pypi/pyversions/snowflake-sql-api)](https://pypi.org/project/snowflake-sql-api/)
[![License: MIT](https://img.shields.io/pypi/l/snowflake-sql-api)](LICENSE)
[![Typed](https://img.shields.io/badge/typing-typed-blue)](https://peps.python.org/pep-0561/)

A lightweight, pure-Python client for [Snowflake's SQL API v2](https://docs.snowflake.com/en/developer-guide/sql-api/index)
(the `POST /api/v2/statements` REST endpoint).

> **Status: pre-1.0.** The API may still change between minor versions.

## Why

The official [`snowflake-connector-python`](https://pypi.org/project/snowflake-connector-python/)
is ~75 MB installed (150 MB with pandas/pyarrow) and ships a compiled extension
that slows container builds and inflates cold starts (its import alone is ~1.5 s
versus ~0.3 s for this client). For serverless and AWS Lambda workloads that only
need to run SQL, that is a lot of weight.

`snowflake-sql-api` talks to Snowflake's SQL API over plain HTTP (`httpx`), so it
stays small and cold-starts quickly. Core dependencies: `httpx`, `PyJWT`,
`cryptography`.

## Features

- Synchronous and asynchronous clients with the same surface
- Keypair (JWT) authentication, including encrypted private keys
- Result type coercion (NUMBER/Decimal, dates/timestamps, VARIANT, and more)
- Multi-partition result handling for large result sets
- Lazy row streams and bounded row batches, with CSV and JSONL exports
- Query helpers, DML, batch insert, async statement submission
- Caller-owned submission UUIDs and explicit replay across worker restarts
- A small CLI for ad-hoc queries
- Optional pandas (`[pandas]`) and typed-row (`[pydantic]`) helpers, kept out of
  the default install

## Install

```bash
pip install snowflake-sql-api
# optional extras
pip install "snowflake-sql-api[pandas]"
pip install "snowflake-sql-api[pydantic]"
```

## Quick start

```python
from snowflake_sql_api import SnowflakeClient

client = SnowflakeClient(
    account="myorg-myaccount",
    user="MY_USER",
    private_key_path="/path/to/rsa_key.p8",
)

rows = client.query("SELECT id, name FROM users WHERE active = ?", [True])
for row in rows:
    print(row["ID"], row["NAME"])
```

Async:

```python
from snowflake_sql_api import AsyncSnowflakeClient

async with AsyncSnowflakeClient.from_env() as client:
    rows = await client.query("SELECT current_timestamp()")
```

CLI:

```bash
snowflake-sql-api query "SELECT current_version()"
```

Stream large results or export them without collecting every row:

```python
with SnowflakeClient.from_env() as client:
    for batch in client.query_batches("SELECT id, amount FROM sales", batch_size=500):
        process(batch)

    with open("sales.csv", "w", encoding="utf-8", newline="") as output:
        count = client.export_csv("SELECT id, amount FROM sales", output)
```

Streaming fetches completed results one server partition at a time. Memory scales
with the largest decoded partition plus the application batch. See
[streaming results and exports](docs/streaming.md) for row streams, async usage,
serialization, and partial-output behavior.

Insert a record stream in bounded requests:

```python
with SnowflakeClient.from_env() as client:
    progress = client.insert_batches(
        "events", ["id", "label"], ((i, f"event {i}") for i in range(2500)),
        max_rows=1000, max_bytes=1_000_000,
    )
    print(progress.input_rows, progress.rows_affected)
```

`insert_many()` sends one statement; `insert_batches()` sends sequential statements
and can partially complete. Its byte budget covers the exact JSON body. On failure,
`BatchWriteError` reports confirmed progress, consumed input and the failed request's
identity. Reconstruct replayable input and reconcile unknown outcomes before retrying;
do not resume with the same advanced iterator. The async equivalent is awaited and
accepts the same synchronous iterable, whose production can block the event loop.
See [batch writes](docs/batch-writes.md) for limits and recovery semantics.

## Replayable submissions

For a queued job, allocate a UUID and persist it with the SQL, typed inputs and
explicit execution context **before** calling `submit()`. Pass that saved ID:

```python
handle = client.submit(job["sql"], job["params"], request_id=job["request_id"])
# Persist handle.statement_handle when it reaches the worker.

# After a restart with no saved statement handle, rebuild identical inputs:
handle = client.resubmit(job["sql"], job["params"], request_id=job["request_id"])
rows = handle.result()  # Only then mark the durable job complete.
```

`resubmit()` sends the same ID with `retry=true` on its first HTTP attempt.
A saved statement handle can instead be attached with `QueryHandle(client, saved_handle)`
and polled without another submission. Both clients expose `handle.request_id`, and
submission errors in the `SnowflakeError` family carry the allocated ID. Async
submission, resubmission and result methods are awaited.

The application owns durable storage, identical payload reconstruction, UUID
uniqueness and reconciliation. Retained Snowflake history governs recovery; there
is no unlimited replay window or exactly-once processing guarantee. Live restart
verification requires the opt-in disposable-resource probes. See
[replayable submissions](docs/submissions.md) for the complete persistence recipe,
limits and verification status.

## Retries

Both clients accept `retry_policy=RetryPolicy(...)` (import `RetryPolicy` from
`snowflake_sql_api`). The frozen policy keeps three total attempts by default.
Without a usable `Retry-After` header, HTTP 429 uses full jitter with delay
ceilings of 5 and 8 seconds; other retryable failures use full jitter with
ceilings of 0.5 and 1 second.
`rate_limit_backoff_multiplier=10.0` scales the normal base for 429 before the
existing `max_backoff=8.0` cap. Set it to `1.0` for the ordinary schedule, or
`base_backoff=0` to disable scheduled backoff.

On retryable responses, `Retry-After` delay-seconds or an HTTP date overrides
that schedule without jitter, capped by `max_retry_after=60.0` seconds per wait.
Invalid values fall back to the schedule; past dates give zero delay.
`max_retry_after=0` disables header-directed waits. Tests that must never wait should set both
`base_backoff=0` and `max_retry_after=0`. Date parsing accepts the
standard and obsolete HTTP date forms and treats a date without a zone as UTC.
The policy's `delay_for(attempt, status_code=..., retry_after=...)` applies these
response rules; `backoff_for(attempt)` keeps its ordinary schedule and signature.

With defaults, a request can wait up to 13 seconds across retries of 429s without
a header, or up to 120 seconds with capped headers, in addition to HTTP timeouts.
These are per transport request: status and partition GETs have their own retry
loops. This policy does not impose an overall query deadline. See
[troubleshooting retries](docs/troubleshooting.md#transient-failures-and-retries)
for retry errors and submission recovery.

## Prior art and alternatives

`snowflake-sql-api` is not the first REST-based Snowflake client. If it does not
fit your needs, one of these might:

| Project | Language | Notes |
|---|---|---|
| [`snowflake-connector-python`](https://github.com/snowflakedb/snowflake-connector-python) | Python | Official driver. Full-featured (DB-API, pandas/Arrow, every auth mode) but large and slow to cold-start. Use it when you need the full driver. |
| [`snowflake-rest`](https://github.com/pps-19012/snowflake-rest) | Python | The closest prior art and a direct inspiration for this project's API. Pure-Python over `requests`, sync-only, currently low-activity. |
| [`snowflake-sql-api-async`](https://github.com/neonblue-ai/snowflake-sql-api-async) | Python | Async-focused, but depends on `snowflake-connector-python`. |
| [`snowflake-sql-api-client`](https://pypi.org/project/snowflake-sql-api-client/) | Python | Low-level generated wrapper; unmaintained since 2022. |
| [`rb_snowflake_client`](https://github.com/rinsed-org/rb-snowflake-client) | Ruby | Mature pure-Ruby SQL API client (streaming, connection pooling). |
| [`rsql`](https://github.com/theseus-rs/rsql) | Rust | Multi-database SQL CLI with a Snowflake SQL API driver. |

**How `snowflake-sql-api` differs:** pure Python over `httpx`, with no
dependency on the official connector (small install, fast cold start), paired
sync and async clients, keypair auth, type coercion, partition handling, and a
CLI as tested first-class behavior.

Note: the similarly named `snowflake-sql-api-async` and `snowflake-sql-api-client`
are separate, unaffiliated projects.

## Acknowledgements

The API design is inspired by [`pps-19012/snowflake-rest`](https://github.com/pps-19012/snowflake-rest)
by Pushpendra Singh. This is a clean-room implementation (no code copied); credit
for the original design and API shape goes to its author.

## License

MIT
