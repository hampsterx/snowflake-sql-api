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
