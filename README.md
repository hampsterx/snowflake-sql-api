# snowflake-sql-api

A lightweight, pure-Python client for [Snowflake's SQL API v2](https://docs.snowflake.com/en/developer-guide/sql-api/index)
(the `POST /api/v2/statements` REST endpoint).

> **Status: in active development, not yet released.** The API may change before `v0.1.0`.

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

**How `snowflake-sql-api` differs:** pure Python over `httpx` with no dependency on
the official connector (small install, fast cold start), paired sync and async
clients, and keypair auth, type coercion, partition handling, and a CLI as tested
first-class behavior.

Note: the similarly named `snowflake-sql-api-async` and `snowflake-sql-api-client`
are separate, unaffiliated projects.

## Acknowledgements

The API design is inspired by [`pps-19012/snowflake-rest`](https://github.com/pps-19012/snowflake-rest)
by Pushpendra Singh. This is a clean-room implementation (no code copied); credit
for the original design and API shape goes to its author.

## License

MIT
