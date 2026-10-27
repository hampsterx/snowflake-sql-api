# Getting Started

`snowflake-sql-api` is a small, pure-Python client for Snowflake's
[SQL API v2](https://docs.snowflake.com/en/developer-guide/sql-api/index). This
page gets you from install to a first query.

## Install

```bash
pip install snowflake-sql-api
```

Optional extras (kept out of the default install to stay small):

```bash
pip install "snowflake-sql-api[pandas]"    # DataFrame output helpers
pip install "snowflake-sql-api[pydantic]"  # typed-row mapping
```

Requires Python 3.11 or newer. Core dependencies are `httpx`, `PyJWT`, and
`cryptography`.

## Prerequisites

You need keypair (JWT) authentication set up: an RSA key pair, with the public
key registered on your Snowflake user. See [authentication.md](authentication.md)
for the full walkthrough. The short version:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```

Then register the public key on the user (run as a role that can alter the user):

```sql
ALTER USER my_user SET RSA_PUBLIC_KEY='<contents of rsa_key.pub, header/footer stripped>';
```

## Your first query

```python
from snowflake_sql_api import SnowflakeClient

client = SnowflakeClient(
    account="myorg-myaccount",        # or a region locator like "xy12345.ap-southeast-2"
    user="MY_USER",
    private_key_path="/path/to/rsa_key.p8",
    warehouse="MY_WH",                # optional session context
    database="MY_DB",
    schema="PUBLIC",
)

rows = client.query("SELECT id, name FROM users WHERE active = ?", [True])
for row in rows:
    print(row["ID"], row["NAME"])

client.close()
```

`query` returns a list of dicts keyed by column name, with values coerced to
native Python types (numbers to `int`/`Decimal`, dates/timestamps to
`datetime`/`date`/`time`, VARIANT to `dict`/`list`, and so on).

Use it as a context manager to close the underlying HTTP client automatically:

```python
with SnowflakeClient(account="myorg-myaccount", user="MY_USER",
                     private_key_path="/path/to/rsa_key.p8") as client:
    version = client.query_scalar("SELECT current_version()")
```

## Query helpers

| Method | Returns |
|--------|---------|
| `query(sql, params)` | all rows (list of dicts) |
| `query_one(sql, params)` | first row, or `None` |
| `query_scalar(sql, params)` | first column of the first row, or `None` |
| `query_column(sql, params)` | first column across all rows |
| `execute(sql, params)` | rows affected (DML/DDL) |
| `insert_many(table, columns, rows)` | rows inserted by one bound multi-row statement |
| `insert_batches(table, columns, rows, max_rows=1000, max_bytes=1_000_000)` | confirmed progress across bounded INSERT statements |
| `submit(sql, params)` | a `QueryHandle` for a long-running statement |
| `query_stream(sql, params)` | a lazy generator of converted rows |
| `query_batches(sql, params, batch_size=1000)` | lazy batches of at most the requested row count |
| `export_csv(sql, output, params)` | data-row count written to a caller-owned text sink, with a CSV header |
| `export_jsonl(sql, output, params)` | row count written as one JSON object per line |

Bind parameters are positional (`?`) and always sent as server-side bindings,
never string-interpolated.

Streams submit on first iteration and fetch later result partitions on demand.
Use `async for` directly with the async streaming methods and await async export
methods. See [streaming.md](streaming.md) for memory limits, file examples, and
failure behavior.

`insert_batches()` consumes an ordinary iterable once and submits bounded,
sequential INSERT statements. Earlier batches can commit before a later failure;
`BatchWriteError` carries confirmed progress and uncertain outcomes. Use
`insert_many()` when you want one statement. See [batch-writes.md](batch-writes.md)
for byte budgets, async input and recovery.

## Configuration from the environment

`from_env()` reads `SNOWFLAKE_*` variables, which keeps credentials out of code:

```python
client = SnowflakeClient.from_env()
```

| Variable | Purpose |
|----------|---------|
| `SNOWFLAKE_ACCOUNT` | account locator (required) |
| `SNOWFLAKE_USER` | user name (required) |
| `SNOWFLAKE_PRIVATE_KEY` | PEM key contents, or |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | path to a PEM/DER key file |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | passphrase for an encrypted key |
| `SNOWFLAKE_ROLE` / `SNOWFLAKE_WAREHOUSE` | session role / warehouse |
| `SNOWFLAKE_DATABASE` / `SNOWFLAKE_SCHEMA` | session database / schema |
| `SNOWFLAKE_HOST` | override the derived API hostname (PrivateLink, etc.) |

## Async

The async client mirrors the sync surface with `await` and an async context
manager:

```python
from snowflake_sql_api import AsyncSnowflakeClient

async def main():
    async with AsyncSnowflakeClient.from_env() as client:
        rows = await client.query("SELECT current_timestamp()")
        print(rows)
```

## Next steps

- [streaming.md](streaming.md): process large results, use bounded batches, and
  export CSV or JSONL.

- [authentication.md](authentication.md): keypair setup, encrypted keys, the
  account-locator region gotcha.
- [cli.md](cli.md): the `snowflake-sql-api` command.
- [testing.md](testing.md): mock the client in your own tests, no Snowflake
  account required.
- [troubleshooting.md](troubleshooting.md): auth failures, polling, partitions.
