# CLI

Installing the package puts a `snowflake-sql-api` command on your PATH for
ad-hoc queries.

## Configuration

The CLI reads connection settings from the environment (the same `SNOWFLAKE_*`
variables as `SnowflakeClient.from_env()`):

```bash
export SNOWFLAKE_ACCOUNT="myorg-myaccount"
export SNOWFLAKE_USER="MY_USER"
export SNOWFLAKE_PRIVATE_KEY_PATH="/path/to/rsa_key.p8"
# optional: SNOWFLAKE_ROLE / SNOWFLAKE_WAREHOUSE / SNOWFLAKE_DATABASE / SNOWFLAKE_SCHEMA
```

See [getting-started.md](getting-started.md) for the full variable list and
[authentication.md](authentication.md) for key setup.

## Running a query

```bash
snowflake-sql-api query "SELECT current_version()"
```

Output is JSON (a list of row objects), with type-coerced values rendered
JSON-safely: `Decimal` as a string, dates/timestamps as ISO 8601, binary as hex.

```bash
$ snowflake-sql-api query "SELECT 1 AS n, 'hi' AS greeting"
[
  {
    "N": 1,
    "GREETING": "hi"
  }
]
```

## Version

```bash
snowflake-sql-api --version
```

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | query ran, result printed |
| `1` | a `SnowflakeError` occurred (message on stderr) |
| `2` | no subcommand given (help printed) |

## Scope

This is the minimal `query` command (JSON output). Richer output formats
(`--format table|csv|json|jsonl`), reading SQL from a file, and a progress spinner
are planned for a later release.
