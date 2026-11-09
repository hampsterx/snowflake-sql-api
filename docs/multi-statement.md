# Multi-statement requests and explicit transactions

Both clients provide `execute_statements(statements)`,
`submit_statements(statements)`, and `execute_transaction(statements)`. Async
clients await these methods and their handle operations. A usable warehouse is
required by [Snowflake's multi-statement API](https://docs.snowflake.com/en/developer-guide/sql-api/submitting-multiple-statements).

## One request, distinct results

```python
from snowflake_sql_api import SnowflakeClient

with SnowflakeClient.from_env() as client:
    result = client.execute_statements([
        "CREATE TEMPORARY TABLE report_values (n INTEGER)",
        "INSERT INTO report_values VALUES (1), (2)",
        "SELECT n FROM report_values ORDER BY n",
    ])
    print(result.results[2].rows)  # [{"N": 1}, {"N": 2}]
```

Session variables, context changes, and temporary objects share the context of
this request. Separate API requests do not provide a persistent session.

Supply a nonempty sequence of trusted, program-authored SQL strings, each
representing exactly one statement without its top-level terminating semicolon.
The client trims each entry and joins with `"\n;\n"`, preserving delimiters after
line comments. It does not split SQL or count semicolons inside literals,
comments, or scripting bodies. Snowflake validates the exact statement count.

The request sets `MULTI_STATEMENT_COUNT` to the number of entries, overriding
constructor count parameters regardless of case without changing that
configuration. An explicit constructor `AUTOCOMMIT` parameter is rejected for
these APIs: the SQL API's published request-parameter allowlist does not list it.
The SQL API requires autocommit enabled; do not disable it to simulate a
cross-request transaction.

There is no `params` argument or bindings field. Do not interpolate untrusted
values to work around the SQL API's missing multi-statement binding support.
Use a parameterized single statement for dynamic values:

```python
rows = client.query(
    "WITH x AS (SELECT ? AS label) SELECT label FROM x", [label]
)
```

For multiple dynamic steps, a caller-managed, scalar-returning procedure can
provide a single bound call, for example
`client.query_scalar("CALL process_event(?, ?)", [event_id, label])`.
The procedure owns its internal binding, transaction and error handling; confirm
its behavior in the target environment. The client does not translate batches
into procedures. See [SQL API procedure support](https://docs.snowflake.com/en/developer-guide/sql-api/using-stored-procedures).
For a single multi-row INSERT, the existing `insert_many()` also binds values.

`MultiStatementResult.statement_handle` identifies the parent request. Its
`results` tuple contains `StatementResult` objects in input order, with
zero-based `index`, `statement_handle`, `columns`, and coerced `rows`. Columns and
rows stay separate between statements. Each child's partitions are fetched in
order. DML returns its own result rows; the client does not infer affected counts
from arbitrary numeric query results. All child results are collected eagerly.
The tuple and result fields are frozen; row dictionaries and column metadata
retain the same mutable Python types used by single-statement queries.

A one-entry request uses the ordinary single-result envelope when Snowflake
returns one. The parent is also that result's handle, and every partition is
still fetched. Existing single-statement methods keep their current behavior.

## Handles and recovering results

```python
from snowflake_sql_api import MultiStatementHandle

handle = client.submit_statements(["SELECT 1 AS n", "SELECT 2 AS n"])
# Persist these fields in application-owned durable storage:
parent, count = handle.statement_handle, handle.expected_count
# The original POST UUID is available for diagnostic correlation:
request_id = handle.request_id

# A later worker can attach without knowing or submitting the SQL:
restored = MultiStatementHandle(client, parent, expected_count=count)
result = restored.result(timeout=60)
```

Async attachment uses `AsyncMultiStatementHandle`. Construction performs no HTTP
and fires no query hook. Retrieval uses only GETs, including after a failed child
download. `status()` checks the parent and returns `RUNNING` or `SUCCESS`;
`cancel()` requests parent cancellation. Cancellation cannot undo committed
statements or exclude a race with later statements.

An attached handle has `request_id=None`, because it does not know the original
submission identity. A UUID alone cannot retrieve a result; retain the parent
handle and count for recovery.

`result(poll=False)` raises `ResultNotReady` with the relevant parent or child
handle on HTTP 202. `result(timeout=...)` shares one polling budget across the
parent and all children. `ResultNotReady` carries no partial-result prefix;
call `result()` again to collect the complete ordered results. The polling
budget's default is 600 seconds. HTTP calls and their retries
retain their own transport timeouts, so this is not a hard wall-clock limit.
A caller-owned handle is left running on a polling timeout. Synchronous-style
`execute_statements()` and `execute_transaction()` ask to cancel a parent they
own when its polling deadline expires, following the existing client behavior.
On the async client, task cancellation during that parent poll also triggers a
bounded best-effort cancel. Cancellation during submission cannot cancel an
unreceived handle. No cancel operation promises rollback.

## Errors and retries

`SnowflakeMultiStatementError` preserves the original exception as `__cause__`
and exposes structured `status_code`, `code`, and `sql_state` when available:

| Field | Meaning |
| --- | --- |
| `statement_handle` | The known parent, or `None` if no handle reaches the client. |
| `request_id` | The original parent POST UUID, or `None` for an attached handle. |
| `expected_count` | The submitted count needed for later handle attachment. |
| `known_statement_handles` | A tuple of validated child handles reported by Snowflake. |
| `completed_results` | The ordered results actually downloaded before the fault. |
| `failed_index` | The child download index, or `None` when the position is unknown. |
| `phase` | `submission`, `execution`, or `result_fetch`. |
| `outcome` | `not_executed`, `failed`, `unknown`, or `succeeded`. |
| `submission_attempts` | Actual parent POST attempts, or `None` when attaching an existing handle. |

A single-attempt 400/401/403 submission rejection is `not_executed`. A parent 422
is `failed` only when one POST attempt is known. Exhausted retries, polling
timeouts, unfamiliar remote faults, or any failure before parent confirmation
after multiple POST attempts are `unknown`. A final rejection cannot erase the
uncertainty of an earlier timeout. Attaching an existing handle does not invent
its attempt history.

An allocated UUID can also accompany a local signing failure with zero POST
attempts. There is then no accepted request to look up at Snowflake.

After validated parent success, a child download error is `result_fetch` with
`succeeded` execution, even if a previous POST attempt was ambiguous. That
observation does not establish exactly-once effects. Recover results using the
saved parent and count; do not submit the batch again because a GET failed.
Partial child lists in an error establish neither positional correspondence nor
committed progress. The library does not parse an error message to infer an index.
`ResultNotReady` keeps its existing exception type. Parent status/cancel methods
also keep the transport's existing exception types.

Ordinary batches can commit a prefix before failing. The default transport retry
policy reuses the parent request ID and sends `retry=true` on subsequent
attempts, but that does not make a failed batch atomic or exactly once. Replaying
a partially failed batch can repeat earlier effects. For non-idempotent plain
batches that need application reconciliation, construct a client explicitly:

```python
from snowflake_sql_api import RetryPolicy, SnowflakeClient

with SnowflakeClient(
    account="myorg-myaccount", user="MY_USER",
    private_key_path="/path/to/rsa_key.p8",
    warehouse="MY_WAREHOUSE",
    retry_policy=RetryPolicy(max_attempts=1),
) as client:
    result = client.execute_statements([
        "INSERT INTO events SELECT * FROM staged_events",
        "SELECT COUNT(*) AS n FROM events",
    ])
```

That policy also disables automatic GET retries. Neither result retrieval nor
GET retries can cause a parent POST. Handles and wrapped errors retain the
generated parent POST UUID, including when a response is lost. This is diagnostic
metadata, not evidence of acceptance or commit. Status/cancel errors and
`ResultNotReady` retain their existing metadata. The new APIs do not accept
caller-owned submission UUIDs or provide durable batch replay. Persist the
parent handle as soon as it is available; an ID returned after submission cannot
cover every process or task loss.

## Constrained transaction helper

```python
result = client.execute_transaction([
    "UPDATE balances SET amount = amount - 10 WHERE id = 1",
    "UPDATE balances SET amount = amount + 10 WHERE id = 2",
])
# Result 0 sets abort-on-error; 1 and 4 describe BEGIN and COMMIT.
# Body results are at 2 and 3.
```

The helper constructs `ALTER SESSION SET TRANSACTION_ABORT_ON_ERROR=TRUE`,
`BEGIN TRANSACTION`, the body, and `COMMIT` in one request with an exact count
of `len(body) + 3`, as required by
[Snowflake's explicit transaction protocol](https://docs.snowflake.com/en/developer-guide/sql-api/using-transactions).
The setup enables server-side abort on statement errors without changing account
or user settings. It is SQL because `TRANSACTION_ABORT_ON_ERROR` is absent from
the [SQL API request-parameter allowlist](https://docs.snowflake.com/en/developer-guide/sql-api/reference#statements_parameters).
All setup, boundary and body results remain in their submitted order. An empty
body is rejected locally.

The helper sends no separate rollback request: the SQL API requires transaction
boundaries in one request. A failed statement prevents later statements,
including COMMIT, from executing. Snowflake documents that an aborted transaction
still requires COMMIT or ROLLBACK to close it; enabling abort-on-error alone does
not establish immediate lock release. The live rollback and independent lock
probe below remain required before relying on this helper's failure cleanup.

Only leading INSERT, UPDATE, DELETE, MERGE, and SELECT forms are accepted, after
ASCII whitespace, `--` line comments, and non-nested `/* ... */` comments.
Comment-only entries, nested or unterminated leading block comments, leading
`//`, WITH, DDL, CALL, EXECUTE IMMEDIATE, session changes, and transaction control
statements are rejected before HTTP or notification. The rest of each SQL body
is preserved. This prefix check defines a restricted API; it is not a SQL parser
or a security boundary for hostile input. Each entry must still be one trusted
statement. External effects of functions are outside the helper's scope. DDL
implicitly commits and is unsuitable for transaction rollback guarantees.

For trusted broader syntax, write the transaction boundaries explicitly using
the existing raw batch API:

```python
result = client.execute_statements([
    "ALTER SESSION SET TRANSACTION_ABORT_ON_ERROR=TRUE",
    "BEGIN TRANSACTION",
    "WITH x AS (SELECT 1 AS n) SELECT n FROM x",
    "COMMIT",
])
```

This has the same single-request and uncertain-outcome rules. The caller owns
the SQL's transaction effects. A leading WITH is excluded from the constrained
helper because [Snowflake also permits anonymous procedure calls after WITH](https://docs.snowflake.com/en/sql-reference/constructs/with).
An `INSERT INTO ... WITH ... SELECT ...` already starts with an allowed keyword.

**Verification limit:** unit tests establish request construction and result/error
handling. Live rollback, lock release, autocommit configuration, completed-parent
replay, and partially failed replay require empirical verification in a
disposable Snowflake namespace. They are not established by mocked HTTP 422
responses. An unknown outcome never means a rollback. Run the opt-in probes:

```bash
smoke_evidence_dir="$(mktemp -d "$HOME/snowflake-smoke.XXXXXXXX")"
SNOWFLAKE_LIVE_DML=1 python -m pytest tests/smoke/test_multi_statement.py -v \
    -o junit_family=legacy --junitxml="$smoke_evidence_dir/results.xml"
```

The probes additionally require credentials plus explicit `SNOWFLAKE_DATABASE`,
`SNOWFLAKE_SCHEMA`, and `SNOWFLAKE_WAREHOUSE`, create uniquely named disposable
objects, and use synthetic rows. They observe successful commits, failed-body
rollback and subsequent lock availability from independent requests. The rollback
probe requires the intended divide-by-zero failure and query-history evidence
that both preceding writes succeeded within the same transaction. Missing history
fails the probe, including when the last 10,000 queries in its 15-minute search
window omit the needed statements. It records the observed AUTOCOMMIT setting
and requires it to be TRUE without changing it, and records the abort-on-error
default from a separate request. That default does not prove the helper's
request-local setting. They also
discard a real successful response before retrying a completed parent, and
record prefix-row counts when replaying a partially failed ordinary batch.
A replay returning HTTP 200 or 202 without a valid handle must raise a request
error, preserving the request ID and an unknown outcome. It must not become an
empty success or an invented repeat of the original 422. A handle from an earlier
attempt remains diagnostic information for GET-based recovery, not evidence that
the handleless response completed the batch. No replacement UUID is submitted.
Skipped probes leave these properties unverified. Keep account configuration and
probe evidence private; do not publish credentials or real account identifiers.

The [query-size limit](https://docs.snowflake.com/en/user-guide/query-size-limits)
applies to the whole joined SQL string. This feature does not split requests,
stream multiple results, or provide cross-request transactions, savepoints,
nested transactions, arbitrary procedure guarantees, or bound batches.
