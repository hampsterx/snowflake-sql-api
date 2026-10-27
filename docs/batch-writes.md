# Batch writes

`insert_batches()` accepts an ordinary iterable of positional records and sends
sequential, bound `INSERT ... VALUES` statements. Choose it for record streams
or inputs that need bounded request sizes. `insert_many()` accepts a sequence
and sends one statement; it does not chunk automatically.

```python
from snowflake_sql_api import BatchWriteError, SnowflakeClient

# The application owns this source and any durable checkpoint.
records = ((i, f"record {i}") for i in range(2500))
with SnowflakeClient.from_env() as client:
    try:
        progress = client.insert_batches(
            "events", ["id", "label"], records,
            max_rows=1000, max_bytes=1_000_000,
        )
    except BatchWriteError as exc:
        print(exc.completed.input_rows, exc.next_row_index, exc.outcome)
        # Stop and reconcile the failed submission before deciding what to replay.
        raise
    print(progress.completed_batches, progress.input_rows, progress.rows_affected)
```

The async client exposes the same method, awaited for the final result:

```python
from snowflake_sql_api import AsyncSnowflakeClient

async with AsyncSnowflakeClient.from_env() as client:
    progress = await client.insert_batches("events", ["id"], ((i,) for i in range(2500)))
```

The input remains a synchronous iterable. Its production runs on the calling
thread, so a slow iterator can block the async event loop. `AsyncIterable`
input and background input production are not supported.

## Limits and consumption

`max_rows` defaults to 1000 and must be a positive integer no larger than 200,000,
the [Snowflake VALUES row limit](https://docs.snowflake.com/en/sql-reference/sql/insert#usage-notes).
`max_bytes` defaults to 1,000,000 and must be a positive integer. Booleans are not
accepted as limits. Columns must be nonempty; identifiers are quoted and values
are bound, as in `insert_many()`.

The byte budget counts the exact UTF-8 JSON request body, including SQL,
placeholders, binding keys and encoded values, context, timeout and statement
parameters. It excludes the URL and HTTP headers. The batch encoder uses compact
JSON with Unicode preserved; the transport sends those exact bytes on every
attempt. Other query methods keep their existing JSON encoding path.

This budget is a client setting, not a universal Snowflake HTTP body limit.
Snowflake's [query-size guidance](https://docs.snowflake.com/en/user-guide/query-size-limits)
recommends 1 MB because larger query text can be truncated in persisted metadata,
which affects rerun and retry. It also covers bindings and recommends keeping the
uncompressed size within the limit.

Input is consumed once, without requiring a length or retaining the whole source.
At a row-count boundary, the batch completes before the next record is requested.
At a byte boundary, one validated lookahead record can be retained for the next
batch. A row that cannot fit alone fails locally before any current buffer is
sent. Iterator and record-validation failures also discard the current unsent
buffer. Empty input returns an empty result without HTTP or `on_query` calls.

Memory scales with one bounded batch and one pending record, plus one summary per
completed batch. A single oversized value still needs inspection before rejection;
these limits are not a process-memory guarantee. Each actual INSERT calls
`on_query(sql, params)` once; transport retries do not call it again.

## Confirmed progress

`BatchWriteResult` is immutable and contains an ordered `batches` tuple of
`BatchSummary` objects. Each summary has:

- `index`: the zero-based batch number.
- `start_row_index`: the zero-based source offset at which this batch starts.
- `row_count`: the number of input records in this confirmed batch.
- `rows_affected`: the count reported by Snowflake.

The result's `input_rows`, `rows_affected` and `completed_batches` properties give
the corresponding totals. Summaries retain no row values or request bodies.
Server-reported affected rows are distinct from input rows.

## Partial completion and failures

Each batch is a separate statement. There is no whole-import transaction: earlier
statements can commit before a later failure. The next batch is neither filled
nor submitted until the current one finishes, including HTTP 202 polling.

Invalid columns or limits raise `SnowflakeConfigError` before input iteration.
Once iteration starts, ordinary input, record, payload and write failures raise
`BatchWriteError`, a `SnowflakeError`, with the original exception as `__cause__`.
Structured server details remain on that cause. The error exposes:

- `completed`: the same confirmed progress structure as a successful result.
- `next_row_index`: the first source offset whose write is unconfirmed.
- `consumed_rows`: records already pulled, including an offending yielded record
  and any pending lookahead. An iterator exception adds no record.
- `failed_batch`: a `FailedBatch` with `index`, `start_row_index`, `row_count`,
  generated `request_id`, optional `statement_handle` and `submission_attempts`.
  It is absent on local input or validation failure before submission is ready.
- `phase`: `input`, `validation`, `submission` or `execution`.
- `outcome`: `not_executed`, `failed`, `unknown` or `succeeded`.

A first-attempt HTTP 400, 401 or 403 rejection has outcome `not_executed` in the
submission phase. SQL HTTP 422 has outcome `failed` in the execution phase when
there is only one parent POST attempt. A valid 202 with a handle starts the
execution phase; later polling failures retain that context. Exhausted retries,
poll timeouts and unrecognized remote errors have outcome `unknown`. Any remote
error after multiple parent POST attempts is also `unknown`, even when the final
response is 400, 401, 403 or 422. A later rejection cannot rule out an earlier
attempt having executed. A successful response followed by an affected-count
parsing failure has outcome `succeeded`, but is excluded from confirmed summaries.
The batch API rejects missing, malformed, negative or non-integer affected counts;
existing `execute()` and `insert_many()` count parsing keeps its behavior.

`submission_attempts` is also optional metadata on library errors and transport
`StatementResponse` objects. It counts actual statement POST attempts, including
the first, and is `None` for GETs; batch errors retain the parent POST count
separately while polling. The transport also attaches this count to other ordinary
exceptions it propagates, preserving their original type and identity. Local
authentication failure before any POST is `not_executed`; if a prior POST loses
its response and local authentication prevents retrying, the outcome is `unknown`.
HTTP authentication errors retain their status code and server envelope so they
remain distinguishable from local token-generation errors.

For statement submissions, `StatementResponse.statement_handle` and a propagated
exception's `statement_handle` retain the latest nonempty handle from an earlier
POST attempt when the final response or error omits one. A handle in the final
response or error takes precedence. This also applies to the transport used by
`execute()`, `query()` and `submit()`. Response bodies stay unchanged, and handles
do not carry between independent operations.

## Recovery belongs to the application

`next_row_index` is a progress boundary, not permission to skip an uncertain
batch. Do not resume using the same advanced iterator: reconstruct replayable
input from that boundary and reconcile any unknown submission using its request
ID and known statement handle before sending again. Failed SQL does not imply
that the whole import is resumable. This method does not replay a completed prefix,
create a new identity to retry an uncertain batch, or cancel a running statement
automatically.

Cancellation and keyboard interruption propagate unchanged. Cancellation can
leave the current submission uncertain, without a `BatchWriteError` checkpoint.
Durable checkpoints, cancellation recovery, parallel writes, whole-import
transactions, staging files, `COPY INTO` and upserts are outside this API.
