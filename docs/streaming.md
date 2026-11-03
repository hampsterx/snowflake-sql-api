# Streaming results and exports

Use streaming when a query result is too large to collect into a Python list.
The synchronous and asynchronous clients provide the same four methods:

| Method | Result |
|--------|--------|
| `query_stream(sql, params=None)` | A generator of converted row dictionaries. |
| `query_batches(sql, params=None, *, batch_size=1000)` | A generator of lists containing at most `batch_size` rows. |
| `export_csv(sql, output, params=None)` | Write a header and rows to a text sink; return the data-row count. |
| `export_jsonl(sql, output, params=None)` | Write one JSON object per line to a text sink; return the row count. |

## What streaming means

The SQL API makes results available after statement execution finishes. The
first completed response contains column metadata, partition metadata, and the
first partition of rows. Later partitions are fetched individually as consumption
advances. Their compressed responses are decoded by the existing HTTP transport.
See Snowflake's [response handling](https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses)
and [SQL API reference](https://docs.snowflake.com/en/developer-guide/sql-api/reference).

Memory scales with the largest decoded server partition plus the application
batch or serialized row. Snowflake chooses partition sizes; `batch_size` limits
the number of converted rows in each application batch, not the size of an HTTP
response. Rows or output retained by the caller add to that working set.

Each stream submits its SQL only when iteration starts, then polls using the
client's existing timeout configuration. Creating an unused stream performs no
I/O. Submission uses the same parameter bindings, retries, and `on_query` hook as
`query()`. No later partition is fetched until the current one is consumed.
Partition order is preserved; use SQL `ORDER BY` when business ordering matters.

## Process rows or batches

```python
from snowflake_sql_api import SnowflakeClient

with SnowflakeClient.from_env() as client:
    for row in client.query_stream(
        "SELECT id, amount FROM sales WHERE sale_date >= ? ORDER BY id",
        [start_date],
    ):
        process(row)

    for batch in client.query_batches(
        "SELECT id, amount FROM sales ORDER BY id", batch_size=500
    ):
        send_to_partner(batch)
```

Batches can span server partition boundaries. The final batch can be smaller;
an empty result produces no batches. A non-positive or non-integer batch size
raises `SnowflakeConfigError` when iteration starts.

Keep the client open until consumption ends. If you retain a stream and stop
early, close it explicitly to release its current partition:

```python
from contextlib import closing

with SnowflakeClient.from_env() as client:
    with closing(client.query_stream("SELECT id FROM sales")) as rows:
        for row in rows:
            if enough_rows(row):
                break
```

Closing stops further fetching. It does not close the client or cancel the
completed query. Existing `query()` and `QueryHandle.result()` still collect
and return all rows.

## Write CSV or JSONL

The destination is a caller-owned writable text stream, such as an open file or
`io.StringIO`. Open files with UTF-8 encoding and `newline=""`:

```python
with SnowflakeClient.from_env() as client:
    with open("sales.csv", "w", encoding="utf-8", newline="") as output:
        count = client.export_csv(
            "SELECT id, amount FROM sales WHERE sale_date >= ?",
            output,
            [start_date],
        )

    with open("sales.jsonl", "w", encoding="utf-8", newline="") as output:
        count = client.export_jsonl("SELECT id, amount FROM sales", output)
```

CSV uses column metadata for the header, including when the query returns zero
rows. A result with zero columns and zero rows writes an empty header line.
Empty JSONL results produce no output. Both methods return the number of data
rows written and leave the destination open.

All query helpers, streams, exports, and `QueryHandle.result()` require unique
column names, including for empty results. Use distinct SQL aliases when
selecting columns with the same name or repeated expressions such as
`SELECT 1, 1`. Duplicates raise `SnowflakeConfigError` naming the repeated names
before any row is returned or export output is written. Streams and batches
raise when iteration starts.

| Python value | CSV representation | JSONL representation |
|--------------|--------------------|----------------------|
| `Decimal` | Exact decimal text. | String containing exact decimal text. |
| Dates, times, timestamps | ISO 8601 text. | ISO 8601 string. |
| Binary | Hexadecimal text. | Hexadecimal string. |
| Dictionaries and lists | JSON within a quoted CSV cell when required. | Nested JSON values. |
| Boolean | `true` or `false`. | JSON boolean. |
| `None` | Empty field, indistinguishable from an empty string. | JSON `null`. |

CSV quotes commas, quotes, and embedded newlines. JSONL escapes newlines within
strings so each record occupies one physical line. Unicode is retained. JSONL
rejects non-finite floats, including nested ones, with `ValueError`.
Both formats serialize the client's converted Python values, including its
microsecond precision for temporal values.

## Async usage

Use `async for` directly on the streaming methods; do not await their creation.
Await the export methods:

```python
from contextlib import aclosing
from snowflake_sql_api import AsyncSnowflakeClient

async with AsyncSnowflakeClient.from_env() as client:
    async with aclosing(client.query_stream("SELECT id FROM sales")) as rows:
        async for row in rows:
            await process(row)

    async for batch in client.query_batches("SELECT id FROM sales", batch_size=500):
        await send_to_partner(batch)

    with open("sales.jsonl", "w", encoding="utf-8", newline="") as output:
        count = await client.export_jsonl("SELECT id FROM sales", output)
```

Async exports await database I/O and write synchronously to the text sink.
Async file-writer interfaces are not accepted. Use `aclose()` or `aclosing` for
a retained async stream when stopping early.

## Failures

Query, polling, partition-fetch, conversion, serialization, and sink errors
propagate to the caller. Missing or malformed partition data raises
`SnowflakeRequestError` on the streaming path. On every result path, each row
must be an array with exactly as many values as the column metadata, otherwise
`SnowflakeRequestError` is raised. A valid empty partition remains valid.

An export can leave a header or earlier rows in the destination when a later
operation fails. The helper closes its internal iterator and leaves the sink
open. The caller chooses whether to retain partial output or publish a temporary
file only after successful completion.
