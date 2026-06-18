# Troubleshooting

The client maps SQL API responses onto a typed exception hierarchy rooted at
`SnowflakeError`. Catch that to handle everything, or branch on the specific
types below.

| Exception | Raised when |
|-----------|-------------|
| `SnowflakeConfigError` | bad/missing config, before any request (no key, unreadable key, missing extra) |
| `SnowflakeAuthError` | JWT generation or Snowflake auth failed (HTTP 401) |
| `SnowflakeProgrammingError` | the SQL failed to compile or execute (HTTP 422) |
| `SnowflakeRequestError` | an HTTP/protocol-level failure (carries `code`, `sql_state`, `status_code`) |
| `SnowflakeTimeoutError` | an HTTP request or a statement exceeded its timeout |
| `SnowflakeRetryError` | retries exhausted (original error on `__cause__`) |
| `ResultNotReady` | `QueryHandle.result(poll=False)` while the statement is still running |

## Authentication failures (401 / `SnowflakeAuthError`)

Work through these in order:

1. **Public key not registered or mismatched.** `DESCRIBE USER my_user` and
   compare `RSA_PUBLIC_KEY_FP` against the key you are signing with. Regenerating
   the key without re-running `ALTER USER ... SET RSA_PUBLIC_KEY` is the usual
   cause.
2. **Account locator.** The JWT claim account must have the region/cloud suffix
   stripped (`xy12345.ap-southeast-2` to `XY12345`); the client does this for
   you. If you hand-built a host or claim, recheck it. See
   [authentication.md](authentication.md#the-account-locator-region-gotcha).
3. **Clock skew.** JWTs are time-bound; a host clock off by minutes will be
   rejected. Sync the clock (NTP).
4. **Encrypted key.** A passphrase-protected key without the right
   `private_key_passphrase` raises `SnowflakeConfigError` at construction, not a
   401.

## SQL errors (422 / `SnowflakeProgrammingError`)

The statement reached Snowflake and was rejected (syntax error, missing object,
constraint violation). The exception carries Snowflake's `code` and `sql_state`:

```python
from snowflake_sql_api.exceptions import SnowflakeProgrammingError

try:
    client.query("SELECT * FROM does_not_exist")
except SnowflakeProgrammingError as exc:
    print(exc.code, exc.sql_state, exc)
```

## Long-running statements (202 / polling)

Large or slow statements come back as HTTP 202 ("still running"). The standard
`query`/`execute` calls poll automatically until completion (bounded by
`statement_timeout` if set, else a default wait).

For fire-and-forget, submit asynchronously and poll yourself:

```python
handle = client.submit("CALL long_running_proc()")

handle.status()              # "RUNNING" or "SUCCESS"

# Non-blocking fetch: raises ResultNotReady if still running, instead of
# returning a misleading in-progress payload.
from snowflake_sql_api.exceptions import ResultNotReady
try:
    rows = handle.result(poll=False)
except ResultNotReady:
    ...  # check again later

rows = handle.result()       # blocking: polls until done
```

If a statement never finishes within the wait, you get `SnowflakeTimeoutError`.

## Large result sets (partitions)

The SQL API splits large results into partitions: partition 0 arrives inline,
the rest are fetched by index. `query` fetches **every** partition and returns
the rows in order, so you never silently get a truncated result. No action
needed; just be aware a single `query` may issue several GETs for a big result.

## Transient failures and retries

Connect/read timeouts and HTTP 429/5xx are retried with exponential backoff and
full jitter. DML submits reuse their `requestId` with `retry=true`, so a retried
insert/update cannot double-apply. When retries are exhausted you get
`SnowflakeRetryError` with the last underlying error attached as `__cause__`.

Tune via `retry_policy=RetryPolicy(...)` on the client constructor.

## Connection / network

`httpx` transport errors (DNS, TLS, connection refused) surface after retries as
`SnowflakeRetryError`. Check the derived host
(`<account>.snowflakecomputing.com`) is reachable from your network; for
PrivateLink or custom endpoints pass `host=` explicitly.

## Testing without a real account

To unit-test code that uses this client, mock it with the shipped
`snowflake_sql_api.testing` helper (no network, no Snowflake). See
[testing.md](testing.md).
