# Troubleshooting

The client maps SQL API responses onto a typed exception hierarchy rooted at
`SnowflakeError`. Catch that to handle everything, or branch on the specific
types below.

| Exception | Raised when |
|-----------|-------------|
| `SnowflakeConfigError` | bad/missing config or an invalid argument, raised locally (for example no key, unreadable key, unbindable parameter value, invalid write columns, rows or limits), or duplicate names in completed result metadata, found after the statement has run |
| `BatchWriteError` | an `insert_batches()` import stopped after input iteration began; the original error is its `__cause__`, including a `SnowflakeConfigError` for an invalid row (see [batch writes](batch-writes.md#partial-completion-and-failures)) |
| `SnowflakeAuthError` | Snowflake rejected authentication (HTTP 401); local JWT signing errors propagate with their original types |
| `SnowflakeForbiddenError` | Snowflake refused an authenticated request (HTTP 403); a `SnowflakeRequestError` subclass |
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

## Forbidden requests (403 / `SnowflakeForbiddenError`)

The JWT was accepted, but Snowflake refused the request. Retrying with the same
identity will not help, so the default `RetryPolicy` does not retry it. (A
custom policy that lists 403 in `retry_statuses` surfaces it as
`SnowflakeRetryError` instead, with a plain `SnowflakeRequestError` as the
`__cause__`.) The exception carries the same
fields as `SnowflakeRequestError` (`status_code`, `code`, `sql_state`,
`statement_handle`) and subclasses it, so an existing
`except SnowflakeRequestError` still catches it.

Read the message first; it usually names the cause. Common ones:

1. **SQL API not enabled.** Snowflake's SQL API reference gives this as the
   cause of a 403. Check with your account administrator.
2. **Network policy.** If the message names an IP address, an account- or
   user-level network policy is blocking the caller, whatever the key. Compare
   the egress IP your requests actually leave from (NAT, proxy, VPC gateway)
   against the policy's allowed list.

A role that lacks a privilege on a table or view is a different failure: the
statement reaches Snowflake and fails to compile, which comes back as a 422
`SnowflakeProgrammingError` ("does not exist or not authorized"), not a 403.

```python
from snowflake_sql_api.exceptions import SnowflakeForbiddenError

try:
    client.query("SELECT 1")
except SnowflakeForbiddenError as exc:
    print(exc.code, exc)
```

## SQL errors (422 / `SnowflakeProgrammingError`)

The statement reached Snowflake and was rejected (syntax error, missing object,
constraint violation). The exception carries Snowflake's `code` and `sql_state`.
From `insert_many()` or `insert_batches()`, a missing-object error for a table you
know exists usually means a lowercase name for a table stored in uppercase; see
[identifier case](batch-writes.md#identifier-case).

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
`statement_timeout` if set, else a default wait of 600 seconds).

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
Who cancels it depends on who owns the statement handle:

- **Calls that poll for you** (`query` and its helpers, `execute`, `insert_many`,
  `insert_batches`, `query_stream`, `query_batches`, `export_csv`,
  `export_jsonl`) started the statement themselves, so nobody else can stop it.
  When the wait runs out they make a best-effort cancel request for that
  statement, then raise the timeout. Without it, a statement submitted with no
  `statement_timeout` would keep running server-side until the account's
  `STATEMENT_TIMEOUT_IN_SECONDS` (two days by default). If the cancel request
  itself fails, you still get the `SnowflakeTimeoutError`, with the cancel
  failure on its `cancel_error` attribute and in its notes. `insert_batches`
  raises `BatchWriteError` instead, with the `SnowflakeTimeoutError` as its
  `__cause__` (see [batch writes](batch-writes.md)). The cancel can arrive
  after the statement finished, so a DML statement that timed out may still
  have committed. The cancel request is retried under the client's
  `retry_policy`, so a failing cancel can delay the timeout by the retry
  backoff plus up to one HTTP `timeout` per attempt. A statement that
  legitimately runs longer than the wait is cancelled too, so set a larger
  `statement_timeout` for it, or use `submit()` and own the handle.
- **`QueryHandle.result()`** (and `AsyncQueryHandle.result()`) does not cancel on
  timeout. You hold the handle and may poll it again later; call
  `handle.cancel()` when you want the statement stopped.

The wait is measured on a monotonic clock, so a system clock correction does
not shorten or stretch it.

`statement_timeout` must be a positive integer number of seconds, or `None`
(the default) for no client setting. Any other value, including `0`, a
negative number or a `bool`, raises `SnowflakeConfigError` when the client is
constructed. Earlier releases accepted `0`, which the client read as a
zero-second wait (cancelling any statement still running at the first poll)
while Snowflake reads a `timeout` of `0` as its maximum, 604800 seconds. Leave
it unset to get Snowflake's `STATEMENT_TIMEOUT_IN_SECONDS` server-side and the
600-second default wait client-side.

Setting `statement_timeout` also sends Snowflake a server-side `timeout`, so
the statement stops on its own even if the client process dies before it can
cancel.

### Interrupting a poll

- **Async task cancellation.** When the task awaiting a call that polls for you
  is cancelled (`task.cancel()`, an enclosing `asyncio.timeout`, a `TaskGroup`
  failing), the client sends a best-effort cancel for the statement it started,
  then re-raises the `CancelledError` unchanged. The cancel request runs in its
  own task, is not abandoned if the task is cancelled again, and is given at
  most 5 seconds; a failed cancel is not reported. A task cancelled while
  awaiting `AsyncQueryHandle.result()` leaves the statement running, as a
  timeout there does. The cancel needs the statement handle, so it is sent
  only once Snowflake has answered the submit with HTTP 202: a task cancelled
  while the submit request is still waiting leaves the statement running until
  its server-side `timeout` or the account's `STATEMENT_TIMEOUT_IN_SECONDS`.
- **Sync `KeyboardInterrupt`.** The sync client does not cancel the statement
  on Ctrl-C: an HTTP call made while the process is being interrupted can hang.
  The statement keeps running until its server-side `timeout` or the account's
  `STATEMENT_TIMEOUT_IN_SECONDS`. Set `statement_timeout` if that matters.

## Large result sets (partitions)

The SQL API splits large results into partitions: partition 0 arrives inline,
the rest are fetched by index. `query` fetches **every** partition and returns
the rows in order, so you never silently get a truncated result. No action
needed; just be aware a single `query` may issue several GETs for a big result.

## Transient failures and retries

Connect/read timeouts and HTTP 429/5xx are retried with exponential backoff and
full jitter. Later HTTP submission attempts reuse the original `requestId` and
body and add `retry=true`. Snowflake documents that a successfully executed
statement is not executed again when resubmitted with that same ID and retry
flag. This depends on identical inputs and retained history, and supplies no
unlimited replay window or application-wide exactly-once guarantee.

When retries are exhausted you get `SnowflakeRetryError` with the last underlying
error attached as `__cause__`. This is an uncertain outcome, not proof the SQL
failed. For worker recovery, persist a caller-owned UUID and all inputs before
`submit(..., request_id=...)`, then use `resubmit(..., request_id=...)` after an
uncertain attempt. Use a saved statement handle directly when available. Do not
repeat ordinary `submit()` with the same ID as a recovery recipe or silently
allocate a new one. Submission errors in the `SnowflakeError` family and returned
handles expose `request_id`; other exceptions pass through without a request ID.
GET/poll errors can be correlated through their handle instead.

See [replayable submissions](submissions.md) for payload invariants, history and
query-size limits, and the live verification gate.

Tune via `retry_policy=RetryPolicy(...)` on either client constructor. The
frozen policy defaults to three total attempts. HTTP 429 uses full jitter with
5 and 8 second delay ceilings, while other retryable failures use full jitter
with ceilings of 0.5 and 1 second. `rate_limit_backoff_multiplier=10.0` scales
the base for 429 before the `max_backoff` cap; `1.0` restores the ordinary schedule.

`Retry-After` delay-seconds or HTTP dates on retryable responses override the
schedule without jitter, capped by `max_retry_after=60.0` seconds per wait.
Invalid headers fall back to the schedule, and past dates give zero delay.
`base_backoff=0` disables scheduled backoff; `max_retry_after=0` disables
header-directed waits. A default request can wait up to 13 seconds across 429
retries without a header, or 120 seconds with capped headers, plus HTTP timeouts.
Status and partition GETs each have their own retry loop; this is not an overall
query deadline. See [retry configuration](../README.md#retries) for the policy
methods and date handling.

## Connection / network

`httpx` transport errors (DNS, TLS, connection refused) surface after retries as
`SnowflakeRetryError`. Check the derived host
(`<account>.snowflakecomputing.com`) is reachable from your network; for
PrivateLink or custom endpoints pass `host=` explicitly.

## Testing without a real account

To unit-test code that uses this client, mock it with the shipped
`snowflake_sql_api.testing` helper (no network, no Snowflake). See
[testing.md](testing.md).
