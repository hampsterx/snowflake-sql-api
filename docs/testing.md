# Testing

`snowflake_sql_api.testing` lets you drive `SnowflakeClient` /
`AsyncSnowflakeClient` against canned results with **no network and no Snowflake
account**. It ships with the package (no extra dependency: it is built on
`httpx.MockTransport`, and `httpx` is already a core dependency). You do not need
`respx` or any other HTTP mock.

## Quick start

```python
from snowflake_sql_api.testing import FakeSnowflake, make_client

fake = FakeSnowflake()
fake.register("SELECT id, name FROM users", [
    {"ID": 1, "NAME": "alice"},
    {"ID": 2, "NAME": "bob"},
])

client = make_client(fake)
assert client.query("SELECT id, name FROM users") == [
    {"ID": 1, "NAME": "alice"},
    {"ID": 2, "NAME": "bob"},
]
```

`make_client(fake)` returns a real `SnowflakeClient` (generated throwaway key, an
`httpx.MockTransport` wired to `fake`, instant polling). Every code path runs for
real except the wire: auth signs a JWT, the transport builds requests, results
are coerced. Only Snowflake is faked.

## Registering results

Rows can be dicts (column names and types inferred) or positional lists with an
explicit `columns` spec:

```python
fake.register("SELECT id, name FROM t", [[1, "alice"], [2, "bob"]],
              columns=["ID", "NAME"])
```

Give explicit column types when inference is not enough (e.g. a fixed-point
scale, or a timestamp variant):

```python
from decimal import Decimal

fake.register("SELECT amount FROM t", [{"AMOUNT": Decimal("10.50")}],
              columns=[{"name": "AMOUNT", "type": "fixed", "scale": 2}])
```

Values are native Python objects. The fake encodes them to the SQL API wire form
and the client coerces them straight back, so the round trip is lossless for:
integers and `Decimal`, floats, text, booleans, `bytes`, `date` / `time` /
`datetime` (naive and tz-aware), and VARIANT (`dict` / `list`). `None` becomes a
SQL NULL.

### Predicate matching

When you cannot pin an exact SQL string (generated SQL, bound inserts), match on
a predicate:

```python
fake.register_match(lambda sql: sql.startswith("SELECT count"), [{"N": 99}])
```

Lookups try exact matches first, then predicates in registration order.

### DML and errors

```python
fake.register_dml("DELETE FROM t WHERE id = 1", rowcount=1)
assert client.execute("DELETE FROM t WHERE id = 1") == 1

fake.register_error("SELECT bad syntax", "SQL compilation error", code="000904")
# client.query("SELECT bad syntax") now raises SnowflakeProgrammingError
```

## Multi-partition results

Split rows across partitions to exercise the partition-fetch path:

```python
rows = [{"N": n} for n in range(1000)]
fake.register("SELECT n FROM big", rows, partitions=4)
assert client.query("SELECT n FROM big") == rows   # all partitions, in order
```

## Long-running / async-submit

`polls_before_ready` makes a statement report RUNNING (HTTP 202) for that many
status polls before completing:

```python
fake.register("CALL slow()", [{"DONE": True}], polls_before_ready=2)

handle = client.submit("CALL slow()")
# handle.result(poll=False) would raise ResultNotReady here
rows = handle.result()        # polls until ready, then returns rows
```

## Async client

```python
from snowflake_sql_api.testing import FakeSnowflake, make_async_client

async def test_async():
    fake = FakeSnowflake()
    fake.register("SELECT 1", [{"N": 1}])
    async with make_async_client(fake) as client:
        assert await client.query_scalar("SELECT 1") == 1
```

## Pytest fixtures

Installing the package registers three fixtures via a `pytest11` entry point. No
`conftest.py` wiring is needed:

| Fixture | Provides |
|---------|----------|
| `fake_snowflake` | a fresh `FakeSnowflake` |
| `snowflake_client` | a `SnowflakeClient` wired to `fake_snowflake` |
| `async_snowflake_client` | an `AsyncSnowflakeClient` wired to `fake_snowflake` (registered only when `pytest-asyncio` is installed) |

```python
def test_users(fake_snowflake, snowflake_client):
    fake_snowflake.register("SELECT name FROM users", [{"NAME": "alice"}])
    assert snowflake_client.query_column("SELECT name FROM users") == ["alice"]
```

> **Coverage note:** because the package ships a pytest plugin, measure coverage
> with `coverage run -m pytest` rather than `pytest --cov`. The former starts
> tracing before the plugin is imported; the latter records the plugin's
> import-time lines as uncovered.

## Drop-in for application code

If your code constructs its own client, build it against the fake by injecting
the mock transport through `http_client`, or patch your factory to return
`make_client(fake)`:

```python
import httpx
from snowflake_sql_api import SnowflakeClient

client = SnowflakeClient(
    account="myorg-myaccount",
    user="MY_USER",
    private_key=test_key_bytes,
    http_client=httpx.Client(transport=fake.transport),
)
```

## Asserting on what ran

```python
fake.submitted_statements   # list of SQL strings submitted, in order
fake.requests               # every httpx.Request the fake handled
```
