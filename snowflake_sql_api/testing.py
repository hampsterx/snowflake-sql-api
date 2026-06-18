"""In-process testing utilities for ``snowflake-sql-api``.

Drive :class:`~snowflake_sql_api.client.SnowflakeClient` and
:class:`~snowflake_sql_api.aclient.AsyncSnowflakeClient` against canned results
with **no network and no real Snowflake account**. A :class:`FakeSnowflake`
registry maps SQL statements to rows and exposes itself as an
``httpx.MockTransport`` handler, which plugs into the client's existing transport
seam (``http_client=``). No ``respx`` (or any other test dependency) is needed:
``httpx`` is already a core dependency.

Quick start::

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

Rows can be given as dicts (column order and types inferred) or as positional
lists with an explicit ``columns`` spec. Values are native Python objects; the
fake encodes them to the SQL API wire form so the client coerces them straight
back (a clean round trip), covering numbers, Decimals, booleans, text, binary,
dates/times/timestamps, and VARIANT (dict/list).

Pytest fixtures (``fake_snowflake``, ``snowflake_client``,
``async_snowflake_client``) are auto-registered via the ``pytest11`` entry point
when the package is installed; no ``conftest.py`` wiring is required.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import httpx

if TYPE_CHECKING:
    from .aclient import AsyncSnowflakeClient
    from .client import SnowflakeClient

__all__ = [
    "FakeSnowflake",
    "FakeSnowflakeError",
    "make_client",
    "make_async_client",
    "ok_body",
    "running_body",
]

STATEMENTS_PATH = "/api/v2/statements"

#: A row given to :meth:`FakeSnowflake.register`: a name->value dict or a
#: positional sequence of cell values (the latter needs an explicit ``columns``).
RowInput = Union[Dict[str, Any], Sequence[Any]]

#: A column spec entry: a bare name (type inferred) or an explicit
#: ``{"name": ..., "type": ..., "scale": ...}`` dict.
ColumnSpec = Union[str, Dict[str, Any]]


class FakeSnowflakeError(AssertionError):
    """Raised for a test-setup mistake (unregistered statement, bad request).

    Subclasses :class:`AssertionError` so pytest surfaces it prominently: it
    signals a gap in the fake's wiring, not a simulated Snowflake error. Use
    :meth:`FakeSnowflake.register_error` to simulate real Snowflake failures.
    """


# ---------------------------------------------------------------------------
# SQL API envelope builders (single source of truth, shared with the test suite)
# ---------------------------------------------------------------------------


def ok_body(
    row_type: List[Dict[str, Any]],
    data: List[List[Any]],
    *,
    partitions: int = 1,
    handle: str = "stmt-1",
    stats: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """Build a 200 success body. ``partitions`` sets the partitionInfo length.

    Partition 0's ``rowCount`` reflects ``data``; the remaining entries are
    placeholders (their rows are served by the partition GET routes).
    """
    if partitions < 1:
        raise ValueError("partitions must be >= 1")
    partition_info: List[Dict[str, int]] = [{"rowCount": len(data)}]
    partition_info += [{"rowCount": 0} for _ in range(partitions - 1)]
    body: Dict[str, Any] = {
        "resultSetMetaData": {
            "numRows": len(data),
            "format": "jsonv2",
            "partitionInfo": partition_info,
            "rowType": row_type,
        },
        "data": data,
        "code": "090001",
        "sqlState": "00000",
        "statementHandle": handle,
        "statementStatusUrl": f"{STATEMENTS_PATH}/{handle}",
    }
    if stats is not None:
        body["stats"] = stats
    return body


def running_body(handle: str = "stmt-1", code: str = "333334") -> Dict[str, Any]:
    """Build a 202 'still running / submitted async' body."""
    return {
        "code": code,
        "message": "Asynchronous execution in progress.",
        "statementHandle": handle,
        "statementStatusUrl": f"{STATEMENTS_PATH}/{handle}",
    }


# ---------------------------------------------------------------------------
# Wire encoding (the exact inverse of snowflake_sql_api.types.coerce_value)
# ---------------------------------------------------------------------------

_EPOCH_DATE = date(1970, 1, 1)
_EPOCH_NAIVE = datetime(1970, 1, 1)


def _fraction(micros: int) -> str:
    return "" if micros == 0 else f".{micros:06d}"


def _epoch_delta(value: datetime) -> timedelta:
    """Signed timedelta from the epoch for an absolute instant (as UTC)."""
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value - _EPOCH_NAIVE


def _epoch_string(delta: timedelta) -> str:
    """Format an epoch ``timedelta`` as the SQL API numeric wire string.

    Uses integer math (no float division, so large timestamps keep full
    precision) and carries the sign into the fractional part so the value
    decodes back exactly, including pre-1970 sub-second instants: the production
    decoder in ``types.py`` reads the fraction as signed, so ``-0.5s`` must be
    ``"-0.500000"``, not ``"-1.500000"``.
    """
    total_us = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    negative = total_us < 0
    secs, frac_us = divmod(abs(total_us), 1_000_000)
    if frac_us == 0:
        return f"-{secs}" if negative and secs else str(secs)
    body = f"{secs}.{frac_us:06d}"
    return f"-{body}" if negative else body


def _infer_type(value: Any) -> Tuple[str, Optional[int]]:
    """Infer ``(snowflake_type, scale)`` from a native Python value."""
    if isinstance(value, bool):  # before int: bool is a subclass of int
        return "boolean", None
    if isinstance(value, int):
        return "fixed", 0
    if isinstance(value, Decimal):
        exponent = value.as_tuple().exponent
        scale = -exponent if isinstance(exponent, int) and exponent < 0 else 0
        return "fixed", scale
    if isinstance(value, float):
        return "real", None
    if isinstance(value, (bytes, bytearray)):
        return "binary", None
    if isinstance(value, datetime):  # before date: datetime is a subclass of date
        return ("timestamp_tz" if value.tzinfo is not None else "timestamp_ntz"), None
    if isinstance(value, date):
        return "date", None
    if isinstance(value, time):
        return "time", None
    if isinstance(value, dict):
        return "object", None
    if isinstance(value, (list, tuple)):
        return "array", None
    return "text", None


def _encode_cell(value: Any, col_type: str) -> Optional[str]:
    """Encode one native value to its SQL API wire string (or ``None``)."""
    if value is None:
        return None
    if col_type == "boolean":
        return "true" if value else "false"
    if col_type in ("variant", "object", "array"):
        return json.dumps(value)
    if col_type == "binary":
        return bytes(value).hex()
    if col_type == "date":
        return str((value - _EPOCH_DATE).days)
    if col_type == "time":
        secs = value.hour * 3600 + value.minute * 60 + value.second
        return f"{secs}{_fraction(value.microsecond)}"
    if col_type in ("timestamp_ntz", "timestamp_ltz", "timestamp_tz"):
        if col_type == "timestamp_ntz":
            naive = value.replace(tzinfo=None) if value.tzinfo else value
            return _epoch_string(naive - _EPOCH_NAIVE)
        instant = (
            value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        )
        if col_type == "timestamp_ltz":
            return _epoch_string(_epoch_delta(instant))
        offset = instant.utcoffset()
        offset_min = int(offset.total_seconds() // 60) if offset is not None else 0
        return f"{_epoch_string(_epoch_delta(instant))} {offset_min + 1440}"
    if col_type == "fixed":
        # Plain decimal text, never scientific notation: str(Decimal("1E+2")) is
        # "1E+2", which the decoder's int()/Decimal() parse would reject. A
        # whole-number Decimal still decodes to int (scale 0), matching Snowflake.
        return format(value, "f") if isinstance(value, Decimal) else str(value)
    if col_type in ("real", "float", "double"):
        return repr(float(value))
    # text / char / string / varchar / unknown: stringify as-is.
    return str(value)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


@dataclass
class _Error:
    status: int
    message: str
    code: Optional[str] = None
    sql_state: Optional[str] = None


@dataclass
class _Reg:
    """A registered result: wire rows split across partitions, plus behavior."""

    row_type: List[Dict[str, Any]]
    chunks: List[List[List[Any]]]
    polls_before_ready: int = 0
    stats: Optional[Dict[str, int]] = None
    error: Optional[_Error] = None


@dataclass
class _State:
    """Per-submission handle state (poll progress)."""

    reg: _Reg
    remaining_polls: int


def _normalize_columns(
    rows: List[RowInput], columns: Optional[Sequence[ColumnSpec]]
) -> List[Tuple[str, str, Optional[int]]]:
    """Resolve column ``(name, type, scale)`` triples from rows and/or specs."""
    if columns is not None:
        resolved: List[Tuple[str, str, Optional[int]]] = []
        for index, spec in enumerate(columns):
            if isinstance(spec, str):
                name, ctype, scale = spec, None, None
            else:
                name = spec["name"]
                ctype = spec.get("type")
                scale = spec.get("scale")
            if ctype is None:
                ctype, scale = _infer_column(rows, name, index)
            resolved.append((name, ctype, scale))
        return resolved

    if not rows:
        return []
    first = rows[0]
    if not isinstance(first, dict):
        raise FakeSnowflakeError(
            "positional rows require an explicit `columns=` spec; "
            "pass dict rows to infer column names"
        )
    return [
        _named(name, *_infer_column(rows, name, idx)) for idx, name in enumerate(first)
    ]


def _named(
    name: str, ctype: str, scale: Optional[int]
) -> Tuple[str, str, Optional[int]]:
    return name, ctype, scale


def _infer_column(
    rows: List[RowInput], name: str, index: int
) -> Tuple[str, Optional[int]]:
    """Infer a column's type/scale from the first non-null value across rows."""
    for row in rows:
        value = row[name] if isinstance(row, dict) else row[index]
        if value is not None:
            return _infer_type(value)
    return "text", None


def _cell(row: RowInput, name: str, index: int) -> Any:
    return row[name] if isinstance(row, dict) else row[index]


def _split(data: List[List[Any]], partitions: int) -> List[List[List[Any]]]:
    """Split wire rows into ``partitions`` chunks, preserving order."""
    if partitions < 1:
        raise ValueError("partitions must be >= 1")
    if partitions == 1:
        return [data]
    size = -(-len(data) // partitions) or 1  # ceil division, at least 1
    chunks = [data[i : i + size] for i in range(0, len(data), size)]
    while len(chunks) < partitions:
        chunks.append([])
    return chunks


class FakeSnowflake:
    """In-memory Snowflake SQL API stand-in backed by ``httpx.MockTransport``.

    Register results for SQL statements, then build a client with
    :func:`make_client` / :func:`make_async_client` (or pass ``.transport`` to an
    ``httpx.Client``). Lookups try exact-string matches first, then predicate
    matches in registration order.
    """

    def __init__(self) -> None:
        self._exact: Dict[str, _Reg] = {}
        self._predicates: List[Tuple[Callable[[str], bool], _Reg]] = []
        self._state: Dict[str, _State] = {}
        self._counter = 0
        #: Every request the fake handled, in order (for assertions).
        self.requests: List[httpx.Request] = []

    # -- registration -----------------------------------------------------

    def register(
        self,
        sql: str,
        rows: Sequence[RowInput],
        *,
        columns: Optional[Sequence[ColumnSpec]] = None,
        partitions: int = 1,
        polls_before_ready: int = 0,
        stats: Optional[Dict[str, int]] = None,
    ) -> "FakeSnowflake":
        """Register the result rows returned for an exact ``sql`` string.

        ``rows`` are dicts (names/types inferred) or positional sequences (need
        ``columns``). ``partitions`` splits the rows across result partitions to
        exercise multi-partition fetching. ``polls_before_ready`` makes the
        statement report RUNNING (HTTP 202) for that many status polls first.
        """
        self._exact[sql] = self._build(
            list(rows), columns, partitions, polls_before_ready, stats
        )
        return self

    def register_match(
        self,
        predicate: Callable[[str], bool],
        rows: Sequence[RowInput],
        *,
        columns: Optional[Sequence[ColumnSpec]] = None,
        partitions: int = 1,
        polls_before_ready: int = 0,
        stats: Optional[Dict[str, int]] = None,
    ) -> "FakeSnowflake":
        """Register a result for any statement where ``predicate(sql)`` is true."""
        reg = self._build(list(rows), columns, partitions, polls_before_ready, stats)
        self._predicates.append((predicate, reg))
        return self

    def register_dml(self, sql: str, rowcount: int) -> "FakeSnowflake":
        """Register a DML/DDL statement, returning ``rowcount`` rows affected."""
        self._exact[sql] = _Reg(row_type=[], chunks=[[[str(rowcount)]]])
        return self

    def register_error(
        self,
        sql: str,
        message: str,
        *,
        status: int = 422,
        code: Optional[str] = None,
        sql_state: Optional[str] = None,
    ) -> "FakeSnowflake":
        """Register a statement that fails (default HTTP 422 -> programming error)."""
        self._exact[sql] = _Reg(
            row_type=[],
            chunks=[[]],
            error=_Error(
                status=status, message=message, code=code, sql_state=sql_state
            ),
        )
        return self

    def _build(
        self,
        rows: List[RowInput],
        columns: Optional[Sequence[ColumnSpec]],
        partitions: int,
        polls_before_ready: int,
        stats: Optional[Dict[str, int]],
    ) -> _Reg:
        specs = _normalize_columns(rows, columns)
        row_type: List[Dict[str, Any]] = []
        for name, ctype, scale in specs:
            entry: Dict[str, Any] = {"name": name, "type": ctype, "nullable": True}
            if scale is not None:
                entry["scale"] = scale
            row_type.append(entry)
        wire: List[List[Any]] = [
            [
                _encode_cell(_cell(row, name, index), ctype)
                for index, (name, ctype, _scale) in enumerate(specs)
            ]
            for row in rows
        ]
        return _Reg(
            row_type=row_type,
            chunks=_split(wire, partitions),
            polls_before_ready=polls_before_ready,
            stats=stats,
        )

    # -- transport seam ---------------------------------------------------

    @property
    def transport(self) -> httpx.MockTransport:
        """An ``httpx.MockTransport`` wired to this fake (for sync or async)."""
        return httpx.MockTransport(self.handle)

    # -- introspection ----------------------------------------------------

    @property
    def submitted_statements(self) -> List[str]:
        """Every SQL statement submitted via POST, in order."""
        out: List[str] = []
        for request in self.requests:
            if request.method == "POST" and request.url.path == STATEMENTS_PATH:
                out.append(json.loads(request.content).get("statement", ""))
        return out

    # -- request handling -------------------------------------------------

    def handle(self, request: httpx.Request) -> httpx.Response:
        """Route a request to the right canned response (the MockTransport hook)."""
        self.requests.append(request)
        path = request.url.path
        if request.method == "POST" and path == STATEMENTS_PATH:
            return self._on_submit(request)
        if request.method == "POST" and path.endswith("/cancel"):
            return httpx.Response(200, json={"code": "090001"})
        if request.method == "GET" and path.startswith(STATEMENTS_PATH + "/"):
            return self._on_get(request)
        raise FakeSnowflakeError(f"unexpected request: {request.method} {path}")

    def _lookup(self, sql: str) -> Optional[_Reg]:
        if sql in self._exact:
            return self._exact[sql]
        for predicate, reg in self._predicates:
            if predicate(sql):
                return reg
        return None

    def _next_handle(self) -> str:
        self._counter += 1
        return f"stmt-{self._counter}"

    def _on_submit(self, request: httpx.Request) -> httpx.Response:
        sql = json.loads(request.content).get("statement", "")
        reg = self._lookup(sql)
        if reg is None:
            raise FakeSnowflakeError(f"no result registered for statement: {sql!r}")
        handle = self._next_handle()
        if reg.error is not None:
            err = reg.error
            return httpx.Response(
                err.status,
                json={
                    "message": err.message,
                    "code": err.code,
                    "sqlState": err.sql_state,
                    "statementHandle": handle,
                },
            )
        self._state[handle] = _State(reg=reg, remaining_polls=reg.polls_before_ready)
        async_exec = request.url.params.get("async") == "true"
        if async_exec or reg.polls_before_ready > 0:
            return httpx.Response(202, json=running_body(handle))
        return self._result_response(handle)

    def _on_get(self, request: httpx.Request) -> httpx.Response:
        handle = request.url.path.rsplit("/", 1)[-1]
        state = self._state.get(handle)
        if state is None:
            raise FakeSnowflakeError(f"unknown statement handle: {handle}")
        partition = request.url.params.get("partition")
        if partition is not None:
            index = int(partition)
            chunks = state.reg.chunks
            if not 0 <= index < len(chunks):
                # Surface partition bugs instead of masking them as an empty page.
                raise FakeSnowflakeError(
                    f"partition {index} out of range (have {len(chunks)})"
                )
            return httpx.Response(200, json={"data": chunks[index]})
        if state.remaining_polls > 0:
            state.remaining_polls -= 1
            return httpx.Response(202, json=running_body(handle))
        return self._result_response(handle)

    def _result_response(self, handle: str) -> httpx.Response:
        reg = self._state[handle].reg
        body = ok_body(
            reg.row_type,
            reg.chunks[0],
            partitions=len(reg.chunks),
            handle=handle,
            stats=reg.stats,
        )
        return httpx.Response(200, json=body)


# ---------------------------------------------------------------------------
# Client factories
# ---------------------------------------------------------------------------

_THROWAWAY_KEY_PEM: Optional[bytes] = None


def _throwaway_key() -> bytes:
    """A cached throwaway RSA private key (PEM). The fake never validates it."""
    global _THROWAWAY_KEY_PEM
    if _THROWAWAY_KEY_PEM is None:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        _THROWAWAY_KEY_PEM = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    return _THROWAWAY_KEY_PEM


#: Constructor args the factories set themselves; passing them via **kwargs would
#: raise an opaque duplicate-keyword TypeError, so reject them with a clear error.
_HELPER_MANAGED = ("private_key", "http_client")


def _reject_managed_kwargs(name: str, kwargs: Dict[str, Any]) -> None:
    overlap = sorted(set(_HELPER_MANAGED) & kwargs.keys())
    if overlap:
        raise FakeSnowflakeError(
            f"{name} manages {overlap} internally; pass session/client options only"
        )


def make_client(
    fake: FakeSnowflake,
    *,
    account: str = "testorg-testaccount",
    user: str = "TEST_USER",
    poll_interval: float = 0.0,
    **kwargs: Any,
) -> "SnowflakeClient":
    """Build a :class:`SnowflakeClient` wired to ``fake`` (no network).

    Extra keyword args pass straight through to the client constructor (but not
    the args this factory sets itself; see ``_HELPER_MANAGED``).
    """
    from .client import SnowflakeClient

    _reject_managed_kwargs("make_client", kwargs)
    return SnowflakeClient(
        account,
        user,
        private_key=_throwaway_key(),
        http_client=httpx.Client(transport=fake.transport),
        poll_interval=poll_interval,
        **kwargs,
    )


def make_async_client(
    fake: FakeSnowflake,
    *,
    account: str = "testorg-testaccount",
    user: str = "TEST_USER",
    poll_interval: float = 0.0,
    **kwargs: Any,
) -> "AsyncSnowflakeClient":
    """Build an :class:`AsyncSnowflakeClient` wired to ``fake`` (no network)."""
    from .aclient import AsyncSnowflakeClient

    _reject_managed_kwargs("make_async_client", kwargs)
    return AsyncSnowflakeClient(
        account,
        user,
        private_key=_throwaway_key(),
        http_client=httpx.AsyncClient(transport=fake.transport),
        poll_interval=poll_interval,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Pytest fixtures (auto-discovered via the pytest11 entry point in pyproject.toml)
# ---------------------------------------------------------------------------

try:
    import pytest

    @pytest.fixture
    def fake_snowflake() -> FakeSnowflake:
        """A fresh :class:`FakeSnowflake` registry."""
        return FakeSnowflake()

    @pytest.fixture
    def snowflake_client(fake_snowflake: FakeSnowflake) -> "Iterator[SnowflakeClient]":
        """A :class:`SnowflakeClient` wired to the ``fake_snowflake`` fixture."""
        client = make_client(fake_snowflake)
        try:
            yield client
        finally:
            client.close()

    try:
        import pytest_asyncio

        @pytest_asyncio.fixture
        async def async_snowflake_client(
            fake_snowflake: FakeSnowflake,
        ) -> "AsyncIterator[AsyncSnowflakeClient]":
            """An :class:`AsyncSnowflakeClient` wired to ``fake_snowflake``."""
            client = make_async_client(fake_snowflake)
            try:
                yield client
            finally:
                await client.aclose()

    except ImportError:  # pragma: no cover - async fixture is optional
        pass

except ImportError:  # pragma: no cover - pytest is a test-only dependency
    pass
