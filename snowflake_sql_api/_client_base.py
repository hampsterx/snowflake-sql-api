"""Configuration and statement construction shared by both clients."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Self, Sequence, Tuple

from .auth import KeypairAuthenticator, account_hostname
from .bindings import to_bindings
from .escaping import quote_identifier, quote_name
from .exceptions import SnowflakeConfigError

#: Positional bind parameters for a statement.
Params = Optional[Sequence[Any]]

#: Callback fired for every executed statement (logging / instrumentation hook).
OnQuery = Callable[[str, "Params"], None]


class _ClientBase:
    """Shared non-I/O state and helpers; concrete clients own their transports."""

    def __init__(
        self,
        account: str,
        user: str,
        *,
        private_key: Optional[bytes] = None,
        private_key_path: Optional[str] = None,
        private_key_passphrase: Optional[str] = None,
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        timezone: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        host: Optional[str] = None,
        statement_timeout: Optional[int] = None,
        poll_interval: float = 1.0,
        on_query: Optional[OnQuery] = None,
    ) -> None:
        if private_key is None and private_key_path is None:
            raise SnowflakeConfigError("a private_key or private_key_path is required")
        try:
            key_bytes = (
                private_key
                if private_key is not None
                else Path(private_key_path).read_bytes()  # type: ignore[arg-type]
            )
        except OSError as exc:
            raise SnowflakeConfigError(
                f"could not read private key file: {private_key_path}"
            ) from exc
        self.account = account
        self.user = user
        self.role = role
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.timezone = timezone
        self._parameters = dict(parameters) if parameters else {}
        self._statement_timeout = statement_timeout
        self._poll_interval = poll_interval
        self.on_query = on_query

        self._auth = KeypairAuthenticator(
            account, user, key_bytes, private_key_passphrase=private_key_passphrase
        )
        self._host = host or account_hostname(account)

    @classmethod
    def from_env(cls) -> Self:
        """Construct a client from ``SNOWFLAKE_*`` environment variables."""
        account = os.environ.get("SNOWFLAKE_ACCOUNT")
        user = os.environ.get("SNOWFLAKE_USER")
        if not account or not user:
            raise SnowflakeConfigError(
                "SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER are required"
            )
        key_pem = os.environ.get("SNOWFLAKE_PRIVATE_KEY")
        return cls(
            account=account,
            user=user,
            private_key=key_pem.encode() if key_pem else None,
            private_key_path=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
            private_key_passphrase=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
            role=os.environ.get("SNOWFLAKE_ROLE"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
            database=os.environ.get("SNOWFLAKE_DATABASE"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA"),
            host=os.environ.get("SNOWFLAKE_HOST"),
        )

    def _build_payload(self, sql: str, params: Params) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"statement": sql}
        if self._statement_timeout is not None:
            payload["timeout"] = self._statement_timeout
        if self.database:
            payload["database"] = self.database
        if self.schema:
            payload["schema"] = self.schema
        if self.warehouse:
            payload["warehouse"] = self.warehouse
        if self.role:
            payload["role"] = self.role
        parameters = dict(self._parameters)
        if self.timezone:
            parameters["TIMEZONE"] = self.timezone
        if parameters:
            payload["parameters"] = parameters
        if params:
            payload["bindings"] = to_bindings(params)
        return payload

    def _notify(self, sql: str, params: Params) -> None:
        if self.on_query is not None:
            self.on_query(sql, params)

    @staticmethod
    def _insert_statement(
        table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]]
    ) -> Optional[Tuple[str, List[Any]]]:
        if not columns:
            raise SnowflakeConfigError("insert_many requires at least one column")
        if not rows:
            return None
        width = len(columns)
        flat: List[Any] = []
        for row in rows:
            if len(row) != width:
                raise SnowflakeConfigError(
                    f"each row must have {width} values, got {len(row)}"
                )
            flat.extend(row)
        cols_sql = ", ".join(quote_identifier(c) for c in columns)
        placeholder = "(" + ", ".join(["?"] * width) + ")"
        values_sql = ", ".join([placeholder] * len(rows))
        sql = f"INSERT INTO {quote_name(table)} ({cols_sql}) VALUES {values_sql}"
        return sql, flat
