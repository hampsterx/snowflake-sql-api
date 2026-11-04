"""Keep the sync and async clients' public call surfaces aligned."""

import inspect

from snowflake_sql_api import AsyncSnowflakeClient, SnowflakeClient


def test_client_public_method_parity() -> None:
    lifecycle_names = {
        "aclose": "close",
        "__aenter__": "__enter__",
        "__aexit__": "__exit__",
    }
    methods = []
    for client in (SnowflakeClient, AsyncSnowflakeClient):
        public = {}
        for name, method in inspect.getmembers(client, callable):
            normalized = lifecycle_names.get(name, name)
            if normalized.startswith("_") and normalized not in {
                "__init__",
                "__enter__",
                "__exit__",
            }:
                continue
            parameters = []
            for parameter in inspect.signature(method).parameters.values():
                # The caller-owned HTTP client follows each client's I/O model.
                if parameter.name == "http_client":
                    parameter = parameter.replace(
                        annotation=str(parameter.annotation).replace(
                            "httpx.AsyncClient", "httpx.Client"
                        )
                    )
                parameters.append(parameter)
            public[normalized] = parameters
        methods.append(public)

    sync, async_ = methods
    assert sync.keys() == async_.keys(), "public method names differ"
    for name in sync:
        assert sync[name] == async_[name], f"parameters differ for {name}"
