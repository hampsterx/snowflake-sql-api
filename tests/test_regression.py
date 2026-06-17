"""Named regression tests for the four spike-discovered bugs.

Each fixed bug gets a test marked ``regression`` so ``pytest -k regression``
proves they stay fixed. Bug #2 (on_query in the streaming path) lands with
``query_stream`` in the v0.2.0 toolkit (Phase 8).
"""

from __future__ import annotations

import httpx
import pytest
import respx

from snowflake_sql_api import SnowflakeClient
from snowflake_sql_api.auth import normalize_account_locator
from snowflake_sql_api.exceptions import ResultNotReady

from .support import ACCOUNT, STATEMENTS_URL, USER, ok_body, running_body, statement_url

pytestmark = pytest.mark.regression

INT_COL = [{"name": "N", "type": "fixed", "scale": 0}]


def test_regression_bug1_jwt_account_locator_strips_region() -> None:
    """Bug #1: the JWT claim account must drop the region/cloud suffix.

    Leaving the region in (``XY12345.AP-SOUTHEAST-2``) makes the JWT invalid.
    """
    assert normalize_account_locator("xy12345.ap-southeast-2") == "XY12345"
    assert normalize_account_locator("xy12345.us-east-1.aws") == "XY12345"
    assert normalize_account_locator("xy12345") == "XY12345"
    # Org-account dash form is preserved (no dot to strip).
    assert normalize_account_locator("myorg-myaccount") == "MYORG-MYACCOUNT"


@respx.mock
def test_regression_bug3_result_poll_false_raises_on_202(
    private_key_pem: bytes,
) -> None:
    """Bug #3: result(poll=False) on a still-running statement signals not-ready.

    It must raise ResultNotReady rather than hand back the 202 body as a result.
    """
    handle = "still-running"
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(202, json=running_body(handle))
    )
    respx.get(statement_url(handle)).mock(
        return_value=httpx.Response(202, json=running_body(handle, code="333333"))
    )
    client = SnowflakeClient(ACCOUNT, USER, private_key=private_key_pem)
    handle_obj = client.submit("CALL long_running()")
    with pytest.raises(ResultNotReady) as info:
        handle_obj.result(poll=False)
    assert info.value.statement_handle == handle
    client.close()


@respx.mock
def test_regression_bug4_fetches_all_partitions(private_key_pem: bytes) -> None:
    """Bug #4: the main query path must fetch every partition, not just 0.

    Stopping at partition 0 silently truncates large result sets.
    """
    handle = "multi"
    respx.post(STATEMENTS_URL).mock(
        return_value=httpx.Response(
            200, json=ok_body(INT_COL, [["0"]], partitions=3, handle=handle)
        )
    )

    def by_partition(request: httpx.Request) -> httpx.Response:
        index = request.url.params.get("partition")
        if index == "1":
            return httpx.Response(200, json={"data": [["1"], ["2"]]})
        if index == "2":
            return httpx.Response(200, json={"data": [["3"]]})
        return httpx.Response(200, json={"data": []})

    respx.get(statement_url(handle)).mock(side_effect=by_partition)

    client = SnowflakeClient(ACCOUNT, USER, private_key=private_key_pem)
    rows = client.query("SELECT n FROM big_table")
    client.close()
    # Partition 0 (inline) + partitions 1 and 2, in order, none dropped.
    assert [r["N"] for r in rows] == [0, 1, 2, 3]
