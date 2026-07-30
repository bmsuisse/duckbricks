from __future__ import annotations

import httpx
import pytest
import respx

from duckbricks import DatabricksClient


@pytest.mark.asyncio
@respx.mock
async def test_failed_statement_raises(warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    respx.mock.get(f"{host}/api/2.0/sql/warehouses/{warehouse_id}").mock(
        return_value=httpx.Response(200, json={"state": "RUNNING"})
    )
    respx.mock.post(f"{host}/api/2.0/sql/statements").mock(
        return_value=httpx.Response(
            200,
            json={
                "statement_id": "stmt-failed",
                "status": {"state": "FAILED", "error": {"error_code": "SYNTAX_ERROR", "message": "bad sql"}},
            },
        )
    )
    client = DatabricksClient(host, warehouse_id, token="test-token")

    with pytest.raises(RuntimeError, match="SYNTAX_ERROR"):
        await client.execute_json_statement("not valid sql")


@pytest.mark.asyncio
@respx.mock
async def test_canceled_statement_raises(warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    respx.mock.get(f"{host}/api/2.0/sql/warehouses/{warehouse_id}").mock(
        return_value=httpx.Response(200, json={"state": "RUNNING"})
    )
    respx.mock.post(f"{host}/api/2.0/sql/statements").mock(
        return_value=httpx.Response(200, json={"statement_id": "stmt-canceled", "status": {"state": "CANCELED"}})
    )
    client = DatabricksClient(host, warehouse_id, token="test-token")

    with pytest.raises(RuntimeError, match="canceled"):
        await client.execute_json_statement("SELECT 1")


@pytest.mark.asyncio
@respx.mock
async def test_transient_5xx_is_retried_then_succeeds(warehouse_host_id, chunk_bytes_builder):
    """A 503 on the warehouse-status check should be retried (see
    _is_transient_error), not surfaced to the caller, as long as a later
    attempt succeeds within the retry budget."""
    host, warehouse_id = warehouse_host_id
    respx.mock.get(f"{host}/api/2.0/sql/warehouses/{warehouse_id}").mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(200, json={"state": "RUNNING"}),
        ]
    )
    statement_id = "stmt-ok"
    respx.mock.post(f"{host}/api/2.0/sql/statements").mock(
        return_value=httpx.Response(
            200,
            json={
                "statement_id": statement_id,
                "status": {"state": "SUCCEEDED"},
                "manifest": {"chunks": [{"chunk_index": 0, "row_count": 3}]},
            },
        )
    )
    respx.mock.get(url__regex=rf"{host}/api/2\.0/sql/statements/{statement_id}/result/chunks/\d+").mock(
        return_value=httpx.Response(200, json={"external_links": [{"external_link": f"{host}/_data/chunk-0"}]})
    )
    respx.mock.get(f"{host}/_data/chunk-0").mock(return_value=httpx.Response(200, content=chunk_bytes_builder(0, 3)))

    client = DatabricksClient(host, warehouse_id, token="test-token")
    statement_id_returned, manifest = await client.execute_arrow_statement("SELECT * FROM whatever")

    assert statement_id_returned == statement_id
    assert manifest["chunks"] == [{"chunk_index": 0, "row_count": 3}]


@pytest.mark.asyncio
@respx.mock
async def test_permanent_4xx_is_not_retried(warehouse_host_id):
    """A permanent 404 (e.g. a typo'd host/path) should fail immediately, not
    burn through the whole retry budget the way a transient 401/403/429/5xx
    does (see _is_transient_error)."""
    host, warehouse_id = warehouse_host_id
    route = respx.mock.get(f"{host}/api/2.0/sql/warehouses/{warehouse_id}").mock(return_value=httpx.Response(404))
    client = DatabricksClient(host, warehouse_id, token="test-token")

    with pytest.raises(httpx.HTTPStatusError):
        await client.execute_json_statement("SELECT 1")
    assert route.call_count == 1
