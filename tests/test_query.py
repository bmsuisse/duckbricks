from __future__ import annotations

import json

import pytest
import respx

from duckbricks import HEARTBEAT, DatabricksClient, run_query, run_query_streamed, stream_query_json


@pytest.mark.asyncio
@respx.mock
async def test_run_query_returns_all_rows_in_order(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=3, rows_per_chunk=10)
    client = DatabricksClient(host, warehouse_id, token="test-token")

    result = await run_query(client, "SELECT * FROM whatever")

    assert [r[0] for r in result.rows] == list(range(30))
    assert result.columns == ["id", "label"]


@pytest.mark.asyncio
@respx.mock
async def test_stream_query_json_preserves_order_despite_out_of_order_chunks(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    # Chunks resolve in REVERSE completion order (see conftest's
    # reverse_arrival) -- this is the scenario that silently breaks ORDER BY
    # if a caller doesn't reorder before emitting.
    mock_warehouse(respx.mock, n_chunks=4, rows_per_chunk=5, reverse_arrival=True)
    client = DatabricksClient(host, warehouse_id, token="test-token")

    rows = [json.loads(row) async for row in stream_query_json(client, "SELECT * FROM whatever ORDER BY id")]

    assert [r["id"] for r in rows] == list(range(20))


@pytest.mark.asyncio
@respx.mock
async def test_run_query_streamed_emits_heartbeat_before_result(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=3)
    client = DatabricksClient(host, warehouse_id, token="test-token")

    items = [item async for item in run_query_streamed(client, "SELECT * FROM whatever", total_timeout_s=5)]

    result = items[-1]
    assert all(item is HEARTBEAT for item in items[:-1])
    assert len(result.rows) == 3


@pytest.mark.asyncio
@respx.mock
async def test_token_provider_sync_and_async(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=2)

    sync_client = DatabricksClient(host, warehouse_id, token_provider=lambda: "sync-token")
    result = await run_query(sync_client, "SELECT * FROM whatever")
    assert len(result.rows) == 2

    async def async_provider() -> str:
        return "async-token"

    async_client = DatabricksClient(host, warehouse_id, token_provider=async_provider)
    result = await run_query(async_client, "SELECT * FROM whatever")
    assert len(result.rows) == 2


def test_requires_token_or_provider(warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    with pytest.raises(ValueError, match="token"):
        DatabricksClient(host, warehouse_id)
