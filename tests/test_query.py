from __future__ import annotations

import json

import duckdb
import pytest
import respx

from duckbricks import (
    HEARTBEAT,
    DatabricksClient,
    feed_select_to_duckdb_table,
    run_query,
    run_query_streamed,
    stream_query_json,
)


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


@pytest.mark.asyncio
@respx.mock
async def test_feed_select_to_duckdb_table_preserves_order_despite_out_of_order_chunks(
    mock_warehouse, warehouse_host_id
):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=4, rows_per_chunk=5, reverse_arrival=True)
    client = DatabricksClient(host, warehouse_id, token="test-token")
    con = duckdb.connect(":memory:")

    rows_written = await feed_select_to_duckdb_table(client, "SELECT * FROM whatever ORDER BY id", con, "mart")

    assert rows_written == 20
    assert [r[0] for r in con.execute("SELECT id FROM mart").fetchall()] == list(range(20))


@pytest.mark.asyncio
@respx.mock
async def test_feed_select_to_duckdb_table_if_exists_replace_drops_old_rows(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=3)
    client = DatabricksClient(host, warehouse_id, token="test-token")
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE mart (id INTEGER, label VARCHAR)")
    con.execute("INSERT INTO mart VALUES (999, 'stale')")

    rows_written = await feed_select_to_duckdb_table(client, "SELECT * FROM whatever", con, "mart")

    assert rows_written == 3
    assert con.execute("SELECT COUNT(*) FROM mart").fetchall()[0][0] == 3


@pytest.mark.asyncio
@respx.mock
async def test_feed_select_to_duckdb_table_if_exists_append(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=3)
    client = DatabricksClient(host, warehouse_id, token="test-token")
    con = duckdb.connect(":memory:")
    await feed_select_to_duckdb_table(client, "SELECT * FROM whatever", con, "mart")

    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=3)
    rows_written = await feed_select_to_duckdb_table(client, "SELECT * FROM whatever", con, "mart", if_exists="append")

    assert rows_written == 3
    assert con.execute("SELECT COUNT(*) FROM mart").fetchall()[0][0] == 6


@pytest.mark.asyncio
@respx.mock
async def test_feed_select_to_duckdb_table_if_exists_fail_raises_when_table_present(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    mock_warehouse(respx.mock, n_chunks=1, rows_per_chunk=1)
    client = DatabricksClient(host, warehouse_id, token="test-token")
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE mart (id INTEGER)")

    with pytest.raises(ValueError, match="already exists"):
        await feed_select_to_duckdb_table(client, "SELECT * FROM whatever", con, "mart", if_exists="fail")


@pytest.mark.asyncio
@respx.mock
async def test_feed_select_to_duckdb_table_rejects_bad_if_exists(mock_warehouse, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    client = DatabricksClient(*warehouse_host_id, token="test-token")
    con = duckdb.connect(":memory:")

    with pytest.raises(ValueError, match="if_exists"):
        await feed_select_to_duckdb_table(client, "SELECT 1", con, "mart", if_exists="bogus")
