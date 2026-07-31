from __future__ import annotations

import json
import re

import duckdb
import httpx
import pytest
import respx

from duckbricks import (
    HEARTBEAT,
    DatabricksClient,
    feed_duckdb_table_to_databricks,
    feed_select_to_duckdb_table,
    run_query,
    run_query_streamed,
    stream_query_json,
)


def _install_json_statements(
    router: respx.Router,
    host: str,
    warehouse_id: str,
    statements: list[tuple[str, list[dict], list[list]]],
) -> respx.Route:
    """Mocks the Databricks-side statement(s) feed_duckdb_table_to_databricks
    submits after staging (COPY INTO / CREATE OR REPLACE TABLE AS SELECT /
    the follow-up SELECT COUNT(*) for "replace" mode) -- one JSON_ARRAY
    result per call, consumed in submission order. `statements` is a list of
    (substring expected in the submitted SQL, manifest schema columns, rows)
    -- rows may be empty for a DDL statement with nothing to fetch."""
    router.get(f"{host}/api/2.0/sql/warehouses/{warehouse_id}").mock(
        return_value=httpx.Response(200, json={"state": "RUNNING"})
    )

    remaining = list(statements)
    rows_by_statement: dict[str, list[list]] = {}

    def submit(request: httpx.Request) -> httpx.Response:
        sql = json.loads(request.content)["statement"]
        substring, columns, rows = remaining.pop(0)
        assert substring in sql, f"expected {substring!r} in submitted SQL: {sql!r}"
        statement_id = f"stmt-{len(rows_by_statement)}"
        rows_by_statement[statement_id] = rows
        manifest = {
            "schema": {"columns": columns},
            "chunks": [{"chunk_index": 0, "row_count": len(rows)}] if rows else [],
        }
        return httpx.Response(
            200, json={"statement_id": statement_id, "status": {"state": "SUCCEEDED"}, "manifest": manifest}
        )

    submit_route = router.post(f"{host}/api/2.0/sql/statements").mock(side_effect=submit)

    def resolve_chunk(request: httpx.Request) -> httpx.Response:
        match = re.search(r"/statements/([^/]+)/result/chunks/", str(request.url))
        assert match is not None
        return httpx.Response(
            200, json={"external_links": [{"external_link": f"{host}/_data/{match.group(1)}-chunk-0"}]}
        )

    router.get(url__regex=rf"{host}/api/2\.0/sql/statements/[^/]+/result/chunks/\d+").mock(side_effect=resolve_chunk)

    def serve_chunk(request: httpx.Request) -> httpx.Response:
        match = re.search(r"/_data/([^/]+)-chunk-\d+", str(request.url))
        assert match is not None
        return httpx.Response(200, content=json.dumps(rows_by_statement[match.group(1)]).encode())

    router.get(url__regex=rf"{host}/_data/.+-chunk-\d+").mock(side_effect=serve_chunk)

    return submit_route


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


@pytest.mark.asyncio
@respx.mock
async def test_feed_duckdb_table_to_databricks_append_writes_via_copy_into(mock_volume_files, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    put_route, delete_route = mock_volume_files(respx.mock, host)
    _install_json_statements(
        respx.mock,
        host,
        warehouse_id,
        [
            (
                "COPY INTO",
                [{"name": "num_affected_rows", "position": 0}, {"name": "num_inserted_rows", "position": 1}],
                [["3", "3"]],
            )
        ],
    )
    client = DatabricksClient(host, warehouse_id, token="test-token")
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE src AS SELECT * FROM range(3) t(id)")

    rows_written = await feed_duckdb_table_to_databricks(
        client, con, "SELECT * FROM src", "my_catalog.my_schema.tbl", staging_volume="/Volumes/my_catalog/my_schema/vol"
    )

    assert rows_written == 3
    assert put_route.call_count == 1
    uploaded_path = put_route.calls.last.request.url.path
    assert uploaded_path.startswith("/api/2.0/fs/files/Volumes/my_catalog/my_schema/vol/_duckbricks_")
    assert delete_route.call_count == 1  # staged file cleaned up after a successful load


@pytest.mark.asyncio
@respx.mock
async def test_feed_duckdb_table_to_databricks_replace_uses_ctas_then_counts(mock_volume_files, warehouse_host_id):
    host, warehouse_id = warehouse_host_id
    _put_route, delete_route = mock_volume_files(respx.mock, host)
    _install_json_statements(
        respx.mock,
        host,
        warehouse_id,
        [
            ("CREATE OR REPLACE TABLE", [], []),
            ("SELECT COUNT(*)", [{"name": "count_star()", "position": 0}], [["5"]]),
        ],
    )
    client = DatabricksClient(host, warehouse_id, token="test-token")
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE src AS SELECT * FROM range(5) t(id)")

    rows_written = await feed_duckdb_table_to_databricks(
        client,
        con,
        "SELECT * FROM src",
        "my_catalog.my_schema.tbl",
        staging_volume="/Volumes/my_catalog/my_schema/vol",
        mode="replace",
    )

    assert rows_written == 5
    assert delete_route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_feed_duckdb_table_to_databricks_rejects_bad_mode(warehouse_host_id):
    client = DatabricksClient(*warehouse_host_id, token="test-token")
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE src AS SELECT * FROM range(1) t(id)")

    with pytest.raises(ValueError, match="mode"):
        await feed_duckdb_table_to_databricks(
            client,
            con,
            "SELECT * FROM src",
            "my_catalog.my_schema.tbl",
            staging_volume="/Volumes/my_catalog/my_schema/vol",
            mode="bogus",
        )
