"""Shared fixtures: a fake Databricks SQL warehouse mocked via respx, so the
suite runs with zero real network calls or credentials.
"""

from __future__ import annotations

import io

import duckdb
import httpx
import pytest
import respx
from nanoarrow.ipc import StreamWriter

HOST = "https://fake-workspace.cloud.databricks.com"
WAREHOUSE_ID = "wh-test-123"


def build_chunk_bytes(lo: int, hi: int) -> bytes:
    """Synthesizes one chunk's Arrow-IPC bytes via DuckDB + nanoarrow's
    StreamWriter -- the same approach query.py's _to_arrow_bytes uses -- so
    tests exercise the real Arrow-C-Data-Interface round trip DuckDB does in
    production, without a live Databricks connection."""
    con = duckdb.connect(":memory:")
    try:
        rel = con.sql(f"SELECT i AS id, 'row_' || i AS label FROM range({lo}, {hi}) t(i)")  # noqa: S608
        buf = io.BytesIO()
        with StreamWriter.from_writable(buf) as writer:
            writer.write_stream(rel)
        return buf.getvalue()
    finally:
        con.close()


@pytest.fixture
def warehouse_host_id() -> tuple[str, str]:
    return HOST, WAREHOUSE_ID


@pytest.fixture
def mock_warehouse():
    """Registers respx routes for a fake warehouse with `n_chunks` chunks of
    `rows_per_chunk` rows each (id column running 0..n_chunks*rows_per_chunk).
    Callers configure it via mock_warehouse(...) inside a `with respx.mock:`
    block (or use the `respx_router` param) -- see test_query.py."""

    def _install(router: respx.Router, n_chunks: int, rows_per_chunk: int, *, reverse_arrival: bool = False) -> None:
        statement_id = "stmt-abc"
        chunks = [{"chunk_index": i, "row_count": rows_per_chunk} for i in range(n_chunks)]
        chunk_bytes = {i: build_chunk_bytes(i * rows_per_chunk, (i + 1) * rows_per_chunk) for i in range(n_chunks)}

        router.get(f"{HOST}/api/2.0/sql/warehouses/{WAREHOUSE_ID}").mock(
            return_value=httpx.Response(200, json={"state": "RUNNING"})
        )
        router.post(f"{HOST}/api/2.0/sql/statements").mock(
            return_value=httpx.Response(
                200,
                json={
                    "statement_id": statement_id,
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {"chunks": chunks},
                },
            )
        )

        def resolve_chunk(request: httpx.Request) -> httpx.Response:
            idx = int(str(request.url).rsplit("/", 1)[-1])
            return httpx.Response(
                200, json={"external_links": [{"external_link": f"{HOST}/_data/chunk-{idx}"}]}
            )

        router.get(url__regex=rf"{HOST}/api/2\.0/sql/statements/{statement_id}/result/chunks/\d+").mock(
            side_effect=resolve_chunk
        )

        async def serve_chunk_bytes(request: httpx.Request) -> httpx.Response:
            idx = int(str(request.url).rsplit("-", 1)[-1])
            if reverse_arrival:
                import asyncio

                await asyncio.sleep((n_chunks - idx) * 0.01)
            return httpx.Response(200, content=chunk_bytes[idx])

        router.get(url__regex=rf"{HOST}/_data/chunk-\d+").mock(side_effect=serve_chunk_bytes)

    return _install
