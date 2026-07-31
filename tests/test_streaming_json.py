"""Tests for _streaming.py's duckdb-free JSON serialization (_write_ndjson,
used by stream_query_json) -- value formatting and the no-arro3 error path.
End-to-end stream_query_json behavior (ordering, heartbeats) is already
covered via the mocked-warehouse tests in test_query.py."""

from __future__ import annotations

import io
import json
import sys

import duckdb
import pytest

from duckbricks._arrow_backend import write_ipc_stream
from duckbricks._streaming import ReplayableArrowChunk, _write_ndjson


def _build_chunk() -> ReplayableArrowChunk:
    con = duckdb.connect(":memory:")
    try:
        rel = con.sql("""
            SELECT
                i AS id,
                (i * 1.5)::DECIMAL(18,2) AS amount,
                DATE '2026-07-31' AS d,
                NULL::VARCHAR AS note
            FROM range(0, 3) t(i)
        """)
        buf = io.BytesIO()
        write_ipc_stream(rel, buf)
        return ReplayableArrowChunk(buf.getvalue(), chunk_index=0, declared_row_count=3)
    finally:
        con.close()


def test_write_ndjson_produces_one_json_object_per_row():
    lines = _write_ndjson(_build_chunk()).splitlines()

    assert len(lines) == 3
    rows = [json.loads(line) for line in lines]
    assert [r["id"] for r in rows] == [0, 1, 2]
    assert rows[1]["amount"] == 1.5
    assert rows[0]["d"] == "2026-07-31"
    assert rows[0]["note"] is None


def test_write_ndjson_raises_clear_error_without_arro3(monkeypatch):
    monkeypatch.setitem(sys.modules, "arro3.io", None)

    with pytest.raises(ImportError, match="duckbricks\\[json\\]"):
        _write_ndjson(_build_chunk())
