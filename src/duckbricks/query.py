"""Runs SQL against a Databricks SQL warehouse (via DatabricksClient) and
serializes the Arrow-IPC result chunks through DuckDB -- used purely as a
thin Arrow -> JSON/rows/Arrow-bytes converter here, not a query engine; all
joins/filtering/ordering happen in the SQL submitted to Databricks. Requires
the `duckdb` extra (`pip install duckbricks[duckdb]`) or its `duckdb-arro3`
alternative (see _arrow_backend.py).
"""

from __future__ import annotations

import asyncio
import contextlib
import io
import json
import os
import tempfile
from collections.abc import AsyncIterator, Awaitable
from dataclasses import dataclass
from typing import Any, TypeVar
from uuid import uuid4

import duckdb

from ._arrow_backend import parse_ipc_stream, write_ipc_stream
from .client import DatabricksClient

__all__ = [
    "HEARTBEAT",
    "QueryResult",
    "QueryTimeout",
    "ReplayableArrowChunk",
    "await_with_heartbeat",
    "feed_duckdb_table_to_databricks",
    "feed_select_to_duckdb_table",
    "run_query",
    "run_query_streamed",
    "stream_query_json",
]

# How often a caller waiting on a slow Databricks round-trip (warehouse cold
# start, a long-running statement) gets a HEARTBEAT -- pick something well
# under whatever idle-connection ceiling sits between your server and its
# client (e.g. many PaaS load balancers cut an idle SSE connection around
# ~230s) if you're forwarding these as keep-alive pings.
_HEARTBEAT_INTERVAL_S = 15.0

# Rows-per-fetchmany() call when streaming JSON out of DuckDB: small enough
# that the first byte reaches the caller quickly, large enough that per-batch
# executor-hop overhead stays a rounding error.
_STREAM_FETCH_CHUNK_ROWS = 1_000


class QueryTimeout(RuntimeError):
    """Raised when a query exceeds its `total_timeout_s`."""


class _Heartbeat:
    __slots__ = ()

    def __repr__(self) -> str:
        return "HEARTBEAT"


HEARTBEAT = _Heartbeat()

T = TypeVar("T")


class ReplayableArrowChunk:
    """Wraps one Arrow IPC-stream byte chunk so it can be handed to DuckDB's
    replacement scan / `from_arrow`. DuckDB calls `__arrow_c_stream__` more
    than once per relation (a schema peek, then the actual scan) -- a plain
    parsed stream is single-use and raises on the second call (both the
    nanoarrow and arro3 backends -- see _arrow_backend.py), so this re-parses
    from the cached bytes every call instead. The bytes are already fully in
    memory (just downloaded), so re-parsing costs a cheap second pass, not a
    second network fetch."""

    __slots__ = ("_data", "chunk_index", "declared_row_count")

    def __init__(self, data: bytes, chunk_index: int, declared_row_count: int | None = None) -> None:
        self._data = data
        self.chunk_index = chunk_index
        self.declared_row_count = declared_row_count

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        return parse_ipc_stream(self._data).__arrow_c_stream__(requested_schema)

    def nbytes(self) -> int:
        return len(self._data)


async def fetch_arrow_chunks_with_manifest(
    client: DatabricksClient,
    sql: str,
    *,
    catalog: str | None = None,
    schema: str | None = None,
    parameters: list[dict[str, Any]] | None = None,
) -> tuple[str, dict[str, Any], AsyncIterator[ReplayableArrowChunk]]:
    """Submits `sql` and returns (statement_id, manifest, chunk_iterator).
    total_row_count/total_chunk_count are already in the manifest once the
    statement succeeds, so callers needing a progress-bar target don't need a
    separate preflight COUNT(*)."""
    statement_id, manifest = await client.execute_arrow_statement(
        sql, catalog=catalog, schema=schema, parameters=parameters
    )
    chunk_metas = manifest.get("chunks") or []
    return statement_id, manifest, fetch_arrow_chunks_for_statement(client, statement_id, chunk_metas)


async def fetch_arrow_chunks_for_statement(
    client: DatabricksClient, statement_id: str, chunk_metas: list[dict[str, Any]]
) -> AsyncIterator[ReplayableArrowChunk]:
    async for chunk_bytes, row_count, chunk_index in client.stream_chunks_by_index(statement_id, chunk_metas):
        if chunk_bytes:
            yield ReplayableArrowChunk(chunk_bytes, chunk_index, declared_row_count=row_count)


async def await_with_heartbeat(
    aw: Awaitable[T], *, interval_s: float = _HEARTBEAT_INTERVAL_S, total_timeout_s: float | None = None
) -> AsyncIterator[Any]:
    """Wraps a single slow awaitable with periodic HEARTBEAT yields, so a
    caller streaming this over e.g. SSE never goes silent for the whole wait.
    Yields HEARTBEAT zero or more times, then yields the awaitable's real
    result exactly once. Re-raises whatever `aw` raised, or QueryTimeout if
    `total_timeout_s` elapses first."""
    task: asyncio.Task[T] = asyncio.ensure_future(aw)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + total_timeout_s if total_timeout_s is not None else None
    try:
        while not task.done():
            wait_for = interval_s if deadline is None else min(interval_s, max(deadline - loop.time(), 0.0))
            done, _ = await asyncio.wait({task}, timeout=wait_for)
            if not done:
                if deadline is not None and loop.time() >= deadline:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                    raise QueryTimeout(f"Query exceeded {total_timeout_s}s timeout")
                yield HEARTBEAT
        yield await task
    finally:
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


async def _heartbeat_over_stream(
    aiter: AsyncIterator[T], *, interval_s: float = _HEARTBEAT_INTERVAL_S, total_timeout_s: float | None = None
) -> AsyncIterator[Any]:
    """Like await_with_heartbeat, but for a stream of items rather than one
    final result: yields HEARTBEAT whenever the wait for the *next* item
    exceeds interval_s, and otherwise passes each item through as it arrives.
    Used by stream_query_json so a slow chunk mid-stream -- not just a cold
    warehouse start before the first one -- never lets a downstream SSE
    connection go silent for the whole wait."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + total_timeout_s if total_timeout_s is not None else None
    it = aiter.__aiter__()
    task: asyncio.Task[Any] | None = None
    try:
        while True:
            if task is None:
                task = asyncio.ensure_future(it.__anext__())
            wait_for = interval_s if deadline is None else min(interval_s, max(deadline - loop.time(), 0.0))
            done, _ = await asyncio.wait({task}, timeout=wait_for)
            if not done:
                if deadline is not None and loop.time() >= deadline:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                    raise QueryTimeout(f"Query exceeded {total_timeout_s}s timeout")
                yield HEARTBEAT
                continue
            try:
                yield task.result()
            except StopAsyncIteration:
                return
            finally:
                task = None
    finally:
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


@dataclass(slots=True)
class QueryResult:
    columns: list[str]
    rows: list[tuple]

    def dicts(self) -> list[dict[str, Any]]:
        return [dict(zip(self.columns, r, strict=True)) for r in self.rows]


def _windowed_sql(sql: str, *, row_limit: int | None, offset: int | None) -> str:
    """Pushes LIMIT/OFFSET into the SQL submitted to Databricks -- a query
    should never fetch more rows from the warehouse than the caller wants."""
    if row_limit is None and not offset:
        return sql
    if row_limit is None:
        return f"SELECT * FROM ({sql}) _q OFFSET {offset}"  # noqa: S608
    if offset:
        return f"SELECT * FROM ({sql}) _q LIMIT {row_limit} OFFSET {offset}"  # noqa: S608
    return f"SELECT * FROM ({sql}) _q LIMIT {row_limit}"  # noqa: S608


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _register_chunks(con: duckdb.DuckDBPyConnection, chunks: list[ReplayableArrowChunk]) -> str:
    """Registers each already-fetched Arrow chunk as its own DuckDB relation
    and returns a UNION ALL selecting across all of them, in list order."""
    for i, chunk in enumerate(chunks):
        con.register(f"_chunk_{i}", chunk)
    return " UNION ALL ".join(f"SELECT * FROM _chunk_{i}" for i in range(len(chunks)))  # noqa: S608 -- chunk indices only


async def _fetch_chunks(
    client: DatabricksClient,
    windowed_sql: str,
    params: list[dict[str, Any]] | None,
    catalog: str | None,
    schema: str | None,
) -> list[ReplayableArrowChunk]:
    _statement_id, _manifest, chunk_iter = await fetch_arrow_chunks_with_manifest(
        client, windowed_sql, catalog=catalog, schema=schema, parameters=params
    )
    # DatabricksClient fetches with bounded concurrency and yields chunks in
    # COMPLETION order, not chunk_index order (see client.py's
    # _fetch_chunks_with_backpressure). A query's own SQL can carry an ORDER
    # BY, so _register_chunks' UNION ALL below must see chunks in
    # chunk_index order to preserve it.
    return sorted([chunk async for chunk in chunk_iter], key=lambda c: c.chunk_index)


def _to_result(chunks: list[ReplayableArrowChunk]) -> QueryResult:
    if not chunks:
        return QueryResult(columns=[], rows=[])
    con = duckdb.connect(":memory:")
    try:
        union_sql = _register_chunks(con, chunks)
        cur = con.execute(union_sql)  # noqa: S608 -- union_sql only ever selects from just-registered chunk relations
        columns = [d[0] for d in cur.description or []]
        return QueryResult(columns=columns, rows=cur.fetchall())
    finally:
        con.close()


def _to_arrow_bytes(chunks: list[ReplayableArrowChunk]) -> bytes:
    if not chunks:
        # No real schema is available here -- Databricks never sent any
        # chunks, so there's nothing to reflect the actual result's column
        # types. Returning empty bytes rather than fabricating a fake schema
        # avoids lying to callers about what columns an empty result has.
        return b""
    con = duckdb.connect(":memory:")
    try:
        sql = _register_chunks(con, chunks)
        rel = con.sql(sql)  # noqa: S608
        buf = io.BytesIO()
        write_ipc_stream(rel, buf)
        return buf.getvalue()
    finally:
        con.close()


async def _execute(
    client: DatabricksClient,
    windowed_sql: str,
    params: list[dict[str, Any]] | None,
    catalog: str | None,
    schema: str | None,
    *,
    as_arrow: bool,
) -> QueryResult | bytes:
    chunks = await _fetch_chunks(client, windowed_sql, params, catalog, schema)
    return _to_arrow_bytes(chunks) if as_arrow else _to_result(chunks)


async def run_query(
    client: DatabricksClient,
    sql: str,
    *,
    params: list[dict[str, Any]] | None = None,
    row_limit: int | None = None,
    offset: int | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    total_timeout_s: float | None = None,
) -> QueryResult:
    """Runs `sql` against the Databricks SQL warehouse (joins/aggregation/
    filtering all computed there) and returns the full result. `params`, if
    given, is Databricks' own named-parameter format --
    [{"name": ..., "value": ..., "type": ...}] bound against `:name` markers
    in `sql` -- not DuckDB's `?` positional style."""
    windowed_sql = _windowed_sql(sql, row_limit=row_limit, offset=offset)
    result: QueryResult | None = None
    async for item in await_with_heartbeat(
        _execute(client, windowed_sql, params, catalog, schema, as_arrow=False), total_timeout_s=total_timeout_s
    ):
        if item is not HEARTBEAT:
            result = item
    assert result is not None  # noqa: S101 -- await_with_heartbeat always yields a real result last
    return result


async def run_query_streamed(
    client: DatabricksClient,
    sql: str,
    *,
    params: list[dict[str, Any]] | None = None,
    row_limit: int | None = None,
    offset: int | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    as_arrow: bool = False,
    total_timeout_s: float | None = None,
) -> AsyncIterator[Any]:
    """Like run_query, but yields HEARTBEAT while waiting on Databricks and
    then either that same HEARTBEAT sentinel or the final QueryResult/bytes --
    for a caller that wants ONE result but must keep e.g. an SSE connection
    alive during a possible multi-minute cold start."""
    windowed_sql = _windowed_sql(sql, row_limit=row_limit, offset=offset)
    async for item in await_with_heartbeat(
        _execute(client, windowed_sql, params, catalog, schema, as_arrow=as_arrow), total_timeout_s=total_timeout_s
    ):
        yield item


async def stream_query_json(
    client: DatabricksClient,
    sql: str,
    *,
    params: list[dict[str, Any]] | None = None,
    row_limit: int | None = None,
    offset: int | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    total_timeout_s: float | None = None,
) -> AsyncIterator[Any]:
    """Yields HEARTBEAT while waiting on Databricks, then each result row as a
    ready-to-send JSON string (DuckDB's native to_json(row)) -- one per SSE
    frame, say.

    Unlike run_query/run_query_streamed, this registers and emits each
    Databricks chunk AS IT ARRIVES rather than buffering the full result
    first -- the first row reaches the caller after ~one chunk's fetch time,
    not the whole statement's, and at most a handful of chunks' bytes
    (bounded by the client's own chunk_fetch_concurrency) are ever held in
    memory at once, not O(whole result). Chunks can arrive out of order (see
    client.py), so out-of-order arrivals sit in a small `pending` buffer until
    the next expected chunk_index shows up -- that buffer stays bounded by
    concurrency, it never grows to the full result."""
    windowed_sql = _windowed_sql(sql, row_limit=row_limit, offset=offset)
    _statement_id, _manifest, chunk_iter = await fetch_arrow_chunks_with_manifest(
        client, windowed_sql, catalog=catalog, schema=schema, parameters=params
    )

    pending: dict[int, ReplayableArrowChunk] = {}
    next_idx = 0
    loop = asyncio.get_running_loop()
    con = duckdb.connect(":memory:")
    try:
        async for item in _heartbeat_over_stream(chunk_iter, total_timeout_s=total_timeout_s):
            if item is HEARTBEAT:
                yield HEARTBEAT
                continue
            pending[item.chunk_index] = item
            while next_idx in pending:
                con.register("_c", pending.pop(next_idx))
                cur = con.execute("SELECT to_json(_j) FROM _c _j")  # noqa: S608
                while True:
                    batch = await loop.run_in_executor(None, cur.fetchmany, _STREAM_FETCH_CHUNK_ROWS)
                    if not batch:
                        break
                    for row in batch:
                        yield row[0]
                con.unregister("_c")
                next_idx += 1
    finally:
        con.close()


async def feed_select_to_duckdb_table(
    client: DatabricksClient,
    sql: str,
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    *,
    params: list[dict[str, Any]] | None = None,
    row_limit: int | None = None,
    offset: int | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    if_exists: str = "replace",
    total_timeout_s: float | None = None,
) -> int:
    """Streams `sql`'s result straight into `table_name` on `con` -- a DuckDB
    connection you already have, in-memory or a persistent
    `duckdb.connect("some.duckdb")` -- as Arrow chunks arrive from Databricks,
    the same reorder-buffered chunk-at-a-time approach as stream_query_json
    (see its docstring), instead of buffering the whole result in Python
    first. `con` is left open and `table_name` a real, independently
    queryable table afterwards -- this function's only job is getting the
    data there.

    `if_exists` is "replace" (default -- drops/recreates `table_name`),
    "append" (table must already exist with a compatible schema), or "fail"
    (raise if it already exists). Returns the number of rows written.

    If the query itself returns zero rows, no table is created/touched --
    without at least one chunk there's no real schema to create an empty
    table from (same reasoning as _to_arrow_bytes's empty-result case)."""
    if if_exists not in ("replace", "append", "fail"):
        raise ValueError(f"if_exists must be 'replace', 'append', or 'fail', got {if_exists!r}")

    quoted = _quote_ident(table_name)
    if if_exists == "fail":
        exists = con.execute("SELECT 1 FROM information_schema.tables WHERE table_name = ?", [table_name]).fetchone()
        if exists:
            raise ValueError(f"table {table_name!r} already exists (if_exists='fail')")

    windowed_sql = _windowed_sql(sql, row_limit=row_limit, offset=offset)
    _statement_id, _manifest, chunk_iter = await fetch_arrow_chunks_with_manifest(
        client, windowed_sql, catalog=catalog, schema=schema, parameters=params
    )

    pending: dict[int, ReplayableArrowChunk] = {}
    next_idx = 0
    table_ready = if_exists == "append"
    rows_written = 0
    async for item in _heartbeat_over_stream(chunk_iter, total_timeout_s=total_timeout_s):
        if item is HEARTBEAT:
            continue
        pending[item.chunk_index] = item
        while next_idx in pending:
            con.register("_c", pending.pop(next_idx))
            try:
                if not table_ready:
                    cur = con.execute(f"CREATE OR REPLACE TABLE {quoted} AS SELECT * FROM _c")  # noqa: S608
                    table_ready = True
                else:
                    cur = con.execute(f"INSERT INTO {quoted} SELECT * FROM _c")  # noqa: S608
                row = cur.fetchone()
                assert row is not None  # noqa: S101 -- CREATE TABLE AS/INSERT INTO always return one Count row
                rows_written += row[0]
            finally:
                con.unregister("_c")
            next_idx += 1
    return rows_written


async def _execute_json_with_timeout(
    client: DatabricksClient, statement: str, *, total_timeout_s: float | None
) -> tuple[str, dict[str, Any]]:
    """Runs one JSON_ARRAY statement to completion, same
    await_with_heartbeat/total_timeout_s pattern run_query uses for its own
    single awaitable -- there's no stream of intermediate items to hand back
    here (this is a short-lived DDL/COPY call, not a big result fetch), just
    the timeout enforcement."""
    result: tuple[str, dict[str, Any]] | None = None
    async for item in await_with_heartbeat(client.execute_json_statement(statement), total_timeout_s=total_timeout_s):
        if item is not HEARTBEAT:
            result = item
    assert result is not None  # noqa: S101 -- await_with_heartbeat always yields a real result last
    return result


async def _fetch_json_rows(client: DatabricksClient, statement_id: str, manifest: dict[str, Any]) -> list[list[Any]]:
    """Fetches every chunk of a JSON_ARRAY statement's result and flattens
    them in chunk_index order -- chunks can complete out of order over the
    network (see client.py), same reasoning as _fetch_chunks' sort on the
    Arrow path. Returns an empty list for a statement with no result rows
    (e.g. a DDL statement)."""
    chunk_metas = manifest.get("chunks") or []
    if not chunk_metas:
        return []
    chunks = [
        (chunk_index, json.loads(blob))
        async for blob, _row_count, chunk_index in client.stream_chunks_by_index(statement_id, chunk_metas)
    ]
    rows: list[list[Any]] = []
    for _chunk_index, chunk_rows in sorted(chunks, key=lambda c: c[0]):
        rows.extend(chunk_rows)
    return rows


def _column_value(manifest: dict[str, Any], row: list[Any], column: str) -> Any:
    """Looks up `column` by name in a JSON_ARRAY statement's manifest schema
    and returns that position's value out of `row`."""
    columns = manifest.get("schema", {}).get("columns") or []
    for i, col in enumerate(columns):
        if col.get("name") == column:
            return row[col.get("position", i)]
    raise KeyError(f"column {column!r} not found in statement result")


async def feed_duckdb_table_to_databricks(
    client: DatabricksClient,
    con: duckdb.DuckDBPyConnection,
    source_sql: str,
    target_table: str,
    *,
    staging_volume: str,
    mode: str = "append",
    total_timeout_s: float | None = None,
) -> int:
    """The reverse of feed_select_to_duckdb_table -- writes `source_sql`'s
    result (any SELECT `con` can run) up to Databricks as `target_table`,
    instead of pulling a Databricks query down into DuckDB.

    `source_sql`'s result is written to a local Parquet file via DuckDB's own
    `COPY ... TO ... (FORMAT PARQUET)` (in a temp directory, cleaned up once
    uploaded), uploaded to a fresh, unique subpath under `staging_volume` -- a
    fully-qualified Unity Catalog volume path, e.g.
    `/Volumes/my_catalog/my_schema/my_volume`, caller-supplied in full like
    everywhere else in this package (see AGENTS.md) -- and loaded into
    `target_table` (also fully-qualified, e.g. `my_catalog.my_schema.customers`)
    with a Databricks-side SQL statement.

    `mode` is "append" (default -- Databricks `COPY INTO`, which tracks
    already-loaded files internally, so re-running against the same staged
    files is a no-op rather than a duplicate load) or "replace"
    (`CREATE OR REPLACE TABLE ... AS SELECT`, fully replacing `target_table`).
    Returns the number of rows written -- for "append" that's `COPY INTO`'s
    own `num_inserted_rows`; "replace" doesn't get a row count from `CREATE
    TABLE AS` the same way, so it follows up with a `SELECT COUNT(*)`.

    Every uploaded staging file is deleted afterwards regardless of outcome
    (best-effort -- a cleanup failure never masks a successful load or
    replaces whatever exception the load itself raised)."""
    if mode not in ("append", "replace"):
        raise ValueError(f"mode must be 'append' or 'replace', got {mode!r}")

    stage_path = f"{staging_volume.rstrip('/')}/_duckbricks_{uuid4()}"
    uploaded_paths: list[str] = []
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_path = os.path.join(tmp_dir, "data.parquet")
            con.execute(f"COPY ({source_sql}) TO '{local_path}' (FORMAT PARQUET)")  # noqa: S608
            for name in sorted(os.listdir(tmp_dir)):
                with open(os.path.join(tmp_dir, name), "rb") as f:
                    data = f.read()
                volume_path = f"{stage_path}/{name}"
                await client.upload_volume_file(volume_path, data)
                uploaded_paths.append(volume_path)

        if mode == "replace":
            await _execute_json_with_timeout(
                client,
                f"CREATE OR REPLACE TABLE {target_table} AS "  # noqa: S608
                f"SELECT * FROM read_files('{stage_path}', format => 'parquet')",
                total_timeout_s=total_timeout_s,
            )
            statement_id, manifest = await _execute_json_with_timeout(
                client,
                f"SELECT COUNT(*) FROM {target_table}",  # noqa: S608
                total_timeout_s=total_timeout_s,
            )
            rows = await _fetch_json_rows(client, statement_id, manifest)
            return int(rows[0][0]) if rows else 0

        statement_id, manifest = await _execute_json_with_timeout(
            client,
            f"COPY INTO {target_table} FROM '{stage_path}' FILEFORMAT = PARQUET",  # noqa: S608
            total_timeout_s=total_timeout_s,
        )
        rows = await _fetch_json_rows(client, statement_id, manifest)
        if not rows:
            return 0
        return int(_column_value(manifest, rows[0], "num_inserted_rows"))
    finally:
        for volume_path in uploaded_paths:
            # Best-effort cleanup -- a failure here never masks a successful
            # load's result or replaces whatever exception the load itself
            # raised (see the try/finally this sits in).
            with contextlib.suppress(Exception):
                await client.delete_volume_file(volume_path)
