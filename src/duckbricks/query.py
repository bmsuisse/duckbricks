"""Runs SQL against a Databricks SQL warehouse (via DatabricksClient) and
serializes the Arrow-IPC result chunks through DuckDB -- used purely as a
thin Arrow -> JSON/rows/Arrow-bytes converter here, not a query engine; all
joins/filtering/ordering happen in the SQL submitted to Databricks. Requires
the `duckdb` extra (`pip install duckbricks[duckdb]`).
"""

from __future__ import annotations

import asyncio
import contextlib
import io
from collections.abc import AsyncIterator, Awaitable
from dataclasses import dataclass
from typing import Any, TypeVar

import duckdb
from nanoarrow.ipc import InputStream, StreamWriter

from .client import DatabricksClient

__all__ = [
    "HEARTBEAT",
    "QueryResult",
    "QueryTimeout",
    "ReplayableArrowChunk",
    "await_with_heartbeat",
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
    nanoarrow InputStream is single-use and raises "no longer valid" on the
    second call, so this re-parses from the cached bytes every call instead.
    The bytes are already fully in memory (just downloaded), so re-parsing
    costs a cheap second pass, not a second network fetch."""

    __slots__ = ("_data", "chunk_index", "declared_row_count")

    def __init__(self, data: bytes, chunk_index: int, declared_row_count: int | None = None) -> None:
        self._data = data
        self.chunk_index = chunk_index
        self.declared_row_count = declared_row_count

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        return InputStream.from_readable(io.BytesIO(self._data)).__arrow_c_stream__(requested_schema)

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
        with StreamWriter.from_writable(buf) as writer:
            writer.write_stream(rel)
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
