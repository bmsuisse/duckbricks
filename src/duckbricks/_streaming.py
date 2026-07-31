"""Duckdb-free half of the query machinery: chunk fetching, heartbeats, and
`stream_query_json` -- everything that only needs an Arrow backend (see
_arrow_backend.py), not a real DuckDB engine. `query.py` builds on top of
this for the functions that genuinely need DuckDB (buffered `run_query`'s
union-of-chunks, `feed_select_to_duckdb_table`'s table materialization,
etc.) -- see its module docstring.

Splitting this out means a caller who only wants `stream_query_json` (e.g.
piping a Databricks query to a FastAPI SSE endpoint) can do so via the
`duckbricks[json]` extra (arro3-core + arro3-io, no duckdb at all) instead of
pulling in a full embedded database just to reshape Arrow bytes into JSON
lines."""

from __future__ import annotations

import asyncio
import contextlib
import io
from collections.abc import AsyncIterator, Awaitable
from typing import Any, TypeVar

from ._arrow_backend import parse_ipc_stream
from .client import DatabricksClient

__all__ = [
    "HEARTBEAT",
    "QueryTimeout",
    "ReplayableArrowChunk",
    "await_with_heartbeat",
    "fetch_arrow_chunks_for_statement",
    "fetch_arrow_chunks_with_manifest",
    "stream_query_json",
]

# How often a caller waiting on a slow Databricks round-trip (warehouse cold
# start, a long-running statement) gets a HEARTBEAT -- pick something well
# under whatever idle-connection ceiling sits between your server and its
# client (e.g. many PaaS load balancers cut an idle SSE connection around
# ~230s) if you're forwarding these as keep-alive pings.
_HEARTBEAT_INTERVAL_S = 15.0


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
    replacement scan / `from_arrow`, or to arro3's write_ndjson. Both call
    `__arrow_c_stream__` more than once per relation (a schema peek, then the
    actual scan) -- a plain parsed stream is single-use and raises on the
    second call (both the nanoarrow and arro3 backends -- see
    _arrow_backend.py), so this re-parses from the cached bytes every call
    instead. The bytes are already fully in memory (just downloaded), so
    re-parsing costs a cheap second pass, not a second network fetch."""

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


def _write_ndjson(chunk: ReplayableArrowChunk) -> bytes:
    try:
        import arro3.io as _arro3_io
    except ImportError as e:
        raise ImportError(
            "stream_query_json needs arro3 to serialize Arrow chunks to JSON -- install "
            "duckbricks[json] (arro3 only, no duckdb needed), or duckbricks[duckdb-arro3] if "
            "you also want duckdb's other functions. nanoarrow (duckbricks[duckdb]'s default) "
            "has no JSON writer of its own."
        ) from e
    buf = io.BytesIO()
    # explicit_nulls=True: arro3 omits null-valued keys by default, unlike
    # DuckDB's to_json (which always emits `"col":null`) -- without this a
    # row's JSON shape would vary by which columns happen to be null in it.
    _arro3_io.write_ndjson(chunk, buf, explicit_nulls=True)
    return buf.getvalue()


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
    ready-to-send JSON string (arro3's native `write_ndjson`, see
    _write_ndjson) -- one per SSE frame, say. Needs the `duckbricks[json]`
    extra (or any other extra that pulls in arro3, e.g. `duckdb-arro3`) --
    nanoarrow has no JSON writer, so a nanoarrow-only install raises a clear
    ImportError the first time this actually runs, not at import time.

    Unlike run_query/run_query_streamed, this registers and emits each
    Databricks chunk AS IT ARRIVES rather than buffering the full result
    first -- the first row reaches the caller after ~one chunk's fetch time,
    not the whole statement's, and at most a handful of chunks' bytes
    (bounded by the client's own chunk_fetch_concurrency) are ever held in
    memory at once, not O(whole result). Chunks can arrive out of order (see
    client.py), so out-of-order arrivals sit in a small `pending` buffer until
    the next expected chunk_index shows up -- that buffer stays bounded by
    concurrency, it never grows to the full result.

    Note this yields a whole chunk's rows at once (write_ndjson has no
    incremental/row-at-a-time mode), rather than DuckDB's old fetchmany-based
    sub-chunk batching -- Databricks' own chunk sizing already bounds how
    much that is, so the overall memory-bounded-across-chunks guarantee above
    still holds, just at chunk granularity rather than sub-chunk."""
    windowed_sql = _windowed_sql(sql, row_limit=row_limit, offset=offset)
    _statement_id, _manifest, chunk_iter = await fetch_arrow_chunks_with_manifest(
        client, windowed_sql, catalog=catalog, schema=schema, parameters=params
    )

    pending: dict[int, ReplayableArrowChunk] = {}
    next_idx = 0
    loop = asyncio.get_running_loop()
    async for item in _heartbeat_over_stream(chunk_iter, total_timeout_s=total_timeout_s):
        if item is HEARTBEAT:
            yield HEARTBEAT
            continue
        pending[item.chunk_index] = item
        while next_idx in pending:
            chunk = pending.pop(next_idx)
            blob = await loop.run_in_executor(None, _write_ndjson, chunk)
            for line in blob.splitlines():
                yield line.decode()
            next_idx += 1
