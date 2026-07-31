# duckbricks

Runs SQL against a Databricks SQL warehouse via the Statement Execution API, streams the Arrow-IPC result chunks with backpressure into DuckDB for JSON/rows/Arrow serialization. See `README.md` for the user-facing API; this file is about working *on* the package.

## Layout

- `src/duckbricks/client.py` -- pure REST client (auth, statement submission/polling, backpressure-bounded concurrent chunk download). No duckdb/Arrow-backend dependency, intentionally: someone who only wants `execute_json_statement` shouldn't have to install them. Retries are a small hand-rolled `_retry_call` loop, not a dependency (see "Design invariants" below).
- `src/duckbricks/_streaming.py` -- the duckdb-free half: `ReplayableArrowChunk`, heartbeat helpers, chunk fetching, and `stream_query_json` (serializes via arro3's `write_ndjson`, not duckdb -- needs the `duckbricks[json]`/`duckdb-arro3` extra, raised as a clear `ImportError` at call time if missing). No `import duckdb` anywhere in this module, on purpose -- `__init__.py` imports it unconditionally.
- `src/duckbricks/query.py` -- the genuinely duckdb-dependent functions: `run_query`/`run_query_streamed` (buffered, unions multiple chunks via a DuckDB `UNION ALL`), `feed_select_to_duckdb_table`/`feed_duckdb_table_to_databricks` (real DuckDB table materialization, not just serialization). Requires the `duckdb` extra; re-exports `_streaming.py`'s names too for backwards compatibility. `__init__.py` imports this in a `try/except ImportError` so the package still works without it.
- `src/duckbricks/_arrow_backend.py` -- the pluggable Arrow IPC read/write backend: nanoarrow (`duckdb` extra, default) or arro3 (`duckdb-arro3` extra, e.g. for platforms where nanoarrow's wheel doesn't work), selected via try/except at import time. `set_arrow_backend()` lets a caller override with their own implementation instead. Unrelated to `_streaming.py`'s arro3 dependency, which is for JSON serialization specifically, not IPC parsing -- nanoarrow has no JSON writer of its own.
- `tests/` -- respx mocks the Databricks REST endpoints (warehouse status, statement submit, chunk-link resolution, external-link byte download); no real warehouse or credentials needed to run the suite.
- `examples/` -- `basic.py` (static token) and `azure_auth.py` (Azure AD `token_provider` via `azure-identity`, kept out of core deps on purpose -- see "Auth" below).

## Commands

```bash
uv sync --all-extras       # install everything, incl. dev group + duckdb/duckdb-arro3/json extras
uv run pytest -q
uv run ruff check .
uv run ty check src tests  # examples/azure_auth.py imports azure-identity, which is deliberately
                            # not a dependency -- don't widen ty's scope to include examples/
```

One-time setup per clone: `prek install` (needs `uv tool install prek` first if not already on PATH).

## Design invariants -- don't casually undo these

- **No cloud-SDK dependency.** Auth is `token: str` or `token_provider: Callable[[], str | Awaitable[str]]`. Do not add `azure-identity`/`boto3`/etc. as a real dependency -- that belongs in the caller's app or an example.
- **No hardcoded catalog/schema.** `catalog`/`schema` default to `None` everywhere. This package has zero knowledge of any specific Databricks workspace's naming.
- **Chunk order is not fetch order.** `DatabricksClient` fetches chunks concurrently (bounded, with backpressure) and they can complete out of order. `query.py`'s buffered paths (`_fetch_chunks`) sort by `chunk_index` before unioning; `stream_query_json` uses a `pending` reorder buffer instead, since it can't buffer the whole result first. If you touch either, keep a test proving order survives out-of-order arrival (see `test_stream_query_json_preserves_order_despite_out_of_order_chunks`).
- **`stream_query_json` must not buffer the full result before emitting.** That's the entire reason it exists instead of just calling `run_query` and serializing the result -- see its docstring for the memory/latency argument.
- **No silent row caps.** There's no `ABSOLUTE_ROW_LIMIT`-style ceiling baked in. If a caller wants one, that's `row_limit`, which they pass explicitly.
- **No retry dependency.** `client.py`'s `_retry_call` is a ~10-line hand-rolled exponential-backoff loop, replacing tenacity on purpose -- it's the only retry pattern in the whole client, so a dependency for it wasn't worth it. Don't reach for tenacity (or another retry library) unless the retry logic actually grows real complexity (e.g. per-endpoint policies); a second small loop is still cheaper than the dependency.
- **Arrow backend is pluggable, not hardcoded.** `query.py` never imports `nanoarrow`/`arro3` directly -- it goes through `_arrow_backend.py`'s `parse_ipc_stream`/`write_ipc_stream`, which resolve to nanoarrow (default) or arro3 depending on what's installed, or to whatever a caller passed to `set_arrow_backend()`. Keep new Arrow-touching code going through that module instead of importing a specific backend.
- **`_streaming.py` never imports duckdb.** That's the entire point of the split from `query.py` -- it's what lets `duckbricks[json]` (arro3, no duckdb) work for `stream_query_json`. If a change to `_streaming.py` needs something duckdb provides, that function belongs in `query.py` instead, not a reason to import duckdb here.
- **`stream_query_json`'s JSON output always goes through arro3, never nanoarrow.** nanoarrow has no JSON writer, so unlike `_arrow_backend.py`'s parse/write functions, there's no nanoarrow fallback here -- `_streaming._write_ndjson` imports `arro3.io` directly (lazily, at call time) and raises a clear `ImportError` if it's missing. Always pass `explicit_nulls=True` to `write_ndjson` -- arro3 omits null-valued keys by default, which would make a row's JSON shape vary by which columns happen to be null (see `test_write_ndjson_produces_one_json_object_per_row`).

## Testing

Mock the Databricks endpoints with `respx` (see `tests/conftest.py`'s `mock_warehouse` fixture) rather than hitting a real warehouse. The fixture builds real Arrow-IPC chunk bytes via DuckDB + `_arrow_backend.write_ipc_stream` (the same trick `query.py`'s `_to_arrow_bytes` uses, whichever backend happens to be installed), so tests exercise the actual Arrow-C-Data-Interface round trip, not a stand-in. Pass `reverse_arrival=True` to force genuine out-of-order chunk completion when a test needs to prove ordering survives it.

## Releasing

1. Bump `version` in `pyproject.toml`.
2. `git tag vX.Y.Z && git push origin vX.Y.Z`.
3. `.github/workflows/release.yml` runs the test job, then builds and publishes to PyPI via trusted publishing (OIDC) -- no stored token.

One-time, outside this repo: register this GitHub repo + `release.yml` workflow as a **trusted publisher** on the `duckbricks` PyPI project (PyPI project settings -> Publishing). Without that, the `publish` job's OIDC exchange fails even though tests pass.
