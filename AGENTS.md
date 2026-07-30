# duckbricks

Runs SQL against a Databricks SQL warehouse via the Statement Execution API, streams the Arrow-IPC result chunks with backpressure into DuckDB for JSON/rows/Arrow serialization. See `README.md` for the user-facing API; this file is about working *on* the package.

## Layout

- `src/duckbricks/client.py` -- pure REST client (auth, statement submission/polling, backpressure-bounded concurrent chunk download). No duckdb/nanoarrow dependency, intentionally: someone who only wants `execute_json_statement` shouldn't have to install them.
- `src/duckbricks/query.py` -- everything that touches duckdb/nanoarrow: `ReplayableArrowChunk`, heartbeat helpers, `run_query`/`run_query_streamed`/`stream_query_json`. Requires the `duckdb` extra. `__init__.py` imports this in a `try/except ImportError` so the package still works without it.
- `tests/` -- respx mocks the Databricks REST endpoints (warehouse status, statement submit, chunk-link resolution, external-link byte download); no real warehouse or credentials needed to run the suite.
- `examples/` -- `basic.py` (static token) and `azure_auth.py` (Azure AD `token_provider` via `azure-identity`, kept out of core deps on purpose -- see "Auth" below).

## Commands

```bash
uv sync --all-extras       # install everything, incl. dev group + duckdb extra
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

## Testing

Mock the Databricks endpoints with `respx` (see `tests/conftest.py`'s `mock_warehouse` fixture) rather than hitting a real warehouse. The fixture builds real Arrow-IPC chunk bytes via DuckDB + nanoarrow's `StreamWriter` (the same trick `query.py`'s `_to_arrow_bytes` uses), so tests exercise the actual Arrow-C-Data-Interface round trip, not a stand-in. Pass `reverse_arrival=True` to force genuine out-of-order chunk completion when a test needs to prove ordering survives it.

## Releasing

1. Bump `version` in `pyproject.toml`.
2. `git tag vX.Y.Z && git push origin vX.Y.Z`.
3. `.github/workflows/release.yml` runs the test job, then builds and publishes to PyPI via trusted publishing (OIDC) -- no stored token.

One-time, outside this repo: register this GitHub repo + `release.yml` workflow as a **trusted publisher** on the `duckbricks` PyPI project (PyPI project settings -> Publishing). Without that, the `publish` job's OIDC exchange fails even though tests pass.
