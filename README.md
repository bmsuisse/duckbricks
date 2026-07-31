<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.svg">
  <img src="assets/logo-light.svg" width="72" height="72" alt="duckbricks">
</picture>

# duckbricks

Runs SQL against a Databricks SQL warehouse via the Statement Execution API, streams the Arrow-IPC result chunks with backpressure into DuckDB (a thin Arrow-to-JSON/rows converter, not a query engine), preserving chunk order with SSE-safe heartbeats during slow cold-starts.

- No pyarrow/pandas/numpy dependency chain -- chunks are parsed via [nanoarrow](https://github.com/apache/arrow-nanoarrow) (default) or [arro3](https://github.com/kylebarron/arro3) and handed to DuckDB through the Arrow C Data Interface.
- Bring-your-own-auth -- a static token or your own token-refresh callable. No cloud-SDK dependency baked in.
- Result-order preserved even though chunks can complete out of order over the network.
- Heartbeats between slow chunks, so a caller streaming this over e.g. SSE never goes silent.

## Install

```bash
pip install duckbricks[duckdb]
```

The `duckdb` extra pulls in `duckdb` + `nanoarrow`, needed for `run_query`/`feed_select_to_duckdb_table`/`feed_duckdb_table_to_databricks`. Omit it if you only want `DatabricksClient.execute_json_statement` (plain JSON rows, no Arrow/DuckDB involved). If nanoarrow doesn't have a working wheel for your platform (this has happened on Windows), install `duckbricks[duckdb-arro3]` instead -- same API, an arro3-backed Arrow IPC implementation instead of nanoarrow's (see [`src/duckbricks/_arrow_backend.py`](src/duckbricks/_arrow_backend.py); ~12x larger on disk, no real speed difference, so prefer `duckdb` unless you specifically need it). You can also plug in your own Arrow IPC implementation via `duckbricks.set_arrow_backend(...)` instead of either extra.

`stream_query_json` doesn't need duckdb at all -- it's ~3.4x faster going straight from Arrow to JSON via arro3 than round-tripping through a DuckDB connection per chunk (see `src/duckbricks/_streaming.py`). If that's all you need (e.g. piping a Databricks query to a FastAPI SSE endpoint), install `duckbricks[json]` instead of `duckdb`/`duckdb-arro3` -- just arro3, no embedded database. Calling `stream_query_json` without arro3 installed at all (e.g. a bare `duckbricks[duckdb]` install) raises a clear `ImportError` telling you to add it.

## Quickstart

```python
import asyncio
from duckbricks import DatabricksClient, run_query, stream_query_json

async def main():
    client = DatabricksClient(
        host="adb-1234567890.1.azuredatabricks.net",
        warehouse_id="abcd1234efgh5678",
        token="dapi...",  # or token_provider=... -- see Auth below
    )

    result = await run_query(client, "SELECT * FROM my_catalog.my_schema.my_table LIMIT 100")
    print(result.dicts())

    async for row_json in stream_query_json(client, "SELECT * FROM my_catalog.my_schema.big_table"):
        print(row_json)  # one ready-to-send JSON string per row

asyncio.run(main())
```

See [`examples/basic.py`](examples/basic.py) for a runnable version,
[`examples/feed_select_to_duckdb.py`](examples/feed_select_to_duckdb.py) for
materializing a query straight into a table on your own DuckDB connection,
[`examples/local_duckdb_mart.py`](examples/local_duckdb_mart.py) for doing
that into a persistent local DuckDB file, exporting it to Excel, and querying
it again with plain DuckDB SQL -- no more Databricks round trips once the
data's on disk -- [`examples/fastapi_sse.py`](examples/fastapi_sse.py) for
streaming a query to a client as Server-Sent Events, first row out as soon as
its chunk arrives, or
[`examples/feed_duckdb_table_to_databricks.py`](examples/feed_duckdb_table_to_databricks.py)
for writing local DuckDB data back up to a Databricks table.

## Why not `databricks-sql-connector`?

The [official driver](https://github.com/databricks/databricks-sql-python) is the right choice if you need full DB-API 2.0 compatibility (generic SQL tooling, JDBC/ODBC-style connection semantics). If you just want to pull a query result into your own app as JSON/rows/Arrow, it drags in a lot for that: `pandas`, `thrift`, `openpyxl`, `pybreaker`, `pyjwt`, `oauthlib`, `lz4`, `requests`, `urllib3` as hard dependencies (`pyarrow` is at least now optional). duckbricks' core is `httpx` alone; `duckdb` + an Arrow backend (nanoarrow or arro3) are one opt-in extra, and that's the whole dependency tree.

## Auth

`DatabricksClient` takes either:

- `token: str` -- a static personal access token or pre-issued OAuth token, or
- `token_provider` -- a callable (sync or async) returning a token string, called on every request.

duckbricks has no opinion on *how* you get a token and no cloud-SDK dependency of its own. If your provider is expensive to call, cache/refresh inside it -- duckbricks does no caching on your behalf.

```python
client = DatabricksClient(host=..., warehouse_id=..., token_provider=my_token_provider)
```

For Azure Databricks via Azure AD (`azure-identity`), see [`examples/azure_auth.py`](examples/azure_auth.py) for a caching `token_provider` built on `DefaultAzureCredential`.

## API

- `DatabricksClient(host, warehouse_id, *, token=None, token_provider=None, ...)`
- `run_query(client, sql, **kwargs) -> QueryResult` -- full result, buffered.
- `run_query_streamed(client, sql, *, as_arrow=False, **kwargs)` -- yields `HEARTBEAT` while waiting, then the final `QueryResult` or Arrow bytes.
- `stream_query_json(client, sql, **kwargs)` -- yields `HEARTBEAT`, then each row as a JSON string, as soon as its chunk arrives. Serialized via arro3 (needs the `duckbricks[json]`/`duckdb-arro3` extra -- see Install above), not DuckDB: timestamps come out as full ISO-8601 (`"2026-07-31T19:38:21.834969+02:00"`, not DuckDB's `to_json`-style `"2026-07-31 19:38:21.834969+02"`), and every column key is always present (`"col":null` for a null value, never an omitted key).
- `feed_select_to_duckdb_table(client, sql, con, table_name, *, if_exists="replace", **kwargs) -> int` -- streams the result straight into `table_name` on your own `duckdb.DuckDBPyConnection` (in-memory or a persistent `duckdb.connect("some.duckdb")`) as chunks arrive; `con` stays open afterwards with a real table to keep querying. `if_exists` is `"replace"` (default), `"append"`, or `"fail"`. Returns the row count written.
- `feed_duckdb_table_to_databricks(client, con, source_sql, target_table, *, staging_volume, mode="append", total_timeout_s=None) -> int` -- the reverse direction: stages `source_sql`'s result (run on your own DuckDB connection) as Parquet under a Unity Catalog volume path and loads it into `target_table` on Databricks. `mode` is `"append"` (default, via `COPY INTO`) or `"replace"` (`CREATE OR REPLACE TABLE ... AS SELECT`). Returns the row count written; staged files are always cleaned up afterwards.
- `client.execute_json_statement(sql, ...)` -- lower-level: JSON rows straight from Databricks, no duckdb/Arrow backend needed.
- `client.upload_volume_file(volume_path, data)` / `client.delete_volume_file(volume_path)` -- lower-level Files API access to a Unity Catalog volume, used internally by `feed_duckdb_table_to_databricks`.

`run_query`/`run_query_streamed`/`stream_query_json`/`feed_select_to_duckdb_table` all accept `catalog`, `schema`, `params` (Databricks' own `[{"name", "value", "type"}]` named-parameter format), `row_limit`, `offset`, and `total_timeout_s`.

## License

MIT
