<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.svg">
  <img src="assets/logo-light.svg" width="72" height="72" alt="duckbricks">
</picture>

# duckbricks

Runs SQL against a Databricks SQL warehouse via the Statement Execution API, streams the Arrow-IPC result chunks with backpressure into DuckDB (a thin Arrow-to-JSON/rows converter, not a query engine), preserving chunk order with SSE-safe heartbeats during slow cold-starts.

- No pyarrow/pandas/numpy dependency chain -- chunks are parsed via [nanoarrow](https://github.com/apache/arrow-nanoarrow) and handed to DuckDB through the Arrow C Data Interface.
- Bring-your-own-auth -- a static token or your own token-refresh callable. No cloud-SDK dependency baked in.
- Result-order preserved even though chunks can complete out of order over the network.
- Heartbeats between slow chunks, so a caller streaming this over e.g. SSE never goes silent.

## Install

```bash
pip install duckbricks[duckdb]
```

The `duckdb` extra pulls in `duckdb` + `nanoarrow`, needed for `run_query`/`stream_query_json`/etc. Omit it if you only want `DatabricksClient.execute_json_statement` (plain JSON rows, no Arrow/DuckDB involved).

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

See [`examples/basic.py`](examples/basic.py) for a runnable version, or
[`examples/local_duckdb_mart.py`](examples/local_duckdb_mart.py) for streaming
a query into a persistent local DuckDB file, exporting it to Excel, and
querying it again with plain DuckDB SQL -- no more Databricks round trips
once the data's on disk.

## Why not `databricks-sql-connector`?

The [official driver](https://github.com/databricks/databricks-sql-python) is the right choice if you need full DB-API 2.0 compatibility (generic SQL tooling, JDBC/ODBC-style connection semantics). If you just want to pull a query result into your own app as JSON/rows/Arrow, it drags in a lot for that: `pandas`, `thrift`, `openpyxl`, `pybreaker`, `pyjwt`, `oauthlib`, `lz4`, `requests`, `urllib3` as hard dependencies (`pyarrow` is at least now optional). duckbricks' core is `httpx` + `tenacity`; `duckdb`/`nanoarrow` are one opt-in extra, and that's the whole dependency tree.

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
- `stream_query_json(client, sql, **kwargs)` -- yields `HEARTBEAT`, then each row as a JSON string, as soon as its chunk arrives.
- `client.execute_json_statement(sql, ...)` -- lower-level: JSON rows straight from Databricks, no duckdb/nanoarrow needed.

`run_query`/`run_query_streamed`/`stream_query_json` all accept `catalog`, `schema`, `params` (Databricks' own `[{"name", "value", "type"}]` named-parameter format), `row_limit`, `offset`, and `total_timeout_s`.

## License

MIT
