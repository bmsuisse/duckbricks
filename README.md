# duckbricks
Runs SQL against a Databricks SQL warehouse via the Statement Execution API, streams the Arrow-IPC result chunks with backpressure into DuckDB (a thin Arrow-to-JSON/rows converter, not a query engine), preserving chunk order with SSE-safe heartbeats during slow cold-starts.
