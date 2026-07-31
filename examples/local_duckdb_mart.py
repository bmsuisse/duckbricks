"""Stream a Databricks query into a persistent local DuckDB file, export it to
Excel, then query the local file directly -- no further Databricks round trips
once the data is on disk. Requires `pip install duckbricks[duckdb]`.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapi... \\
    python examples/local_duckdb_mart.py

This uses `stream_query_json` (not `run_query`) specifically because it
writes each batch to disk AS IT ARRIVES rather than buffering the whole
result in memory first -- the same reason that function exists in the first
place (see its docstring). For a result you're happy to hold in memory as one
shot, `run_query_streamed(..., as_arrow=True)` + DuckDB's Arrow C Data
Interface (the same trick duckbricks uses internally) is less code; this
example intentionally shows the streaming path since that's what was asked.
"""

import asyncio
import os
import tempfile

import duckdb

from duckbricks import DatabricksClient, stream_query_json

DB_PATH = "onesales_local.duckdb"
TABLE = "customers"
XLSX_PATH = "customers.xlsx"
BATCH_SIZE = 1_000  # rows buffered in Python before each disk write


async def stream_into_duckdb(client: DatabricksClient, sql: str, con: duckdb.DuckDBPyConnection) -> int:
    """Batches stream_query_json's per-row JSON strings and flushes each
    batch to `TABLE` via a temp NDJSON file + DuckDB's own read_json_auto --
    the first flush creates the table (schema inferred from the data), every
    later one just inserts. Returns the total row count written."""
    batch: list[str] = []
    total = 0
    table_created = False

    async def flush() -> None:
        nonlocal table_created
        if not batch:
            return
        with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
            f.write("\n".join(batch))
            path = f.name
        try:
            # TABLE is a fixed module constant, not user input, in both queries below.
            if not table_created:
                sql = f"CREATE OR REPLACE TABLE {TABLE} AS SELECT * FROM read_json_auto(?)"  # noqa: S608
                con.execute(sql, [path])
                table_created = True
            else:
                sql = f"INSERT INTO {TABLE} SELECT * FROM read_json_auto(?)"  # noqa: S608
                con.execute(sql, [path])
        finally:
            os.unlink(path)
        batch.clear()

    async for row_json in stream_query_json(client, sql):
        batch.append(row_json)
        total += 1
        if len(batch) >= BATCH_SIZE:
            await flush()
    await flush()  # remaining partial batch
    return total


async def main() -> None:
    client = DatabricksClient(
        host=os.environ["DATABRICKS_HOST"],
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        token=os.environ["DATABRICKS_TOKEN"],
    )
    con = duckdb.connect(DB_PATH)

    rows = await stream_into_duckdb(client, "SELECT * FROM my_catalog.my_schema.customers", con)
    print(f"Wrote {rows} rows to {DB_PATH}::{TABLE}")

    # Pure DuckDB from here on -- no more Databricks calls. The `excel`
    # extension auto-installs on first use (needs network access once).
    con.execute("INSTALL excel; LOAD excel;")
    con.execute(f"COPY {TABLE} TO '{XLSX_PATH}' WITH (FORMAT xlsx, HEADER true)")
    print(f"Exported to {XLSX_PATH}")

    # Querying the now-local file -- this connection (or a fresh
    # duckdb.connect(DB_PATH) in a completely separate process later) never
    # touches Databricks again.
    top = con.execute(f"SELECT * FROM {TABLE} LIMIT 5").fetchall()  # noqa: S608
    for row in top:
        print(row)


if __name__ == "__main__":
    asyncio.run(main())
