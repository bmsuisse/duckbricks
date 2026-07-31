"""Stream a Databricks query into a persistent local DuckDB file, export it to
Excel, then query the local file directly -- no further Databricks round trips
once the data is on disk. Requires `pip install duckbricks[duckdb]`.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapi... \\
    python examples/local_duckdb_mart.py

Uses feed_select_to_duckdb_table, which writes each Arrow chunk to `con`
directly as it arrives (bounded memory, same as stream_query_json) rather
than buffering the whole result first -- see its docstring.
"""

import asyncio
import os

import duckdb

from duckbricks import DatabricksClient, feed_select_to_duckdb_table

DB_PATH = "onesales_local.duckdb"
TABLE = "customers"
XLSX_PATH = "customers.xlsx"


async def main() -> None:
    client = DatabricksClient(
        host=os.environ["DATABRICKS_HOST"],
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        token=os.environ["DATABRICKS_TOKEN"],
    )
    con = duckdb.connect(DB_PATH)

    rows = await feed_select_to_duckdb_table(client, "SELECT * FROM my_catalog.my_schema.customers", con, TABLE)
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
