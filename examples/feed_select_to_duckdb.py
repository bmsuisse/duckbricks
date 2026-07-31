"""Materialize a Databricks query straight into a table on your own DuckDB
connection -- chunk by chunk as it arrives, not buffered in Python first.
Requires `pip install duckbricks[duckdb]`.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapi... \\
    python examples/feed_select_to_duckdb.py

See examples/local_duckdb_mart.py for the same function used as a step in a
bigger pipeline (persistent file + Excel export + local querying).
"""

import asyncio
import os

import duckdb

from duckbricks import DatabricksClient, feed_select_to_duckdb_table


async def main() -> None:
    client = DatabricksClient(
        host=os.environ["DATABRICKS_HOST"],
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        token=os.environ["DATABRICKS_TOKEN"],
    )
    con = duckdb.connect(":memory:")  # or duckdb.connect("some.duckdb") for a persistent file

    # Default if_exists="replace" -- drops/recreates the table.
    rows = await feed_select_to_duckdb_table(client, "SELECT * FROM my_catalog.my_schema.customers", con, "customers")
    print(f"Wrote {rows} rows to in-memory table 'customers'")

    # From here on it's just DuckDB SQL -- no more Databricks calls.
    print(con.execute("SELECT COUNT(*) FROM customers").fetchone())
    print(con.execute("SELECT * FROM customers LIMIT 5").fetchall())

    # if_exists="append" -- e.g. tacking today's rows onto an existing table.
    more = await feed_select_to_duckdb_table(
        client,
        "SELECT * FROM my_catalog.my_schema.customers WHERE updated_at > current_date()",
        con,
        "customers",
        if_exists="append",
    )
    print(f"Appended {more} more rows")


if __name__ == "__main__":
    asyncio.run(main())
