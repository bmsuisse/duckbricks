"""Write local DuckDB data back up to a Databricks table -- the reverse of
feed_select_to_duckdb.py. Stages the result as Parquet under a Unity Catalog
volume path and loads it via `COPY INTO` (append) or `CREATE OR REPLACE
TABLE ... AS SELECT` (replace). Requires `pip install duckbricks[duckdb]`.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapi... \\
    python examples/feed_duckdb_table_to_databricks.py
"""

import asyncio
import os

import duckdb

from duckbricks import DatabricksClient, feed_duckdb_table_to_databricks


async def main() -> None:
    client = DatabricksClient(
        host=os.environ["DATABRICKS_HOST"],
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        token=os.environ["DATABRICKS_TOKEN"],
    )
    con = duckdb.connect(":memory:")  # or duckdb.connect("some.duckdb") for a persistent file
    con.execute("CREATE TABLE todays_orders AS SELECT * FROM read_csv('todays_orders.csv')")

    # Default mode="append" -- Databricks' own COPY INTO, which tracks
    # already-loaded staged files internally.
    rows = await feed_duckdb_table_to_databricks(
        client,
        con,
        "SELECT * FROM todays_orders",
        "my_catalog.my_schema.orders",
        staging_volume="/Volumes/my_catalog/my_schema/staging",
    )
    print(f"Appended {rows} rows to my_catalog.my_schema.orders")

    # mode="replace" -- fully replaces the target table with this result.
    rows = await feed_duckdb_table_to_databricks(
        client,
        con,
        "SELECT * FROM todays_orders WHERE status != 'canceled'",
        "my_catalog.my_schema.orders_snapshot",
        staging_volume="/Volumes/my_catalog/my_schema/staging",
        mode="replace",
    )
    print(f"Replaced my_catalog.my_schema.orders_snapshot with {rows} rows")


if __name__ == "__main__":
    asyncio.run(main())
