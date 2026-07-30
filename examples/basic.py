"""Run a query against a Databricks SQL warehouse using a static personal
access token (or any other pre-issued OAuth token). Requires the `duckdb`
extra: `pip install duckbricks[duckdb]`.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXX \\
    python examples/basic.py
"""

import asyncio
import os

from duckbricks import DatabricksClient, run_query, stream_query_json


async def main() -> None:
    client = DatabricksClient(
        host=os.environ["DATABRICKS_HOST"],
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        token=os.environ["DATABRICKS_TOKEN"],
    )

    result = await run_query(client, "SELECT 1 AS n, 'hello' AS greeting")
    print(result.dicts())

    async for row_json in stream_query_json(client, "SELECT * FROM range(5)"):
        print(row_json)


if __name__ == "__main__":
    asyncio.run(main())
