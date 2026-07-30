"""Plug Azure AD auth into duckbricks via `token_provider` -- duckbricks itself
has no azure-identity dependency, so this lives in your app, not the library.

Needs `pip install azure-identity` plus `pip install duckbricks[duckdb]`.

    DATABRICKS_HOST=adb-1234567890.1.azuredatabricks.net \\
    DATABRICKS_WAREHOUSE_ID=abcd1234efgh5678 \\
    python examples/azure_auth.py

Uses whatever azure-identity's DefaultAzureCredential finds in your
environment (az CLI login, managed identity, env-var service principal, ...).
"""

import asyncio
import os
import time

from azure.identity import DefaultAzureCredential

from duckbricks import DatabricksClient, run_query

# Fixed Azure AD application ID for Azure Databricks -- the same for every
# Azure Databricks workspace, not something you configure per-deployment.
# See https://learn.microsoft.com/azure/databricks/dev-tools/auth/oauth-m2m
_AZURE_DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"

# Refresh well before actual expiry, not just-in-time -- a long-running
# streamed query calls the provider on every request, and re-deriving a fresh
# token per call (rather than serving a cached one) would be needlessly slow.
_TOKEN_REFRESH_MARGIN_S = 300


class AzureTokenProvider:
    """Caches the AAD token between calls -- DefaultAzureCredential.get_token()
    does its own network/subprocess round trip on every call and does not
    cache internally. get_token() is blocking, so it runs via
    asyncio.to_thread when called from the event loop."""

    def __init__(self) -> None:
        self._credential = DefaultAzureCredential()
        self._cached: tuple[str, float] | None = None  # (token, expires_on)

    def _get_or_refresh(self) -> str:
        if self._cached is None or self._cached[1] - _TOKEN_REFRESH_MARGIN_S <= time.time():
            token = self._credential.get_token(_AZURE_DATABRICKS_SCOPE)
            self._cached = (token.token, token.expires_on)
        return self._cached[0]

    async def __call__(self) -> str:
        return await asyncio.to_thread(self._get_or_refresh)


async def main() -> None:
    client = DatabricksClient(
        host=os.environ["DATABRICKS_HOST"],
        warehouse_id=os.environ["DATABRICKS_WAREHOUSE_ID"],
        token_provider=AzureTokenProvider(),
    )
    result = await run_query(client, "SELECT current_catalog(), current_schema()")
    print(result.dicts())


if __name__ == "__main__":
    asyncio.run(main())
