from ._arrow_backend import set_arrow_backend
from ._streaming import (
    HEARTBEAT,
    QueryTimeout,
    ReplayableArrowChunk,
    await_with_heartbeat,
    stream_query_json,
)
from .client import DatabricksClient

__all__ = [
    "HEARTBEAT",
    "DatabricksClient",
    "QueryTimeout",
    "ReplayableArrowChunk",
    "await_with_heartbeat",
    "set_arrow_backend",
    "stream_query_json",
]

# The DuckDB-based serialization layer needs the `duckdb` extra
# (`pip install duckbricks[duckdb]`, or `duckdb-arro3` -- see
# src/duckbricks/_arrow_backend.py) -- keep it out of the top-level import so
# `DatabricksClient`/`stream_query_json` (imported above, needs only an Arrow
# backend -- see _streaming.py and the `duckbricks[json]` extra) stay usable
# without a real DuckDB engine installed.
try:
    from .query import (
        QueryResult,
        feed_duckdb_table_to_databricks,
        feed_select_to_duckdb_table,
        run_query,
        run_query_streamed,
    )

    __all__ += [
        "QueryResult",
        "feed_duckdb_table_to_databricks",
        "feed_select_to_duckdb_table",
        "run_query",
        "run_query_streamed",
    ]
except ImportError:
    pass
