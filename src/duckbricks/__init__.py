from .client import DatabricksClient

__all__ = ["DatabricksClient"]

# The DuckDB-based serialization layer needs the `duckdb` extra
# (`pip install duckbricks[duckdb]`) -- keep it out of the top-level import so
# `DatabricksClient` alone (e.g. for execute_json_statement) stays usable
# without duckdb/nanoarrow installed.
try:
    from .query import (
        HEARTBEAT,
        QueryResult,
        QueryTimeout,
        ReplayableArrowChunk,
        run_query,
        run_query_streamed,
        stream_query_json,
    )

    __all__ += [
        "HEARTBEAT",
        "QueryResult",
        "QueryTimeout",
        "ReplayableArrowChunk",
        "run_query",
        "run_query_streamed",
        "stream_query_json",
    ]
except ImportError:
    pass
