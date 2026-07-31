"""Pluggable Arrow IPC read/write backend -- nanoarrow (`[duckdb]` extra,
default), arro3 (`[duckdb-arro3]` extra), or your own via
`set_arrow_backend()`. All expose the exact same two operations query.py
needs: parse IPC-stream bytes into something implementing
`__arrow_c_stream__` (for DuckDB's Arrow C Data Interface registration), and
write a `__arrow_c_stream__`-implementing object (a DuckDB relation) out as
IPC-stream bytes.

Why two built-in backends: nanoarrow is a ~2MB C library and the leaner
choice by far, so it's tried first and used whenever available. arro3 is a
Rust-compiled alternative -- about 12x larger on disk with no real speed
advantage (see this project's own benchmark notes) -- but its prebuilt wheels
cover platforms nanoarrow's own PyPI coverage has been inconsistent on
(Windows in particular). Install `duckbricks[duckdb-arro3]` instead of
`duckbricks[duckdb]` if nanoarrow doesn't have a working wheel for your
platform; everything else in this package is identical either way.

`parse_ipc_stream`/`write_ipc_stream` are stable function objects that
dispatch to whichever backend is current -- `set_arrow_backend()` swaps the
backend out from under them, so it takes effect even for code that already
did `from duckbricks._arrow_backend import parse_ipc_stream`."""

from __future__ import annotations

import io
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, BinaryIO, Protocol

if TYPE_CHECKING:
    import duckdb


class ArrowCStreamable(Protocol):
    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any: ...


ParseFn = Callable[[bytes], ArrowCStreamable]
WriteFn = Callable[["duckdb.DuckDBPyRelation", BinaryIO], None]

_NO_BACKEND_MSG = (
    "duckbricks has no Arrow IPC backend configured -- install duckbricks[duckdb] "
    "(nanoarrow, the default), duckbricks[duckdb-arro3] (arro3, e.g. if nanoarrow "
    "doesn't have a working wheel for your platform), or call "
    "duckbricks.set_arrow_backend(parse_fn, write_fn) with your own implementation."
)

BACKEND: str | None = None
_parse_impl: ParseFn | None = None
_write_impl: WriteFn | None = None

try:
    from nanoarrow.ipc import InputStream as _NanoInputStream
    from nanoarrow.ipc import StreamWriter as _NanoStreamWriter

    def _nano_parse(data: bytes) -> ArrowCStreamable:
        return _NanoInputStream.from_readable(io.BytesIO(data))

    def _nano_write(stream: duckdb.DuckDBPyRelation, buf: BinaryIO) -> None:
        with _NanoStreamWriter.from_writable(buf) as writer:
            writer.write_stream(stream)

    _parse_impl, _write_impl, BACKEND = _nano_parse, _nano_write, "nanoarrow"

except ImportError:
    try:
        import arro3.io as _arro3_io

        def _arro3_parse(data: bytes) -> ArrowCStreamable:
            return _arro3_io.read_ipc_stream(io.BytesIO(data))

        def _arro3_write(stream: duckdb.DuckDBPyRelation, buf: BinaryIO) -> None:
            # arro3's default (compression="LZ4") body-compresses every record batch --
            # DuckDB's own Arrow C Data Interface reader decompresses that fine (round-trips
            # in-process), but consumers using a different Arrow IPC reader may not support
            # LZ4-compressed IPC bodies at all (observed: duckdb-wasm's browser-side decoder
            # silently fails to parse it). nanoarrow's writer never compresses, so parity with
            # that backend -- and with any generic Arrow IPC reader -- means writing plain,
            # uncompressed bodies here too.
            _arro3_io.write_ipc_stream(stream, buf, compression=None)

        _parse_impl, _write_impl, BACKEND = _arro3_parse, _arro3_write, "arro3"

    except ImportError:
        pass  # no built-in backend installed -- fine as long as set_arrow_backend() gets called


def set_arrow_backend(parse_fn: ParseFn, write_fn: WriteFn, *, name: str = "custom") -> None:
    """Bring your own Arrow IPC engine instead of nanoarrow/arro3 -- e.g. a
    third Arrow library, or a vendored/patched build of either. `parse_fn(data)`
    must return something implementing `__arrow_c_stream__`; `write_fn(relation,
    buf)` must write `relation`'s Arrow-IPC-stream bytes into `buf`. Call this
    once at startup, before running any query -- it replaces the process-wide
    backend immediately, including for code that already imported
    `parse_ipc_stream`/`write_ipc_stream` directly."""
    global _parse_impl, _write_impl, BACKEND
    _parse_impl, _write_impl, BACKEND = parse_fn, write_fn, name


def parse_ipc_stream(data: bytes) -> ArrowCStreamable:
    if _parse_impl is None:
        raise ImportError(_NO_BACKEND_MSG)
    return _parse_impl(data)


def write_ipc_stream(stream: duckdb.DuckDBPyRelation, buf: BinaryIO) -> None:
    if _write_impl is None:
        raise ImportError(_NO_BACKEND_MSG)
    _write_impl(stream, buf)
