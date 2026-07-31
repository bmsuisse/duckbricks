"""Tests for the pluggable Arrow IPC backend (see _arrow_backend.py) -- the
bring-your-own-engine override specifically. The built-in nanoarrow/arro3
paths are already exercised end-to-end by every test in test_query.py that
round-trips real Arrow-IPC bytes through DuckDB."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, cast

import pytest

from duckbricks import _arrow_backend

if TYPE_CHECKING:
    import duckdb

_FAKE_RELATION = cast("duckdb.DuckDBPyRelation", "relation-marker")


@pytest.fixture
def restore_backend():
    parse, write, name = _arrow_backend._parse_impl, _arrow_backend._write_impl, _arrow_backend.BACKEND
    yield
    _arrow_backend._parse_impl, _arrow_backend._write_impl, _arrow_backend.BACKEND = parse, write, name


def test_set_arrow_backend_overrides_dispatch(restore_backend):
    calls = []

    def fake_parse(data: bytes):
        calls.append(("parse", data))
        return "parsed-marker"

    def fake_write(stream, buf):
        calls.append(("write", stream))
        buf.write(b"written-marker")

    _arrow_backend.set_arrow_backend(fake_parse, fake_write, name="fake")

    assert _arrow_backend.BACKEND == "fake"
    assert _arrow_backend.parse_ipc_stream(b"raw-bytes") == "parsed-marker"

    buf = io.BytesIO()
    _arrow_backend.write_ipc_stream(_FAKE_RELATION, buf)
    assert buf.getvalue() == b"written-marker"
    assert calls == [("parse", b"raw-bytes"), ("write", "relation-marker")]


def test_no_backend_configured_raises_clear_import_error(restore_backend):
    _arrow_backend._parse_impl = None
    _arrow_backend._write_impl = None
    _arrow_backend.BACKEND = None

    with pytest.raises(ImportError, match="set_arrow_backend"):
        _arrow_backend.parse_ipc_stream(b"data")
    with pytest.raises(ImportError, match="set_arrow_backend"):
        _arrow_backend.write_ipc_stream(_FAKE_RELATION, io.BytesIO())


def test_arro3_write_does_not_compress(monkeypatch):
    """Regression test: arro3.io.write_ipc_stream defaults to compression="LZ4",
    which DuckDB's own Arrow C Data Interface reader decompresses transparently
    (so an in-process round-trip test would never catch this) but which other
    Arrow IPC readers may not support at all -- observed as a silent decode
    failure in duckdb-wasm's browser-side reader. Must always write plain,
    uncompressed bodies, matching the nanoarrow backend's behavior."""
    arro3_io = pytest.importorskip("arro3.io")
    # _arro3_write only exists as a module attribute if nanoarrow's import failed at
    # duckbricks import time (nanoarrow is tried first) -- skip rather than fail in an
    # environment where nanoarrow happens to also be installed.
    arro3_write = getattr(_arrow_backend, "_arro3_write", None)
    if arro3_write is None:
        pytest.skip("nanoarrow is importable here, so the arro3 code path never ran")

    calls = []
    monkeypatch.setattr(
        arro3_io,
        "write_ipc_stream",
        lambda stream, buf, **kwargs: calls.append(kwargs),
    )
    arro3_write(_FAKE_RELATION, io.BytesIO())
    assert calls == [{"compression": None}]
