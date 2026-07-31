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
