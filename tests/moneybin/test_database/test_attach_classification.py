"""Regression tests for `_attach_encrypted`'s lock-error string classification.

Pins the classifier to the actual DuckDB error phrasing so a future DuckDB
upgrade that shifts the string surfaces here as a test failure rather than a
silent regression where raw IOExceptions leak to MCP / CLI callers.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import duckdb
import pytest

from moneybin.database import (
    DatabaseLockError,
    _attach_encrypted,  # pyright: ignore[reportPrivateUsage]
)


def _make_mock_conn(exc: Exception) -> MagicMock:
    conn = MagicMock()
    conn.execute.side_effect = exc
    return conn


def test_duckdb_1_5_3_lock_message_classified_as_lock_error() -> None:
    """DuckDB 1.5.3 emits 'Could not set lock on file ...' on contention."""
    msg = (
        'IO Error: Could not set lock on file "/tmp/probe.duckdb": Resource '
        "temporarily unavailable. The database is locked by another process."
    )
    conn = _make_mock_conn(duckdb.IOException(msg))
    with pytest.raises(DatabaseLockError):
        _attach_encrypted(conn, "ATTACH '/tmp/probe.duckdb' AS m (TYPE DUCKDB)")


def test_duckdb_1_5_2_lock_message_still_classified() -> None:
    """DuckDB 1.5.2 emitted 'Conflicting lock is held' — keep the matcher."""
    msg = (
        "IO Error: Conflicting lock is held in /tmp/probe.duckdb by PID 12345. "
        "Database is locked by another process."
    )
    conn = _make_mock_conn(duckdb.IOException(msg))
    with pytest.raises(DatabaseLockError):
        _attach_encrypted(conn, "ATTACH '/tmp/probe.duckdb' AS m (TYPE DUCKDB)")


def test_duckdb_1_5_2_catalog_message_still_classified() -> None:
    """DuckDB 1.5.2 catalog mismatch on read-while-write contention.

    Raised as CatalogException with 'different configuration'. Keep the
    matcher for environments running older DuckDB.
    """
    msg = (
        'CatalogException: Catalog "moneybin" already exists with different '
        "configuration."
    )
    conn = _make_mock_conn(duckdb.CatalogException(msg))
    with pytest.raises(DatabaseLockError):
        _attach_encrypted(conn, "ATTACH '/tmp/probe.duckdb' AS m (TYPE DUCKDB)")


def test_unrelated_ioexception_reraised_unchanged() -> None:
    """Unrelated IOExceptions must NOT be wrapped as lock errors.

    Disk-full and other non-contention failures need to surface the real
    cause to the caller.
    """
    msg = "IO Error: No space left on device"
    exc = duckdb.IOException(msg)
    conn = _make_mock_conn(exc)
    with pytest.raises(duckdb.IOException) as excinfo:
        _attach_encrypted(conn, "ATTACH '/tmp/probe.duckdb' AS m (TYPE DUCKDB)")
    assert excinfo.value is exc


def test_unrelated_catalog_exception_reraised_unchanged() -> None:
    """Missing-table CatalogExceptions must NOT be wrapped.

    They signal a partially-initialised database, not lock contention.
    """
    msg = 'CatalogException: Table "moneybin.app.foo" does not exist'
    exc = duckdb.CatalogException(msg)
    conn = _make_mock_conn(exc)
    with pytest.raises(duckdb.CatalogException) as excinfo:
        _attach_encrypted(conn, "ATTACH '/tmp/probe.duckdb' AS m (TYPE DUCKDB)")
    assert excinfo.value is exc
