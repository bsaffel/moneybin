"""Tests for Database.interrupt_and_reset and interrupt_and_reset_database helper."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database, interrupt_and_reset_database


@pytest.mark.unit
def test_interrupt_and_reset_calls_interrupt_then_closes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key-32-bytes-padding-padding-pad"
    db = Database(tmp_path / "t.duckdb", secret_store=mock_store, no_auto_upgrade=True)

    # DuckDB's C-extension connection is read-only, so we swap in a MagicMock
    # that wraps the real connection; interrupt() and close() are tracked on the
    # mock while all other attribute accesses fall through.
    real_conn = db._conn
    assert real_conn is not None
    mock_conn = MagicMock(wraps=real_conn)
    db._conn = mock_conn  # type: ignore[assignment]

    db.interrupt_and_reset()

    mock_conn.interrupt.assert_called_once()
    assert db._conn is None
    assert db._closed is True


@pytest.mark.unit
def test_module_helper_clears_singleton(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key-32-bytes-padding-padding-pad"

    monkeypatch.setattr(db_module, "_database_instance", None)
    db = Database(tmp_path / "t.duckdb", secret_store=mock_store, no_auto_upgrade=True)
    monkeypatch.setattr(db_module, "_database_instance", db)

    interrupt_and_reset_database()

    assert db_module._database_instance is None
