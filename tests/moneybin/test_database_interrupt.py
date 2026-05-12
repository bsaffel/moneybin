"""Tests for interrupt_and_reset_database() using the _active_write_conn slot."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database, interrupt_and_reset_database


@pytest.mark.unit
def test_interrupt_and_reset_calls_interrupt_then_closes(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    db = Database(
        tmp_path / "t.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )
    real_conn = db._conn  # pyright: ignore[reportPrivateUsage]
    assert real_conn is not None
    mock_conn = MagicMock(wraps=real_conn)
    db._conn = mock_conn  # type: ignore[assignment]  # pyright: ignore[reportPrivateUsage]
    db.interrupt_and_reset()
    mock_conn.interrupt.assert_called_once()
    assert db._conn is None  # pyright: ignore[reportPrivateUsage]
    assert db._closed is True  # pyright: ignore[reportPrivateUsage]


@pytest.mark.unit
def test_module_helper_fires_on_active_write_conn(
    tmp_path: Path,
    mock_secret_store: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(db_module, "_active_write_conn", None)
    db = Database(
        tmp_path / "t2.duckdb", secret_store=mock_secret_store, no_auto_upgrade=True
    )
    # Register as active write conn
    with db_module._active_write_lock:  # pyright: ignore[reportPrivateUsage]
        db_module._active_write_conn = db  # pyright: ignore[reportPrivateUsage]

    interrupt_and_reset_database()

    assert db_module._active_write_conn is None  # pyright: ignore[reportPrivateUsage]
    assert db._closed is True  # pyright: ignore[reportPrivateUsage]


@pytest.mark.unit
def test_module_helper_is_noop_when_no_active_conn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(db_module, "_active_write_conn", None)
    # Should not raise
    interrupt_and_reset_database()
    assert db_module._active_write_conn is None  # pyright: ignore[reportPrivateUsage]
