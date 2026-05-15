"""Unit tests for TransformService."""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services.transform_service import TransformService


def _utc(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


def _open_db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    return Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )


@pytest.fixture()
def freshness_db(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Generator[Database, None, None]:
    """Empty DB with app.import_log and core.dim_accounts shimmed in."""
    db = _open_db(tmp_path, mock_secret_store)
    try:
        db.execute("CREATE SCHEMA IF NOT EXISTS app")
        db.execute("CREATE SCHEMA IF NOT EXISTS core")
        db.execute(
            "CREATE TABLE app.import_log "
            "(import_id VARCHAR, status VARCHAR, completed_at TIMESTAMPTZ)"
        )
        db.execute(
            "CREATE TABLE core.dim_accounts "
            "(account_id VARCHAR, updated_at TIMESTAMPTZ)"
        )
        yield db
    finally:
        db.close()


def test_freshness_pending_when_import_newer_than_apply(
    freshness_db: Database,
) -> None:
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?)", [_utc(2026, 5, 10, 12, 0)]
    )
    freshness_db.execute(
        "INSERT INTO app.import_log VALUES ('i1', 'complete', ?)",
        [_utc(2026, 5, 13, 18, 24)],
    )
    f = TransformService(freshness_db).freshness()
    assert f.pending is True
    assert f.last_apply_at == _utc(2026, 5, 10, 12, 0)
    assert f.latest_import_at == _utc(2026, 5, 13, 18, 24)


def test_freshness_not_pending_when_apply_newer(freshness_db: Database) -> None:
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?)", [_utc(2026, 5, 13, 19, 0)]
    )
    freshness_db.execute(
        "INSERT INTO app.import_log VALUES ('i1', 'complete', ?)",
        [_utc(2026, 5, 13, 18, 24)],
    )
    f = TransformService(freshness_db).freshness()
    assert f.pending is False


def test_freshness_pending_when_dim_table_missing(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    """Pre-first-transform: dim_accounts doesn't exist; pending if any imports."""
    db = _open_db(tmp_path, mock_secret_store)
    try:
        db.execute("CREATE SCHEMA IF NOT EXISTS app")
        db.execute(
            "CREATE TABLE app.import_log "
            "(import_id VARCHAR, status VARCHAR, completed_at TIMESTAMPTZ)"
        )
        db.execute(
            "INSERT INTO app.import_log VALUES ('i1', 'complete', ?)",
            [_utc(2026, 5, 13, 18, 24)],
        )
        f = TransformService(db).freshness()
        assert f.pending is True
        assert f.last_apply_at is None
    finally:
        db.close()


def test_freshness_no_imports_no_pending(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    """No imports yet: pending=False (nothing waiting to be refreshed)."""
    db = _open_db(tmp_path, mock_secret_store)
    try:
        f = TransformService(db).freshness()
        assert f.pending is False
        assert f.last_apply_at is None
        assert f.latest_import_at is None
    finally:
        db.close()


def test_freshness_filters_incomplete_imports(freshness_db: Database) -> None:
    """Only status='complete' rows count for staleness."""
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?)", [_utc(2026, 5, 10, 12, 0)]
    )
    freshness_db.execute(
        "INSERT INTO app.import_log VALUES ('i1', 'in_progress', ?)",
        [_utc(2026, 5, 13, 18, 24)],
    )
    f = TransformService(freshness_db).freshness()
    assert f.pending is False
    assert f.latest_import_at is None
