"""Unit tests for TransformService."""

from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services.transform_service import TransformService, TransformStatus

# raw.import_log columns required by NOT NULL constraints. The table is
# auto-created by Database() schema init; tests only need to provide
# import_id, status, completed_at — the rest are dummy values.
_INSERT_IMPORT = (
    "INSERT INTO raw.import_log "
    "(import_id, source_file, source_type, source_origin, account_names, "
    "status, completed_at) "
    "VALUES (?, '/tmp/f.csv', 'csv', 'test', '[]'::JSON, ?, ?)"
)


def _ts(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    # Naive timestamp; mirrors raw.import_log.completed_at (TIMESTAMP).
    return datetime(year, month, day, hour, minute)


def _tz(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    # tz-aware (UTC); mirrors core.dim_accounts.updated_at in production. SQLMesh
    # materializes the column from CURRENT_TIMESTAMP, which DuckDB types as
    # TIMESTAMP WITH TIME ZONE. The unit fixture matches that type so a naive-vs-
    # aware comparison bug in TransformService.freshness() surfaces in tests.
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
    """Empty DB with core.dim_accounts shimmed in (raw.import_log is auto-created).

    The session TZ is pinned to UTC so tz-aware datetimes inserted into
    ``dim_accounts.updated_at`` round-trip predictably through the
    ``::TIMESTAMP`` cast in ``TransformService.freshness()`` (DuckDB casts
    ``TIMESTAMPTZ`` to ``TIMESTAMP`` using the session TZ).
    """
    db = _open_db(tmp_path, mock_secret_store)
    try:
        db.execute("SET TimeZone = 'UTC'")
        db.execute(
            "CREATE TABLE core.dim_accounts "
            "(account_id VARCHAR, updated_at TIMESTAMP WITH TIME ZONE)"
        )
        yield db
    finally:
        db.close()


def test_freshness_pending_when_import_newer_than_apply(
    freshness_db: Database,
) -> None:
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?)", [_tz(2026, 5, 10, 12, 0)]
    )
    freshness_db.execute(_INSERT_IMPORT, ["i1", "complete", _ts(2026, 5, 13, 18, 24)])
    f = TransformService(freshness_db).freshness()
    assert f.pending is True
    assert f.last_apply_at == _ts(2026, 5, 10, 12, 0)
    assert f.latest_import_at == _ts(2026, 5, 13, 18, 24)


def test_freshness_not_pending_when_apply_newer(freshness_db: Database) -> None:
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?)", [_tz(2026, 5, 13, 19, 0)]
    )
    freshness_db.execute(_INSERT_IMPORT, ["i1", "complete", _ts(2026, 5, 13, 18, 24)])
    f = TransformService(freshness_db).freshness()
    assert f.pending is False


def test_freshness_pending_when_dim_table_missing(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    """Pre-first-transform: dim_accounts doesn't exist; pending if any imports."""
    db = _open_db(tmp_path, mock_secret_store)
    try:
        db.execute(_INSERT_IMPORT, ["i1", "complete", _ts(2026, 5, 13, 18, 24)])
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


def test_freshness_filters_reverted_and_failed_imports(
    freshness_db: Database,
) -> None:
    """Reverted and failed rows must not count toward staleness."""
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?)", [_tz(2026, 5, 10, 12, 0)]
    )
    freshness_db.execute(_INSERT_IMPORT, ["i1", "reverted", _ts(2026, 5, 13, 18, 24)])
    freshness_db.execute(_INSERT_IMPORT, ["i2", "failed", _ts(2026, 5, 13, 18, 30)])
    f = TransformService(freshness_db).freshness()
    assert f.pending is False
    assert f.latest_import_at is None


def test_freshness_counts_partial_imports(freshness_db: Database) -> None:
    """Partial imports landed some rows; they count toward staleness."""
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?)", [_tz(2026, 5, 10, 12, 0)]
    )
    freshness_db.execute(_INSERT_IMPORT, ["i1", "partial", _ts(2026, 5, 13, 18, 24)])
    f = TransformService(freshness_db).freshness()
    assert f.pending is True
    assert f.latest_import_at == _ts(2026, 5, 13, 18, 24)


def test_apply_returns_apply_result_shape(
    tmp_path: Path, mock_secret_store: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """apply() returns ApplyResult(applied=True, duration_seconds>=0) on success."""
    from contextlib import contextmanager

    fake_ctx = MagicMock()

    @contextmanager
    def fake_sqlmesh_context(_db: Database):  # type: ignore[no-untyped-def]
        yield fake_ctx

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context",
        fake_sqlmesh_context,
    )
    monkeypatch.setattr(
        "moneybin.services.transform_service.seed_source_priority",
        lambda _db, _settings: None,
    )
    monkeypatch.setattr(
        "moneybin.services.transform_service.refresh_views",
        lambda _db: None,
    )

    db = _open_db(tmp_path, mock_secret_store)
    try:
        result = TransformService(db).apply()
    finally:
        db.close()

    assert result.applied is True
    assert result.duration_seconds >= 0
    assert result.error is None
    fake_ctx.plan.assert_called_once_with(auto_apply=True, no_prompts=True)


def test_import_service_run_transforms_delegates_to_transform_service(
    tmp_path: Path, mock_secret_store: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ImportService.run_transforms() delegates to TransformService.apply()."""
    from moneybin.services.import_service import ImportService
    from moneybin.services.transform_service import ApplyResult

    calls: list[str] = []

    def fake_apply(self: TransformService) -> ApplyResult:
        calls.append("apply")
        return ApplyResult(applied=True, duration_seconds=0.0)

    monkeypatch.setattr(TransformService, "apply", fake_apply)

    db = _open_db(tmp_path, mock_secret_store)
    try:
        result = ImportService(db).run_transforms()
    finally:
        db.close()

    assert result is True
    assert calls == ["apply"]


def test_status_uninitialized_environment(
    tmp_path: Path, mock_secret_store: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fresh DB: no SQLMesh env → initialized=False, pending=False."""
    from contextlib import contextmanager

    fake_ctx = MagicMock()
    fake_ctx.state_reader.get_environment.return_value = None

    @contextmanager
    def fake_sqlmesh_context(_db: Database):  # type: ignore[no-untyped-def]
        yield fake_ctx

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context",
        fake_sqlmesh_context,
    )

    db = _open_db(tmp_path, mock_secret_store)
    try:
        s: TransformStatus = TransformService(db).status()
    finally:
        db.close()

    assert s.environment == "prod"
    assert s.initialized is False
    assert s.last_apply_at is None
    assert s.pending is False


def test_status_initialized_with_finalized_ts(
    tmp_path: Path, mock_secret_store: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SQLMesh env exists and is finalized → initialized=True, last_apply_at set."""
    from contextlib import contextmanager

    # finalized_ts is milliseconds since epoch (SQLMesh convention). Build via
    # an explicit UTC tz-aware datetime so the test is host-TZ independent.
    expected_utc = datetime(2026, 5, 13, 18, 24, 0, tzinfo=UTC)
    finalized_ms = int(expected_utc.timestamp() * 1000)
    expected_naive = expected_utc.replace(tzinfo=None)

    fake_env = MagicMock()
    fake_env.finalized_ts = finalized_ms
    fake_ctx = MagicMock()
    fake_ctx.state_reader.get_environment.return_value = fake_env

    @contextmanager
    def fake_sqlmesh_context(_db: Database):  # type: ignore[no-untyped-def]
        yield fake_ctx

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context",
        fake_sqlmesh_context,
    )

    db = _open_db(tmp_path, mock_secret_store)
    try:
        s: TransformStatus = TransformService(db).status()
    finally:
        db.close()

    assert s.environment == "prod"
    assert s.initialized is True
    assert s.last_apply_at is not None
    assert abs((s.last_apply_at - expected_naive).total_seconds()) < 1.0
