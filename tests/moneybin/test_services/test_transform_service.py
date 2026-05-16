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

# raw.ofx_accounts NOT NULL columns. Tests only care about account_id,
# extracted_at, and import_id; the rest are stubs.
_INSERT_RAW_ACCOUNT = (
    "INSERT INTO raw.ofx_accounts "
    "(account_id, source_file, extracted_at, import_id) "
    "VALUES (?, '/tmp/f.ofx', ?, ?)"
)


def _ts(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    # Naive timestamp; mirrors raw.*_accounts.extracted_at and
    # raw.import_log.completed_at (both TIMESTAMP).
    return datetime(year, month, day, hour, minute)


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
    """Empty DB with core.dim_accounts shimmed in (raw.* are auto-created).

    The shimmed dim has both ``extracted_at`` (the propagated raw value
    that drives the pending comparison) and ``updated_at`` (the SQLMesh
    CURRENT_TIMESTAMP retained for the informational ``last_apply_at``
    field). The session TZ is pinned to UTC so naive vs. tz-aware
    inserts round-trip predictably through the ``updated_at::TIMESTAMP``
    cast in :meth:`TransformService._max_dim_accounts_updated_at`.
    """
    db = _open_db(tmp_path, mock_secret_store)
    try:
        db.execute("SET TimeZone = 'UTC'")
        db.execute(
            "CREATE TABLE core.dim_accounts "
            "(account_id VARCHAR, extracted_at TIMESTAMP, "
            "updated_at TIMESTAMP WITH TIME ZONE)"
        )
        yield db
    finally:
        db.close()


def test_freshness_pending_when_raw_newer_than_dim(
    freshness_db: Database,
) -> None:
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?, ?)",
        [_ts(2026, 5, 10, 12, 0), _ts(2026, 5, 10, 12, 0)],
    )
    freshness_db.execute(_INSERT_RAW_ACCOUNT, ["a", _ts(2026, 5, 13, 18, 24), "i1"])
    freshness_db.execute(_INSERT_IMPORT, ["i1", "complete", _ts(2026, 5, 13, 18, 24)])
    f = TransformService(freshness_db).freshness()
    assert f.pending is True
    assert f.last_apply_at == _ts(2026, 5, 10, 12, 0)
    assert f.latest_import_at == _ts(2026, 5, 13, 18, 24)


def test_freshness_not_pending_when_dim_caught_up(freshness_db: Database) -> None:
    extracted = _ts(2026, 5, 13, 18, 24)
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?, ?)",
        [extracted, _ts(2026, 5, 13, 19, 0)],
    )
    freshness_db.execute(_INSERT_RAW_ACCOUNT, ["a", extracted, "i1"])
    freshness_db.execute(_INSERT_IMPORT, ["i1", "complete", _ts(2026, 5, 13, 18, 30)])
    f = TransformService(freshness_db).freshness()
    assert f.pending is False


def test_freshness_pending_when_dim_table_missing(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    """Pre-first-transform: dim_accounts doesn't exist; pending if any raw rows."""
    db = _open_db(tmp_path, mock_secret_store)
    try:
        db.execute(_INSERT_RAW_ACCOUNT, ["a", _ts(2026, 5, 13, 18, 24), None])
        f = TransformService(db).freshness()
        assert f.pending is True
        assert f.last_apply_at is None
    finally:
        db.close()


def test_freshness_no_raw_no_pending(
    tmp_path: Path, mock_secret_store: MagicMock
) -> None:
    """No raw rows yet: pending=False (nothing waiting to be refreshed)."""
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
    """Raw rows tied to reverted/failed imports must not count toward staleness."""
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?, ?)",
        [_ts(2026, 5, 10, 12, 0), _ts(2026, 5, 10, 12, 0)],
    )
    # Reverted revert deletes raw rows in production; failed imports may leave
    # partial raw rows. Both should be filtered by import_log status.
    freshness_db.execute(_INSERT_RAW_ACCOUNT, ["a", _ts(2026, 5, 13, 18, 24), "i1"])
    freshness_db.execute(_INSERT_RAW_ACCOUNT, ["b", _ts(2026, 5, 13, 18, 30), "i2"])
    freshness_db.execute(_INSERT_IMPORT, ["i1", "reverted", _ts(2026, 5, 13, 18, 24)])
    freshness_db.execute(_INSERT_IMPORT, ["i2", "failed", _ts(2026, 5, 13, 18, 30)])
    f = TransformService(freshness_db).freshness()
    assert f.pending is False
    assert f.latest_import_at is None


def test_freshness_counts_partial_imports(freshness_db: Database) -> None:
    """Partial imports landed some rows; they count toward staleness."""
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?, ?)",
        [_ts(2026, 5, 10, 12, 0), _ts(2026, 5, 10, 12, 0)],
    )
    freshness_db.execute(_INSERT_RAW_ACCOUNT, ["a", _ts(2026, 5, 13, 18, 24), "i1"])
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

    def fake_seed(_db: object, _settings: object) -> None:
        return None

    def fake_refresh(_db: object) -> None:
        return None

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context",
        fake_sqlmesh_context,
    )
    monkeypatch.setattr(
        "moneybin.services.transform_service.seed_source_priority",
        fake_seed,
    )
    monkeypatch.setattr(
        "moneybin.services.transform_service.refresh_views",
        fake_refresh,
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


def test_apply_soft_fails_with_error_type_on_sqlmesh_exception(
    tmp_path: Path, mock_secret_store: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """apply() returns ApplyResult(applied=False, error=<TypeName>) when SQLMesh raises.

    Locks the soft-fail contract: ImportService.run_transforms() re-raises as
    RuntimeError to preserve fail-loud semantics for callers that ignore the
    boolean, but apply() itself must NOT raise — MCP/CLI consumers depend on
    the structured envelope.
    """
    from contextlib import contextmanager

    @contextmanager
    def fake_sqlmesh_context(_db: Database):  # type: ignore[no-untyped-def]
        raise RuntimeError("plan exploded")
        yield  # unreachable; satisfies the contextmanager generator contract

    def fake_seed(_db: object, _settings: object) -> None:
        return None

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context",
        fake_sqlmesh_context,
    )
    monkeypatch.setattr(
        "moneybin.services.transform_service.seed_source_priority",
        fake_seed,
    )

    db = _open_db(tmp_path, mock_secret_store)
    try:
        result = TransformService(db).apply()
    finally:
        db.close()

    assert result.applied is False
    assert result.error == "RuntimeError"
    assert result.duration_seconds >= 0


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


def test_plan_no_changes(
    tmp_path: Path, mock_secret_store: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """plan() returns has_changes=False when SQLMesh Plan is empty."""
    from contextlib import contextmanager

    fake_plan = MagicMock()
    fake_plan.directly_modified = set()
    fake_plan.indirectly_modified = {}
    fake_plan.new_snapshots = []
    fake_plan.context_diff.removed_snapshots = {}

    fake_ctx = MagicMock()
    fake_ctx.plan_builder.return_value.build.return_value = fake_plan

    @contextmanager
    def fake_sqlmesh_context(_db: Database) -> Generator[MagicMock, None, None]:
        yield fake_ctx

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context", fake_sqlmesh_context
    )

    db = _open_db(tmp_path, mock_secret_store)
    try:
        p = TransformService(db).plan()
    finally:
        db.close()

    assert p.has_changes is False
    assert p.directly_modified == []
    assert p.indirectly_modified == []
    assert p.added == []
    assert p.removed == []


def test_plan_lists_changed_models(
    tmp_path: Path, mock_secret_store: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """plan() surfaces directly_modified + indirectly_modified snapshot names."""
    from contextlib import contextmanager

    direct_snap = MagicMock()
    direct_snap.name = "core.dim_accounts"
    indirect_snap = MagicMock()
    indirect_snap.name = "core.fct_transactions"

    fake_plan = MagicMock()
    fake_plan.directly_modified = {direct_snap}
    # indirectly_modified is Dict[SnapshotId, Set[SnapshotId]]; we only read values.
    fake_plan.indirectly_modified = {direct_snap: {indirect_snap}}
    fake_plan.new_snapshots = []
    fake_plan.context_diff.removed_snapshots = {}

    fake_ctx = MagicMock()
    fake_ctx.plan_builder.return_value.build.return_value = fake_plan

    @contextmanager
    def fake_sqlmesh_context(_db: Database) -> Generator[MagicMock, None, None]:
        yield fake_ctx

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context", fake_sqlmesh_context
    )

    db = _open_db(tmp_path, mock_secret_store)
    try:
        p = TransformService(db).plan()
    finally:
        db.close()

    assert p.has_changes is True
    assert p.directly_modified == ["core.dim_accounts"]
    assert p.indirectly_modified == ["core.fct_transactions"]


def test_validate_passes_when_plan_builds(
    tmp_path: Path, mock_secret_store: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """validate() returns valid=True when SQLMesh builds the plan without raising."""
    from contextlib import contextmanager

    fake_ctx = MagicMock()
    fake_ctx.plan_builder.return_value.build.return_value = MagicMock()

    @contextmanager
    def fake_sqlmesh_context(_db: Database) -> Generator[MagicMock, None, None]:
        yield fake_ctx

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context", fake_sqlmesh_context
    )

    db = _open_db(tmp_path, mock_secret_store)
    try:
        v = TransformService(db).validate()
    finally:
        db.close()

    assert v.valid is True
    assert v.errors == []


def test_validate_reports_errors_on_raise(
    tmp_path: Path, mock_secret_store: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """validate() returns valid=False with error detail when SQLMesh raises."""
    from contextlib import contextmanager

    @contextmanager
    def fake_sqlmesh_context(_db: Database) -> Generator[None, None, None]:
        raise RuntimeError("model parse error")
        yield  # unreachable; satisfies the contextmanager generator contract

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context", fake_sqlmesh_context
    )

    db = _open_db(tmp_path, mock_secret_store)
    try:
        v = TransformService(db).validate()
    finally:
        db.close()

    assert v.valid is False
    assert len(v.errors) == 1
    # message is the exception type name, not str(e) — see transform_service.validate
    # docstring for the PII-safety rationale.
    assert v.errors[0]["message"] == "RuntimeError"


def test_audit_aggregates_pass_fail_counts(
    tmp_path: Path, mock_secret_store: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """audit() derives passed/failed counts from per-snapshot audit results."""
    from contextlib import contextmanager

    good = MagicMock()
    good.audit.name = "fct_transactions_pk"
    good.skipped = False
    good.count = 0

    bad = MagicMock()
    bad.audit.name = "fct_transactions_fk"
    bad.skipped = False
    bad.count = 3

    fake_snapshot = MagicMock()
    fake_ctx = MagicMock()
    fake_ctx.snapshots = {"s1": fake_snapshot}
    fake_ctx.snapshot_evaluator.audit.return_value = [good, bad]

    @contextmanager
    def fake_sqlmesh_context(_db: Database) -> Generator[MagicMock, None, None]:
        yield fake_ctx

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context", fake_sqlmesh_context
    )

    db = _open_db(tmp_path, mock_secret_store)
    try:
        result = TransformService(db).audit(start="2026-01-01", end="2026-12-31")
    finally:
        db.close()

    assert result.passed == 1
    assert result.failed == 1
    names = [a["name"] for a in result.audits]
    assert "fct_transactions_pk" in names
    assert "fct_transactions_fk" in names
