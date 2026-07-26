"""Unit tests for TransformService."""

from __future__ import annotations

import logging
from collections.abc import Callable, Generator
from datetime import UTC, date, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services.transform_service import TransformService, TransformStatus
from tests.moneybin.db_helpers import record_sqlmesh_apply

# raw.import_log columns required by NOT NULL constraints. The table is
# auto-created by Database() schema init; tests only need to provide
# import_id, status, completed_at — the rest are dummy values.
_INSERT_IMPORT = (
    "INSERT INTO raw.import_log "
    "(import_id, source_file, source_type, source_origin, account_names, "
    "status, completed_at) "
    "VALUES (?, '/tmp/f.csv', 'csv', 'test', '[]'::JSON, ?, ?)"
)

# raw.ofx_accounts NOT NULL columns. `loaded_at` is what freshness reads;
# `extracted_at` is supplied only because it is part of the primary key.
_INSERT_RAW_ACCOUNT = (
    "INSERT INTO raw.ofx_accounts "
    "(account_id, source_file, extracted_at, loaded_at, import_id) "
    "VALUES (?, '/tmp/f.ofx', ?, ?, ?)"
)

# raw.security_prices NOT NULL columns — a price feed row, which opens no
# import_log batch at all.
_INSERT_RAW_PRICE = (
    "INSERT INTO raw.security_prices "
    "(provider_security_key, price_date, quote_currency, source_type, "
    "source_origin, close, price_basis, loaded_at) "
    "VALUES ('AAPL', DATE '2026-05-13', 'USD', 'stooq', '', 190.00, 'raw', ?)"
)


def _ts(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    # Naive timestamp; mirrors the raw landing columns and
    # raw.import_log.completed_at (all TIMESTAMP).
    return datetime(year, month, day, hour, minute)


@pytest.fixture()
def freshness_db(db: Database) -> Database:
    """Empty DB with core.dim_accounts shimmed in (raw.* are auto-created).

    The shimmed dim carries ``updated_at`` for the informational
    ``last_apply_at`` field; ``extracted_at`` is retained because
    ``dim_accounts`` really has it, not because freshness reads it. The
    session TZ is pinned to UTC so the naive literals these tests write
    into raw landing columns describe the same instants as
    :func:`record_sqlmesh_apply`'s epoch stamp on any machine, and so naive vs.
    tz-aware inserts round-trip predictably through the
    ``updated_at::TIMESTAMP`` cast in
    :meth:`TransformService._max_dim_accounts_updated_at`.
    """
    db.execute("SET TimeZone = 'UTC'")
    db.execute(
        "CREATE TABLE core.dim_accounts "
        "(account_id VARCHAR, extracted_at TIMESTAMP, "
        "updated_at TIMESTAMP WITH TIME ZONE)"
    )
    return db


def test_freshness_pending_when_raw_landed_after_the_last_apply(
    freshness_db: Database, declare_only_models: Callable[..., None]
) -> None:
    declare_only_models("core.dim_accounts")
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?, ?)",
        [_ts(2026, 5, 10, 12, 0), _ts(2026, 5, 10, 12, 0)],
    )
    record_sqlmesh_apply(freshness_db, _ts(2026, 5, 10, 12, 0))
    freshness_db.execute(
        _INSERT_RAW_ACCOUNT,
        ["a", _ts(2026, 5, 13, 18, 0), _ts(2026, 5, 13, 18, 24), "i1"],
    )
    freshness_db.execute(_INSERT_IMPORT, ["i1", "complete", _ts(2026, 5, 13, 18, 24)])
    f = TransformService(freshness_db).freshness()
    assert f.pending is True
    assert f.last_apply_at == _ts(2026, 5, 10, 12, 0)
    assert f.latest_import_at == _ts(2026, 5, 13, 18, 24)


def test_freshness_not_pending_when_the_apply_followed_every_raw_row(
    freshness_db: Database, declare_only_models: Callable[..., None]
) -> None:
    declare_only_models("core.dim_accounts")
    landed = _ts(2026, 5, 13, 18, 24)
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?, ?)",
        [landed, _ts(2026, 5, 13, 19, 0)],
    )
    record_sqlmesh_apply(freshness_db, _ts(2026, 5, 13, 19, 0))
    freshness_db.execute(
        _INSERT_RAW_ACCOUNT, ["a", _ts(2026, 5, 13, 18, 0), landed, "i1"]
    )
    freshness_db.execute(_INSERT_IMPORT, ["i1", "complete", _ts(2026, 5, 13, 18, 30)])
    f = TransformService(freshness_db).freshness()
    assert f.pending is False


def test_freshness_pending_when_no_apply_has_ever_run(
    db: Database, declare_only_models: Callable[..., None]
) -> None:
    """Pre-first-transform: no SQLMesh state at all; pending if any raw rows."""
    declare_only_models("core.dim_accounts")
    db.execute(
        _INSERT_RAW_ACCOUNT,
        ["a", _ts(2026, 5, 13, 18, 0), _ts(2026, 5, 13, 18, 24), None],
    )
    f = TransformService(db).freshness()
    assert f.pending is True
    assert f.last_apply_at is None


def test_freshness_no_raw_no_pending(
    db: Database, declare_only_models: Callable[..., None]
) -> None:
    """No raw rows yet: pending=False (nothing waiting to be refreshed)."""
    declare_only_models()
    f = TransformService(db).freshness()
    assert f.pending is False
    assert f.last_apply_at is None
    assert f.latest_import_at is None


def test_freshness_does_not_report_healthy_when_the_catalog_is_unreadable(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unreadable catalog must not surface as `pending=False`.

    `freshness()` discards the missing set on a never-built warehouse, so a
    swallowed catalog error reached this branch and reported the same value a
    healthy first run does — the one signal watching for absent models going
    quiet on a database that cannot be inspected at all.

    It must also stay *classified* on the way out. `freshness()` has no catch
    of its own, so this exception is what `moneybin system status` and
    `moneybin transform status` surface, and `handle_cli_errors` re-raises
    anything `classify_user_error` returns None for — an unclassified error
    here is a traceback on a user-facing command.
    """
    from moneybin.errors import UserError, classify_user_error

    real_execute = db.execute

    def _fail_catalog_reads(sql: str, *args: object, **kwargs: object) -> object:
        if "duckdb_tables" in sql:
            raise RuntimeError("catalog unreadable")
        return real_execute(sql, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(db, "execute", _fail_catalog_reads)

    with pytest.raises(UserError) as excinfo:
        TransformService(db).freshness()

    assert classify_user_error(excinfo.value) is not None


def test_freshness_filters_reverted_and_failed_imports(
    freshness_db: Database, declare_only_models: Callable[..., None]
) -> None:
    """Raw rows tied to reverted/failed imports must not count toward staleness."""
    declare_only_models("core.dim_accounts")
    record_sqlmesh_apply(freshness_db, _ts(2026, 5, 10, 12, 0))
    # Reverted revert deletes raw rows in production; failed imports may leave
    # partial raw rows. Both should be filtered by import_log status.
    freshness_db.execute(
        _INSERT_RAW_ACCOUNT,
        ["a", _ts(2026, 5, 13, 18, 0), _ts(2026, 5, 13, 18, 24), "i1"],
    )
    freshness_db.execute(
        _INSERT_RAW_ACCOUNT,
        ["b", _ts(2026, 5, 13, 18, 0), _ts(2026, 5, 13, 18, 30), "i2"],
    )
    freshness_db.execute(_INSERT_IMPORT, ["i1", "reverted", _ts(2026, 5, 13, 18, 24)])
    freshness_db.execute(_INSERT_IMPORT, ["i2", "failed", _ts(2026, 5, 13, 18, 30)])
    f = TransformService(freshness_db).freshness()
    assert f.pending is False
    assert f.latest_import_at is None


def test_freshness_counts_partial_imports(
    freshness_db: Database, declare_only_models: Callable[..., None]
) -> None:
    """Partial imports landed some rows; they count toward staleness."""
    declare_only_models("core.dim_accounts")
    record_sqlmesh_apply(freshness_db, _ts(2026, 5, 10, 12, 0))
    freshness_db.execute(
        _INSERT_RAW_ACCOUNT,
        ["a", _ts(2026, 5, 13, 18, 0), _ts(2026, 5, 13, 18, 24), "i1"],
    )
    freshness_db.execute(_INSERT_IMPORT, ["i1", "partial", _ts(2026, 5, 13, 18, 24)])
    f = TransformService(freshness_db).freshness()
    assert f.pending is True
    assert f.latest_import_at == _ts(2026, 5, 13, 18, 24)


def test_latest_import_at_ignores_an_in_flight_batch(
    freshness_db: Database, declare_only_models: Callable[..., None]
) -> None:
    """An unfinished import has no completion time and must not be reported.

    Pins the behavior against the reading the old filter documented — that
    an in-flight batch should count. It never could: `completed_at` is NULL
    until finalize, so MAX skipped it while the filter claimed otherwise.
    """
    declare_only_models("core.dim_accounts")
    freshness_db.execute(_INSERT_IMPORT, ["i1", "complete", _ts(2026, 5, 13, 18, 24)])
    freshness_db.execute(_INSERT_IMPORT, ["i2", "importing", None])
    f = TransformService(freshness_db).freshness()
    assert f.latest_import_at == _ts(2026, 5, 13, 18, 24)


def test_freshness_pending_when_a_price_row_lands_after_the_last_apply(
    freshness_db: Database, declare_only_models: Callable[..., None]
) -> None:
    """A price feed writes no import_log batch — it must still count.

    ``raw.security_prices`` feeds ``core.fct_security_prices`` and arrives
    with no ``import_id`` at all, so a scan set built around the import
    ledger would miss every price observation MoneyBin fetches.
    """
    declare_only_models("core.dim_accounts")
    record_sqlmesh_apply(freshness_db, _ts(2026, 5, 10, 12, 0))
    freshness_db.execute(_INSERT_RAW_PRICE, [_ts(2026, 5, 13, 18, 24)])
    assert TransformService(freshness_db).freshness().pending is True


def test_apply_returns_apply_result_shape(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """apply() returns ApplyResult(applied=True, duration_seconds>=0) on success."""
    from contextlib import contextmanager

    fake_ctx = MagicMock()
    fake_ctx.run.return_value.is_failure = False

    @contextmanager
    def fake_sqlmesh_context(_db: Database):  # type: ignore[no-untyped-def]
        yield fake_ctx

    def fake_seed(_self: object) -> None:
        return None

    def fake_refresh(_db: object) -> None:
        return None

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context",
        fake_sqlmesh_context,
    )
    monkeypatch.setattr(
        "moneybin.services.matching_service.MatchingService.seed_priority",
        fake_seed,
    )
    monkeypatch.setattr(
        "moneybin.services.transform_service.refresh_views",
        fake_refresh,
    )

    result = TransformService(db).apply()

    assert result.applied is True
    assert result.duration_seconds >= 0
    assert result.error is None
    fake_ctx.plan.assert_called_once_with(auto_apply=True, no_prompts=True)


def test_apply_soft_fails_when_run_reports_failure(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """apply() must fail when ctx.run() returns a failure status.

    SQLMesh's Context.run() returns a CompletionStatus and does NOT raise on
    scheduler/audit/model errors — it returns is_failure=True (its own CLI
    checks the status and raises "Run failed"). apply() must detect that and
    surface applied=False, not proceed to the FULL-model restate and falsely
    report success.
    """
    from contextlib import contextmanager

    fake_ctx = MagicMock()
    fake_ctx.run.return_value.is_failure = True

    @contextmanager
    def fake_sqlmesh_context(_db: Database):  # type: ignore[no-untyped-def]
        yield fake_ctx

    def fake_seed(_self: object) -> None:
        return None

    def fake_refresh(_db: object) -> None:
        return None

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context",
        fake_sqlmesh_context,
    )
    monkeypatch.setattr(
        "moneybin.services.matching_service.MatchingService.seed_priority",
        fake_seed,
    )
    monkeypatch.setattr(
        "moneybin.services.transform_service.refresh_views",
        fake_refresh,
    )

    result = TransformService(db).apply()

    assert result.applied is False
    assert result.error is not None
    # A run failure short-circuits before the FULL-model restate: only the
    # definition plan ran, never a second restate plan.
    assert fake_ctx.plan.call_count == 1


def test_apply_soft_fails_with_error_type_on_sqlmesh_exception(
    db: Database, monkeypatch: pytest.MonkeyPatch
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

    def fake_seed(_self: object) -> None:
        return None

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context",
        fake_sqlmesh_context,
    )
    monkeypatch.setattr(
        "moneybin.services.matching_service.MatchingService.seed_priority",
        fake_seed,
    )

    result = TransformService(db).apply()

    assert result.applied is False
    assert result.error == "RuntimeError"
    assert result.duration_seconds >= 0


def test_apply_logs_exception_message_for_debuggability(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """apply() logs the exception MESSAGE so failures are diagnosable.

    The envelope stays type-name-only for privacy (locked by the test above),
    but the local log — PII-masked by SanitizedLogFormatter — must carry the
    detail. Without this, a `SQLMeshError` (e.g. a version-migration prompt)
    is invisible everywhere and undebuggable. Coherent with the match/
    categorize steps in refresh.py, which already log ``{exc}`` + exc_info.
    """
    from contextlib import contextmanager

    detail = "version '0.235.3' ahead of '0.234.1'; run a migration"

    @contextmanager
    def fake_sqlmesh_context(_db: Database):  # type: ignore[no-untyped-def]
        raise RuntimeError(detail)
        yield  # unreachable; satisfies the contextmanager generator contract

    def fake_seed(_self: object) -> None:
        return None

    monkeypatch.setattr(
        "moneybin.services.transform_service.sqlmesh_context",
        fake_sqlmesh_context,
    )
    monkeypatch.setattr(
        "moneybin.services.matching_service.MatchingService.seed_priority",
        fake_seed,
    )

    with caplog.at_level(logging.WARNING, logger="moneybin.services.transform_service"):
        result = TransformService(db).apply()

    assert result.error == "RuntimeError"  # envelope contract unchanged
    assert detail in caplog.text  # the message reached the log


def test_import_service_run_transforms_delegates_to_transform_service(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ImportService.run_transforms() delegates to TransformService.apply()."""
    from moneybin.services.import_service import ImportService
    from moneybin.services.transform_service import ApplyResult

    calls: list[str] = []

    def fake_apply(self: TransformService) -> ApplyResult:
        calls.append("apply")
        return ApplyResult(applied=True, duration_seconds=0.0)

    monkeypatch.setattr(TransformService, "apply", fake_apply)

    result = ImportService(db).run_transforms()

    assert result is True
    assert calls == ["apply"]


def test_status_uninitialized_environment(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    declare_only_models: Callable[..., None],
) -> None:
    """Fresh DB: no SQLMesh env → initialized=False, pending=False."""
    declare_only_models()
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

    s: TransformStatus = TransformService(db).status()

    assert s.environment == "prod"
    assert s.initialized is False
    assert s.last_apply_at is None
    assert s.pending is False


def test_status_initialized_with_finalized_ts(
    db: Database, monkeypatch: pytest.MonkeyPatch
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

    s: TransformStatus = TransformService(db).status()

    assert s.environment == "prod"
    assert s.initialized is True
    assert s.last_apply_at is not None
    assert abs((s.last_apply_at - expected_naive).total_seconds()) < 1.0


def test_plan_no_changes(db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
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

    p = TransformService(db).plan()

    assert p.has_changes is False
    assert p.directly_modified == []
    assert p.indirectly_modified == []
    assert p.added == []
    assert p.removed == []


def test_plan_lists_changed_models(
    db: Database, monkeypatch: pytest.MonkeyPatch
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

    p = TransformService(db).plan()

    assert p.has_changes is True
    assert p.directly_modified == ["core.dim_accounts"]
    assert p.indirectly_modified == ["core.fct_transactions"]


def test_validate_passes_when_plan_builds(
    db: Database, monkeypatch: pytest.MonkeyPatch
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

    v = TransformService(db).validate()

    assert v.valid is True
    assert v.errors == []


def test_validate_reports_errors_on_raise(
    db: Database, monkeypatch: pytest.MonkeyPatch
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

    v = TransformService(db).validate()

    assert v.valid is False
    assert len(v.errors) == 1
    # message is the exception type name, not str(e) — see transform_service.validate
    # docstring for the PII-safety rationale.
    assert v.errors[0]["message"] == "RuntimeError"


def test_audit_aggregates_pass_fail_counts(
    db: Database, monkeypatch: pytest.MonkeyPatch
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

    result = TransformService(db).audit(start="2026-01-01", end="2026-12-31")

    assert result.passed == 1
    assert result.failed == 1
    names = [a["name"] for a in result.audits]
    assert "fct_transactions_pk" in names
    assert "fct_transactions_fk" in names


def test_freshness_pending_and_named_when_a_registered_model_was_never_built(
    freshness_db: Database,
) -> None:
    """An unbuilt model is staleness, and freshness must say which one.

    Breaks under the timestamp-only proxy: a model with no relation has no
    timestamp to compare, so it reads as fresh forever.
    """
    # `core.dim_accounts` below is a registered model no `db init` creates, so
    # its presence is what marks this warehouse built rather than brand new.
    landed = _ts(2026, 5, 13, 18, 24)
    freshness_db.execute(
        "INSERT INTO core.dim_accounts VALUES ('a', ?, ?)",
        [landed, _ts(2026, 5, 13, 19, 0)],
    )
    record_sqlmesh_apply(freshness_db, _ts(2026, 5, 13, 19, 0))
    freshness_db.execute(
        _INSERT_RAW_ACCOUNT, ["a", _ts(2026, 5, 13, 18, 0), landed, "i1"]
    )
    freshness_db.execute(_INSERT_IMPORT, ["i1", "complete", _ts(2026, 5, 13, 18, 30)])

    f = TransformService(freshness_db).freshness()

    assert f.pending is True
    assert "core.fct_transactions" in f.missing_models
    assert "core.dim_accounts" not in f.missing_models


def _manual_entry_db(
    db: Database, declare_only_models: Callable[..., None]
) -> Database:
    """A UTC-pinned DB with real core tables and one applied SQLMesh plan."""
    from tests.moneybin.db_helpers import create_core_tables

    db.execute("SET TimeZone = 'UTC'")
    create_core_tables(db)
    declare_only_models("core.dim_accounts")
    record_sqlmesh_apply(db, _ts(2020, 1, 1))
    return db


def test_freshness_pending_after_a_manual_transaction_is_recorded(
    db: Database, declare_only_models: Callable[..., None]
) -> None:
    """The 2026-07-26 finding: manual entry wrote rows no freshness check read.

    Drives the real write path rather than inserting into
    ``raw.manual_transactions`` directly — a hand-rolled row would still
    pass if the service left the landing column unset.
    """
    from moneybin.services.transaction_service import TransactionService

    manual_db = _manual_entry_db(db, declare_only_models)
    manual_db.execute(
        "INSERT INTO core.dim_accounts (account_id, account_type, source_type) "
        "VALUES ('A1', 'checking', 'manual')"
    )

    TransactionService(manual_db).create_manual_batch(
        [
            {
                "account_id": "A1",
                "amount": Decimal("-12.34"),
                "transaction_date": "2026-04-15",
                "description": "Coffee Shop",
            }
        ],
        actor="cli",
    )

    assert TransformService(manual_db).freshness().pending is True


def test_freshness_pending_after_a_manual_investment_event_is_recorded(
    db: Database, declare_only_models: Callable[..., None]
) -> None:
    """Same finding, the ``investments_record`` half of it."""
    from moneybin.repositories.securities_repo import SecuritiesRepo
    from moneybin.services.investment_service import InvestmentService

    manual_db = _manual_entry_db(db, declare_only_models)
    manual_db.execute(
        "INSERT INTO core.dim_accounts "
        "(account_id, account_type, institution_name, source_type) "
        "VALUES ('acct_brokerage', 'investment', 'Fidelity', 'manual')"
    )
    SecuritiesRepo(manual_db).upsert(
        security_id=None,
        name="Apple Inc.",
        ticker="AAPL",
        security_type="equity",
        actor="cli",
    )

    InvestmentService(manual_db).record_event(
        account_ref="acct_brokerage",
        security_ref="AAPL",
        type_="buy",
        subtype=None,
        trade_date=date(2024, 1, 15),
        quantity=Decimal("10"),
        price=Decimal("150.00"),
        amount=Decimal("-1500.00"),
        fees=None,
        acquired=None,
        basis=None,
        event_group_id=None,
        currency_code="USD",
        description="buy aapl",
        actor="cli",
        created_by="cli",
    )

    assert TransformService(manual_db).freshness().pending is True


def test_pending_scan_set_covers_every_raw_table_the_transforms_read(
    db: Database,
) -> None:
    """The scan set must equal the raw tables SQLMesh models actually read.

    Set equality both ways, not a count and not a subset. A raw table wired
    into a model but absent here is data whose arrival ``pending`` cannot
    see — the exact defect this scan set was widened to fix — and one listed
    here that no model reads is a table whose arrivals no refresh can clear.
    """
    from moneybin.services.transform_service import (
        _RAW_LANDING_COLUMNS,  # pyright: ignore[reportPrivateUsage]  # the guarded list
    )
    from moneybin.sqlmesh_registry import raw_tables_read_by_models

    assert set(_RAW_LANDING_COLUMNS) == set(raw_tables_read_by_models())


def test_every_declared_landing_and_import_column_exists(db: Database) -> None:
    """A declaration that no longer matches the schema must fail, not go quiet.

    Both halves are silent when stale: a renamed landing column makes the
    scan raise (caught and reported as "no raw data"), and an ``import_id``
    that appears on a table declared without one leaves reverted rows
    counting toward staleness forever.
    """
    from moneybin.services.transform_service import (
        _RAW_IMPORT_SCOPED,  # pyright: ignore[reportPrivateUsage]  # the guarded list
        _RAW_LANDING_COLUMNS,  # pyright: ignore[reportPrivateUsage]  # the guarded list
    )

    rows = db.execute(
        "SELECT table_name, column_name FROM duckdb_columns() WHERE schema_name = 'raw'"
    ).fetchall()
    columns: dict[str, set[str]] = {}
    for table, column in rows:
        columns.setdefault(str(table), set()).add(str(column))

    declared_landing = {
        table: {column} for table, column in _RAW_LANDING_COLUMNS.items()
    }
    actual_landing = {
        table: columns.get(table, set()) & {column}
        for table, column in _RAW_LANDING_COLUMNS.items()
    }
    assert actual_landing == declared_landing

    actual_import_scoped = {
        table for table in _RAW_LANDING_COLUMNS if "import_id" in columns.get(table, ())
    }
    assert actual_import_scoped == _RAW_IMPORT_SCOPED


@pytest.mark.fresh_db
def test_freshness_not_pending_on_a_never_built_warehouse(db: Database) -> None:
    """A profile between `db init` and its first refresh is not stale.

    Deliberately runs against the REAL 52-model registry on a database with
    none of them built — the state every new user hits first. Breaks if a
    never-built warehouse is treated as missing-models: `pending` would be
    permanently true and `moneybin system doctor` would exit 1 on a profile
    where nothing is wrong.
    """
    f = TransformService(db).freshness()

    assert f.pending is False
    assert f.missing_models == ()
