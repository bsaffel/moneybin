"""Immutable manual identity routing and legacy whole-operation recovery."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.manual_investment_transactions_repo import (
    ManualInvestmentTransactionsRepo,
)
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.audit_service import AuditService
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_services.test_security_links_service import add_manual_event


def raw_observations(db: Database) -> list[tuple[Any, ...]]:
    return db.execute("SELECT * FROM raw.manual_investment_transactions").fetchall()


def add_security_route(db: Database, security: str, source: str = "manual_buy") -> None:
    SecurityLinksRepo(db).insert(
        security_id=security,
        ref_kind="manual_investment_transaction_id",
        ref_value=source,
        source_type="manual",
        decided_by="user",
        actor="cli",
    )


def test_manual_enumeration_uses_current_routes(db: Database) -> None:
    add_manual_event(db, security_id="a")
    add_manual_event(db, security_id="b", source_transaction_id="from_b")
    add_manual_event(db, security_id="a", source_transaction_id="elsewhere")
    add_security_route(db, "b")
    add_security_route(db, "c", "elsewhere")
    repo = ManualInvestmentTransactionsRepo(db)
    assert repo.list_ids_for_security("b") == ["from_b", "manual_buy"]
    assert repo.list_ids_for_security("a") == []


def legacy_repoint(
    db: Database,
    *,
    invalid: bool = False,
    captured_created_at: str | None = None,
    before_created_at: str | None = None,
    before_security_id: str = "a",
) -> str:
    add_manual_event(db, security_id="b")
    if captured_created_at is not None:
        db.execute(
            "UPDATE raw.manual_investment_transactions SET created_at = ?",
            [datetime(2024, 7, 1, 16, tzinfo=UTC)],
        )
    cursor = db.execute(
        "SELECT * FROM raw.manual_investment_transactions WHERE source_transaction_id = ?",
        ["manual_buy"],
    )
    columns = [str(column[0]) for column in cursor.description]
    row = cursor.fetchone()
    assert row is not None
    after = {
        column: value.isoformat()
        if isinstance(value, date)
        else str(value)
        if isinstance(value, Decimal)
        else value
        for column, value in zip(columns, row, strict=True)
    }
    if captured_created_at is not None:
        after["created_at"] = captured_created_at
    before = dict(after, security_id=before_security_id)
    if before_created_at is not None:
        before["created_at"] = before_created_at
    if invalid:
        before["amount"] = "-12.00"
    with operation() as op:
        AuditService(db).record_audit_event(
            action="manual_investment.repoint_security",
            target=("raw", "manual_investment_transactions", "manual_buy"),
            before=before,
            after=after,
            actor="cli",
        )
    if captured_created_at is not None:
        db.execute(
            "UPDATE app.audit_log SET occurred_at = ? WHERE operation_id = ?",
            [datetime(2025, 1, 1, tzinfo=UTC), op],
        )
    return op


def _legacy_manual_time_policy(db: Database, count: int) -> None:
    rule = {
        "schema": "raw",
        "table": "manual_investment_transactions",
        "column": "created_at",
        "writer_era": "local-naive",
        "zone": "America/New_York",
        "fold": None,
        "recorded_from": "2020-01-01T00:00:00.000000Z",
        "recorded_until": "2026-01-01T00:00:00.000000Z",
    }
    assert db.execute(
        "UPDATE app.utc_upgrade_provenance SET rules = ? WHERE contract_version = 2",
        [json.dumps([rule] * count)],
    ).fetchone() == (1,)


def _legacy_manual_image_policy(db: Database, op: str) -> None:
    _legacy_manual_time_policy(db, 1)
    event = AuditService(db).events_for_operation(op)[0]
    record = db.execute("SELECT rules FROM app.utc_upgrade_provenance").fetchone()
    assert record is not None
    rule = json.loads(record[0])[0]
    db.execute(
        "UPDATE app.utc_upgrade_provenance SET rules = ?",
        [
            json.dumps([
                rule | {"audit_id": event.audit_id, "image_side": "before"},
                rule
                | {
                    "audit_id": event.audit_id,
                    "image_side": "after",
                    "zone": "UTC",
                    "writer_era": "utc-naive",
                },
            ])
        ],
    )


@pytest.mark.parametrize(
    "captured_created_at,rule_count,mixed_images",
    [
        ("2024-07-01T12:00:00-04:00", 0, False),
        ("2024-07-01T12:00:00", 1, False),
        ("2024-07-01T16:00:00", 1, True),
    ],
)
def test_legacy_identity_recovery_compares_instants_without_rewriting_evidence(
    db: Database,
    captured_created_at: str,
    rule_count: int,
    mixed_images: bool,
) -> None:
    op = legacy_repoint(
        db,
        captured_created_at=captured_created_at,
        before_created_at="2024-07-01T12:00:00" if mixed_images else None,
    )
    _legacy_manual_time_policy(db, rule_count)
    if mixed_images:
        _legacy_manual_image_policy(db, op)
    frozen = raw_observations(db)
    evidence = db.execute(
        "SELECT before_value, after_value FROM app.audit_log WHERE operation_id = ?",
        [op],
    ).fetchall()

    undo = UndoService(db).undo(op, actor="cli")

    assert (
        SecurityLinksRepo(db).lookup(
            ref_kind="manual_investment_transaction_id",
            ref_value="manual_buy",
            source_type="manual",
        )
        == "a"
    )
    assert raw_observations(db) == frozen
    assert (
        db.execute(
            "SELECT before_value, after_value FROM app.audit_log WHERE operation_id = ?",
            [op],
        ).fetchall()
        == evidence
    )
    UndoService(db).undo(undo.undo_operation_id, actor="cli")
    assert raw_observations(db) == frozen
    assert (
        SecurityLinksRepo(db).lookup(
            ref_kind="manual_investment_transaction_id",
            ref_value="manual_buy",
            source_type="manual",
        )
        is None
    )


@pytest.mark.parametrize(
    "captured_created_at,rule_count",
    [
        ("2024-07-01T12:00:00", 0),
        ("2024-07-01T12:00:00", 2),
        ("2024-07-01T13:00:00-04:00", 0),
    ],
)
def test_legacy_identity_recovery_refuses_uncertain_or_changed_instants_atomically(
    db: Database, captured_created_at: str, rule_count: int
) -> None:
    op = legacy_repoint(db, captured_created_at=captured_created_at)
    _legacy_manual_time_policy(db, rule_count)
    frozen = raw_observations(db)
    audit_count = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()

    with pytest.raises(UserError) as exc:
        UndoService(db).undo(op, actor="cli")

    assert exc.value.code == error_codes.RECOVERY_NO_PATH
    assert raw_observations(db) == frozen
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audit_count
    assert db.execute("SELECT COUNT(*) FROM app.security_links").fetchone() == (0,)


def test_legacy_manual_repo_skips_semantically_equal_images(db: Database) -> None:
    op = legacy_repoint(
        db,
        captured_created_at="2024-07-01T16:00:00",
        before_created_at="2024-07-01T12:00:00",
        before_security_id="b",
    )
    _legacy_manual_image_policy(db, op)
    event = AuditService(db).events_for_operation(op)[0]
    frozen = raw_observations(db)
    audit_count = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    assert ManualInvestmentTransactionsRepo(db).undo_event(event, actor="cli") is None
    assert raw_observations(db) == frozen
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audit_count


def test_legacy_undo_redo_routes_without_restoring_raw(db: Database) -> None:
    op = legacy_repoint(db)
    frozen = raw_observations(db)
    undo = UndoService(db).undo(op, actor="cli")
    assert raw_observations(db) == frozen
    assert (
        SecurityLinksRepo(db).lookup(
            ref_kind="manual_investment_transaction_id",
            ref_value="manual_buy",
            source_type="manual",
        )
        == "a"
    )
    inverses = AuditService(db).events_for_operation(undo.undo_operation_id)
    assert inverses and all(e.target_table == "security_links" for e in inverses)
    assert all(e.is_undo and e.undoes_operation_id == op for e in inverses)
    UndoService(db).undo(undo.undo_operation_id, actor="cli")
    assert raw_observations(db) == frozen
    assert (
        SecurityLinksRepo(db).lookup(
            ref_kind="manual_investment_transaction_id",
            ref_value="manual_buy",
            source_type="manual",
        )
        is None
    )


def test_legacy_nonidentity_mutation_refuses_atomically(db: Database) -> None:
    op = legacy_repoint(db, invalid=True)
    frozen = raw_observations(db)
    before = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError):
        UndoService(db).undo(op, actor="cli")
    assert raw_observations(db) == frozen
    assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == before


def test_later_manual_link_blocks_legacy_undo(db: Database) -> None:
    legacy = legacy_repoint(db)
    with operation() as later:
        add_security_route(db, "c")
    assert later in UndoService(db).cascade_blockers(legacy)
    with pytest.raises(UserError):
        UndoService(db).undo(legacy, actor="cli")
    UndoService(db).undo(later, actor="cli")
    UndoService(db).undo(legacy, actor="cli")
    assert raw_observations(db)[0][5] == "b"
