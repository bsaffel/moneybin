"""Immutable manual identity routing and legacy whole-operation recovery."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

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


def legacy_repoint(db: Database, *, invalid: bool = False) -> str:
    add_manual_event(db, security_id="b")
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
    before = dict(after, security_id="a")
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
    return op


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
