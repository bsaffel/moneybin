"""Undo distinguishes review and price bindings from investment identity changes."""

from decimal import Decimal

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_services.test_security_links_service import (
    add_disposal,
    add_lot,
    create_core_tables,
)


def _later_selection(db: Database) -> list[tuple[str, Decimal]]:
    create_core_tables(db)
    lot = add_lot(db, security_id="security_a")
    add_disposal(db, "sell", "security_a")
    selections = [(lot, Decimal("2"))]
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id="sell", selections=selections, actor="cli"
    )
    return selections


@pytest.mark.parametrize("status", ["rejected", "accepted"])
def test_account_decision_undo_guards_only_accepted_identity(
    db: Database, status: str
) -> None:
    repo = AccountLinkDecisionsRepo(db)
    repo.insert(
        decision_id="decision_a",
        provisional_account_id="acc_1",
        candidate_account_id="acc_2",
        confidence_score=0.9,
        match_signals={},
        decided_by="auto",
        actor="system",
    )
    with operation() as op:
        repo.update_status(
            "decision_a",
            status=status,
            decided_by="user",
            actor="cli",
            provisional_display_name="First",
            candidate_display_name="Second",
        )
    selections = _later_selection(db)
    if status == "accepted":
        audits = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
        with pytest.raises(UserError) as exc:
            UndoService(db).undo(op, actor="cli")
        assert exc.value.code == error_codes.UNDO_CASCADE_BLOCKED
        assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audits
    else:
        undo = UndoService(db).undo(op, actor="cli")
        assert db.execute(
            "SELECT status FROM app.account_link_decisions WHERE decision_id = ?",
            ["decision_a"],
        ).fetchone() == ("pending",)
        UndoService(db).undo(undo.undo_operation_id, actor="cli")
    assert db.execute(
        "SELECT status FROM app.account_link_decisions WHERE decision_id = ?",
        ["decision_a"],
    ).fetchone() == (status,)
    assert LotSelectionsRepo(db).list_for_disposal("sell") == selections


@pytest.mark.parametrize(
    ("ref_kind", "source_type", "identity"),
    [
        ("tiingo_ticker", "tiingo", False),
        ("coingecko_slug", "coingecko", False),
        ("plaid_security_id", "plaid", True),
        ("manual_investment_transaction_id", "manual", True),
    ],
)
def test_security_link_undo_guards_identity_but_allows_price_bindings(
    db: Database, ref_kind: str, source_type: str, identity: bool
) -> None:
    repo = SecurityLinksRepo(db)
    with operation() as op:
        repo.insert(
            security_id="security_a",
            ref_kind=ref_kind,
            ref_value="test_ref",
            source_type=source_type,
            decided_by="user",
            actor="cli",
        )
    selections = _later_selection(db)
    if identity:
        audits = db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
        with pytest.raises(UserError) as exc:
            UndoService(db).undo(op, actor="cli")
        assert exc.value.code == error_codes.UNDO_CASCADE_BLOCKED
        assert db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone() == audits
    else:
        undo = UndoService(db).undo(op, actor="cli")
        assert (
            repo.lookup(
                ref_kind=ref_kind, ref_value="test_ref", source_type=source_type
            )
            is None
        )
        UndoService(db).undo(undo.undo_operation_id, actor="cli")
    assert (
        repo.lookup(ref_kind=ref_kind, ref_value="test_ref", source_type=source_type)
        == "security_a"
    )
    assert LotSelectionsRepo(db).list_for_disposal("sell") == selections
