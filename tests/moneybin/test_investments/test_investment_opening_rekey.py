"""Opening identity dependencies rekey curation without changing Golden identity.

Snapshot three/$30 minus buy two/$200 plus sell two/$300 leaves opening
three/$30. Selecting one opening share and one bought share realizes $190
($140 + $50), leaving three shares/$120; FIFO would instead realize $280.
"""

import json
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments import event_openings
from moneybin.investments.cost_basis import compute_lot_id
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_link_decisions_repo import SecurityLinkDecisionsRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services import investment_membership_service
from moneybin.services.audit_service import AuditService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.investment_service import InvestmentService
from moneybin.services.mutation_context import operation
from moneybin.services.security_links_service import SecurityLinksService
from moneybin.services.transform_service import TransformService
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_investments.comparison_helpers import seed_plaid_event
from tests.moneybin.test_investments.test_investment_opening_lifecycle import (
    _snapshot,  # pyright: ignore[reportPrivateUsage]  # reuse retained synthetic holdings fixture
)

pytestmark = pytest.mark.integration


def _opening(db: Database) -> dict[str, Any]:
    rows = [
        row
        for row in InvestmentEventMembersRepo(db).active_members()
        if row["member_kind"] == "opening_lot_reconstruction"
    ]
    assert len(rows) == 1
    return rows[0]


def _position(db: Database) -> dict[str, dict[str, Any]]:
    for identity, creator in (("provisional", "plaid"), ("security", "user")):
        SecuritiesRepo(db).upsert(
            security_id=identity,
            name="Synthetic Security",
            security_type="equity",
            cost_basis_method="specific",
            created_by=creator,
            actor="test",
        )
    # Initial source binding is fixture input; the later merge must use its service.
    link = db.execute("""SELECT link_id FROM app.security_links
        WHERE ref_value = 'native_security' AND status = 'accepted'""").fetchone()
    assert link is not None
    SecurityLinksRepo(db).repoint(
        link_id=link[0], new_security_id="provisional", decided_by="user", actor="test"
    )
    seed_plaid_event(db, "buy", quantity="2", amount="200")
    seed_plaid_event(
        db,
        "sell",
        type_="sell",
        subtype="sell",
        quantity="-2",
        amount="-300",
        price="150",
        day=20,
    )
    _snapshot(db, "first", [("native_account", "3", "30")])
    built = TransformService(db).apply()
    assert built.applied, built.error
    rows = {
        row["native_reference"]: row
        for row in InvestmentMembershipService(db).project()
        if row["subtype"] != "opening_bootstrap"
    }
    rows["opening"] = _opening(db)
    opening_lot, buy_lot = _lot_ids(rows, "provisional")
    InvestmentService(db).select_lots(
        rows["sell"]["investment_transaction_id"],
        [(opening_lot, Decimal(1)), (buy_lot, Decimal(1))],
        actor="test",
    )
    _assert_finances(db, rows, "provisional")
    return rows


def _lot_ids(rows: dict[str, dict[str, Any]], security: str) -> tuple[str, str]:
    return (
        compute_lot_id(
            "account",
            security,
            date(2024, 1, 1),
            rows["opening"]["investment_transaction_id"],
        ),
        compute_lot_id(
            "account",
            security,
            date(2026, 1, 10),
            rows["buy"]["investment_transaction_id"],
        ),
    )


def _assert_finances(
    db: Database, rows: dict[str, dict[str, Any]], security: str
) -> None:
    opening_lot, buy_lot = _lot_ids(rows, security)
    disposal = rows["sell"]["investment_transaction_id"]
    assert LotSelectionsRepo(db).list_for_disposal(disposal) == sorted([
        (opening_lot, Decimal(1)),
        (buy_lot, Decimal(1)),
    ])
    assert set(
        db.execute("""SELECT lot_id, security_id, original_quantity,
        remaining_quantity, cost_basis_total, cost_basis_remaining,
        cost_basis_method, basis_incomplete FROM core.fct_investment_lots""").fetchall()
    ) == {
        (
            opening_lot,
            security,
            Decimal(3),
            Decimal(2),
            Decimal(30),
            Decimal(20),
            "specific",
            False,
        ),
        (
            buy_lot,
            security,
            Decimal(2),
            Decimal(1),
            Decimal(200),
            Decimal(100),
            "specific",
            False,
        ),
    }
    assert set(
        db.execute("""SELECT disposal_txn_id, lot_id, quantity, proceeds,
        cost_basis, gain_loss, cost_basis_method, basis_incomplete
        FROM core.fct_realized_gains""").fetchall()
    ) == {
        (
            disposal,
            opening_lot,
            Decimal(1),
            Decimal(150),
            Decimal(10),
            Decimal(140),
            "specific",
            False,
        ),
        (
            disposal,
            buy_lot,
            Decimal(1),
            Decimal(150),
            Decimal(100),
            Decimal(50),
            "specific",
            False,
        ),
    }
    assert db.execute("""SELECT security_id, quantity, cost_basis
        FROM core.dim_holdings""").fetchall() == [(security, Decimal(3), Decimal(120))]


def test_opening_security_merge_rekeys_complete_election_and_undo(
    comparison_db: Database,
) -> None:
    db = comparison_db
    before = _position(db)
    opening_before = before["opening"]
    decision = SecurityLinkDecisionsRepo(db).insert(
        ref_kind="plaid_security_id",
        ref_value="native_security",
        source_type="plaid",
        candidate_security_id="security",
        actor="test",
    )
    assert decision.target_id is not None
    with operation() as merged:
        SecurityLinksService(db, actor="test").accept_merge(
            decision.target_id, into="security"
        )
    after = _opening(db)
    for key in ("event_group_id", "investment_transaction_id"):
        assert after[key] == opening_before[key]
    assert after["observation_version"] != opening_before["observation_version"]
    assert after["bound_security_id"] == "security"
    assert (
        json.loads(after["reconstruction_evidence"])["snapshot"]
        == (json.loads(opening_before["reconstruction_evidence"])["snapshot"])
    )
    assert _lot_ids(before, "security") != _lot_ids(before, "provisional")
    _assert_finances(db, before, "security")
    events = AuditService(db).events_for_operation(merged)
    selected = [event for event in events if event.action == "lot_selections.set"]
    assert len(selected) == 1
    assert selected[0].target_id == before["sell"]["investment_transaction_id"]
    for payload, security in (
        (selected[0].before_value, "provisional"),
        (selected[0].after_value, "security"),
    ):
        assert payload is not None
        assert {
            (row["lot_id"], Decimal(row["quantity"])) for row in payload["selections"]
        } == {(lot, Decimal(1)) for lot in _lot_ids(before, security)}
    assert db.execute(
        """SELECT status FROM app.security_link_decisions
        WHERE decision_id = ?""",
        [decision.target_id],
    ).fetchone() == ("accepted",)
    undone = UndoService(db).undo(merged, actor="test")
    restored = _opening(db)
    for key in ("event_group_id", "investment_transaction_id", "bound_security_id"):
        assert restored[key] == opening_before[key]
    _assert_finances(db, before, "provisional")
    UndoService(db).undo(undone.undo_operation_id, actor="test")
    _assert_finances(db, before, "security")


def test_unsupported_opening_algorithm_refuses_atomically_without_losing_selection(
    comparison_db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db = comparison_db
    before = _position(db)
    initial = before["opening"]
    evidence = json.loads(initial["reconstruction_evidence"])
    assert evidence["algorithm_version"] == event_openings.ALGORITHM_VERSION
    assert evidence["snapshot"] == {
        "account_id": "native_account",
        "source_origin": "origin",
        "source_file": "first",
    }
    history = InvestmentEventMembersRepo(db).history()
    disposal = before["sell"]["investment_transaction_id"]
    selections = LotSelectionsRepo(db).list_for_disposal(disposal)
    with monkeypatch.context() as unavailable:
        version = event_openings.ALGORITHM_VERSION + "_unsupported"
        unavailable.setattr(event_openings, "ALGORITHM_VERSION", version)
        unavailable.setattr(investment_membership_service, "ALGORITHM_VERSION", version)
        with operation() as refused:
            built = TransformService(db).apply()
        assert not built.applied
        assert built.error == "Opening reconstruction algorithm is unavailable."
        assert built.investment_match is not None
        assert built.investment_match.error_code == "recovery_no_path"
        with pytest.raises(UserError) as caught:
            InvestmentMembershipService(db).project()
        assert caught.value.code == "recovery_no_path"
        assert InvestmentEventMembersRepo(db).history() == history
        assert _opening(db) == initial
        assert LotSelectionsRepo(db).list_for_disposal(disposal) == selections
        assert AuditService(db).events_for_operation(refused) == []
    # Restoring the available evaluator recovers the same reviewed projection.
    _assert_finances(db, before, "provisional")
    built = TransformService(db).apply()
    assert built.applied, built.error
    assert InvestmentEventMembersRepo(db).history() == history
    _assert_finances(db, before, "provisional")
