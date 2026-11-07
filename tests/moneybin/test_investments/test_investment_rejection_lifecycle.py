"""Durable rejections constrain real SQL assignment until evidence changes or undo."""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.services.audit_service import AuditService
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_account_metadata,
    seed_manual_event,
    seed_plaid_event,
)
from tests.moneybin.test_investments.test_investment_golden_projection import (
    install_golden_models,
    ledger,
)


def _references(proposal: dict[str, Any]) -> frozenset[str]:
    return frozenset(leg["native_reference"] for leg in proposal["legs"])


def _revise(db: Database, native: str, *, day: int, name: str, job: str) -> None:
    txn = SyncInvestmentTransaction(
        investment_transaction_id=native,
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=date(2026, 1, day),
        name=name,
        quantity=Decimal("1"),
        amount=Decimal("100"),
        price=Decimal("100"),
        fees=Decimal("0"),
        type="buy",
        subtype="buy",
    )
    at = datetime(2026, 2, 2, tzinfo=UTC)
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # retain original evidence while delivering a new revision
        [txn], job, at, at
    )


def _constrained_triangle(
    db: Database,
) -> tuple[InvestmentMatchingService, str, dict[str, Any]]:
    """Reject A-C while A-D competes, then extend only the surrounding graph."""
    seed_account_metadata(
        db, "account", native="native_account", origin="second_origin", currency="USD"
    )
    seed_manual_event(db, "a")
    seed_plaid_event(db, "c")
    seed_plaid_event(db, "d")
    install_comparison_models(db)
    InvestmentMembershipService(db).reconcile(actor="test")
    install_golden_models(db)
    matching = InvestmentMatchingService(db)
    matching.run()
    initial = matching.pending()
    assert {_references(row) for row in initial} == {
        frozenset({"a", "c"}),
        frozenset({"a", "d"}),
    }
    assert all(row["is_competing"] for row in initial)
    rejected = next(row for row in initial if _references(row) == frozenset({"a", "c"}))
    with operation() as rejection_operation:
        matching.decide(rejected["proposal_id"], decision="reject", actor="test")
    assert matching.pending() == []
    events = AuditService(db).events_for_operation(rejection_operation)
    assert sum(event.action == "investment_match.reject" for event in events) == 1
    assert matching.run().suppressed == 1
    assert [_references(row) for row in matching.pending()] == [frozenset({"a", "d"})]
    prior_ad = matching.pending()[0]["proposal_id"]

    # D leaves the candidate window. A and C retain their exact observations,
    # while B's distinct Source origin makes A-B-C a fully eligible triangle.
    _revise(db, "d", day=25, name="Unrelated later trade", job="sync_d_later")
    seed_plaid_event(db, "b", source_origin="second_origin")
    result = matching.run()
    assert result.suppressed == 2  # Exact A-C and its unchanged induced A-B-C superset.
    constrained = matching.pending()
    assert {_references(row) for row in constrained} == {
        frozenset({"a", "b"}),
        frozenset({"b", "c"}),
    }
    assert all(row["is_competing"] for row in constrained)
    assert all(not {"a", "c"} <= _references(row) for row in constrained)
    assert any(
        row["proposal_id"] == prior_ad and row["status"] == "stale"
        for row in matching.history()
    )
    retained = next(
        row
        for row in matching.history()
        if row["proposal_id"] == rejected["proposal_id"]
    )
    assert retained["status"] == "rejected"
    assert retained["relationship_fingerprint"] == rejected["relationship_fingerprint"]
    assert retained["legs"] == rejected["legs"]
    assert retained["evidence"] == rejected["evidence"]
    assert retained["decided_at"] is not None
    assert retained["accepted_at"] is None
    return matching, rejection_operation, retained


def test_rejection_excludes_unchanged_superset_and_undo_restores_assignment_space(
    comparison_db: Database,
) -> None:
    # Treating rejection as a graph-wide veto, or as a pairwise approximation,
    # would either hide the other pairs or let the rejected superset return.
    db = comparison_db
    matching, rejection_operation, rejected = _constrained_triangle(db)
    constrained_ids = {row["proposal_id"] for row in matching.pending()}
    before = ledger(db)
    assert len(before) == 3
    assert sorted(row["amount"] for row in before) == [Decimal("-100")] * 3
    members = InvestmentEventMembersRepo(db).history()
    raw = db.execute(
        """SELECT * FROM raw.plaid_investment_transactions
           ORDER BY investment_transaction_id, observation_version"""
    ).fetchall()
    result = UndoService(db).undo(rejection_operation, actor="test")
    assert result.undone_operation_id == rejection_operation
    assert matching.pending() == []
    assert not any(row["status"] == "rejected" for row in matching.history())
    assert all(
        next(row for row in matching.history() if row["proposal_id"] == proposal_id)[
            "status"
        ]
        == "stale"
        for proposal_id in constrained_ids
    )
    reopened = InvestmentMatchingService(db)
    assert reopened.run().suppressed == 0
    pending = reopened.pending()
    assert len(pending) == 1
    assert _references(pending[0]) == {"a", "b", "c"}
    assert not pending[0]["is_competing"]
    assert pending[0]["status"] == "pending"
    assert pending[0]["proposal_id"] not in constrained_ids
    assert pending[0]["proposal_id"] != rejected["proposal_id"]
    # Rejection and its undo only change review constraints, never membership.
    assert ledger(db) == before
    assert InvestmentEventMembersRepo(db).history() == members
    assert (
        db.execute(
            """SELECT * FROM raw.plaid_investment_transactions
           ORDER BY investment_transaction_id, observation_version"""
        ).fetchall()
        == raw
    )


@pytest.mark.parametrize("drift", ["revision", "inherited_currency"])
def test_changed_rejected_relationship_becomes_reviewable_without_erasing_rejection(
    comparison_db: Database, drift: str
) -> None:
    # Persisted negative constraints bind exact evidence and canonical inputs;
    # keeping the same Source keys alone must not reject a changed relationship.
    db = comparison_db
    matching, _, rejected = _constrained_triangle(db)
    constrained_ids = {row["proposal_id"] for row in matching.pending()}
    before = ledger(db)
    members = InvestmentEventMembersRepo(db).history()
    if drift == "revision":
        _revise(db, "c", day=10, name="New exact observation", job="sync_c_revision")
    else:
        db.execute("UPDATE core.dim_accounts SET currency_code = 'CAD'")
    reopened = InvestmentMatchingService(db)
    assert reopened.run().suppressed == 0
    pending = reopened.pending()
    assert len(pending) == 1
    assert _references(pending[0]) == {"a", "b", "c"}
    assert not pending[0]["is_competing"]
    assert pending[0]["status"] == "pending"
    assert pending[0]["proposal_id"] not in constrained_ids
    assert all(
        next(row for row in reopened.history() if row["proposal_id"] == proposal_id)[
            "status"
        ]
        == "stale"
        for proposal_id in constrained_ids
    )
    current_c = next(
        leg for leg in pending[0]["legs"] if leg["native_reference"] == "c"
    )
    old_c = next(leg for leg in rejected["legs"] if leg["native_reference"] == "c")
    assert current_c["source_event_key"] == old_c["source_event_key"]
    if drift == "revision":
        assert current_c["observation_version"] != old_c["observation_version"]
        assert current_c["description"] == "New exact observation"
        assert (
            Decimal(current_c["amount"]) == Decimal(old_c["amount"]) == Decimal("-100")
        )
    else:
        assert current_c["observation_version"] == old_c["observation_version"]
        assert current_c["currency_code"] == "CAD"
        assert old_c["currency_code"] == "USD"
    assert (
        next(
            row
            for row in reopened.history()
            if row["proposal_id"] == rejected["proposal_id"]
        )
        == rejected
    )
    assert ledger(db) == before
    assert InvestmentEventMembersRepo(db).history() == members
