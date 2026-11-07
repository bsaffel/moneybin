"""A reviewed whole event atomically becomes exact Golden membership."""

from typing import Any

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.services.audit_service import AuditService
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.mutation_context import operation
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


def registered_proposal(
    db: Database,
) -> tuple[InvestmentMatchingService, str, dict[str, Any]]:
    seed_manual_event(db, "manual")
    install_comparison_models(db)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    oldest = membership.project()[0]
    seed_plaid_event(db, "native")
    membership.reconcile(actor="test")
    matching = InvestmentMatchingService(db)
    matching.run()
    assert len(matching.pending()) == 1
    return matching, matching.pending()[0]["proposal_id"], oldest


def test_accept_preserves_oldest_ids_and_exact_member_bindings(
    comparison_db: Database,
) -> None:
    db = comparison_db
    matching, proposal_id, oldest = registered_proposal(db)
    raw = db.execute(
        "SELECT COUNT(*) FROM raw.plaid_investment_transactions"
    ).fetchone()
    with operation() as op:
        matching.decide(proposal_id, decision="accept", actor="test")
    rows = InvestmentMembershipService(db).project()
    assert len(rows) == 1
    assert rows[0]["event_group_id"] == oldest["event_group_id"]
    assert rows[0]["investment_transaction_id"] == oldest["investment_transaction_id"]
    assert rows[0]["source_type"] == "plaid"
    assert rows[0]["provider_type"] == "buy"
    active = InvestmentEventMembersRepo(db).active_members()
    assert len(active) == 2
    assert {row["decision_id"] for row in active} == {proposal_id}
    assert {row["bound_account_id"] for row in active} == {"account"}
    assert {row["bound_security_id"] for row in active} == {"security"}
    assert {row["bound_effective_currency"] for row in active} == {"USD"}
    events = AuditService(db).events_for_operation(op)
    assert sum(event.action == "investment_match.accept" for event in events) == 1
    root = next(event for event in events if event.action == "investment_match.accept")
    assert root.context_json is not None
    assert len(root.context_json["prior_membership_ids"]) == 2
    assert all(
        event.parent_audit_id == root.audit_id for event in events if event is not root
    )
    assert matching.history()[0]["status"] == "accepted"
    assert (
        db.execute("SELECT COUNT(*) FROM raw.plaid_investment_transactions").fetchone()
        == raw
    )


def test_acceptance_audit_failure_rolls_back_decision_and_topology(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.repositories.investment_match_decisions_repo import (
        InvestmentMatchDecisionsRepo,
    )

    db = comparison_db
    matching, proposal_id, _oldest = registered_proposal(db)
    before = InvestmentEventMembersRepo(db).history()

    def fail(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("injected acceptance audit failure")

    monkeypatch.setattr(InvestmentMatchDecisionsRepo, "_emit_audit", fail)
    with operation() as op:
        with pytest.raises(RuntimeError, match="injected"):
            matching.decide(proposal_id, decision="accept", actor="test")
    assert matching.pending()[0]["status"] == "pending"
    assert InvestmentEventMembersRepo(db).history() == before
    assert AuditService(db).events_for_operation(op) == []


def test_accept_requires_registered_complete_membership(
    comparison_db: Database,
) -> None:
    db = comparison_db
    seed_manual_event(db, "manual")
    seed_plaid_event(db, "native")
    install_comparison_models(db)
    matching = InvestmentMatchingService(db)
    matching.run()
    before = matching.pending()
    with operation() as op:
        with pytest.raises(UserError, match="membership"):
            matching.decide(before[0]["proposal_id"], decision="accept", actor="test")
    assert matching.pending() == before
    assert AuditService(db).events_for_operation(op) == []


def test_explicit_date_choice_persists_exact_provenance_and_keeps_representative(
    comparison_db: Database,
) -> None:
    from datetime import date

    from moneybin.repositories.investment_match_field_resolutions_repo import (
        InvestmentMatchFieldResolutionsRepo,
    )

    db = comparison_db
    seed_manual_event(db, "manual", day=9)
    seed_plaid_event(db, "native", day=10, explicit_date=True)
    install_comparison_models(db)
    InvestmentMembershipService(db).reconcile(actor="test")
    matching = InvestmentMatchingService(db)
    matching.run()
    proposal = matching.pending()[0]
    conflict = next(c for c in proposal["field_choices"] if c["field"] == "trade_date")
    choice = next(c for c in conflict["choices"] if c["source_type"] == "manual")
    with operation():
        matching.decide(
            proposal["proposal_id"],
            decision="accept",
            actor="test",
            field_choices=[
                {
                    "conflict_id": conflict["conflict_id"],
                    "choice_id": choice["choice_id"],
                }
            ],
        )
    row = InvestmentMembershipService(db).project()[0]
    assert row["trade_date"] == date(2026, 1, 9)
    assert row["source_type"] == "plaid"
    resolutions = InvestmentMatchFieldResolutionsRepo(db).active_resolutions()
    assert len(resolutions) == 1
    assert resolutions[0]["field_name"] == "trade_date"
    assert resolutions[0]["native_reference"] == "manual"
    assert resolutions[0]["observation_version"] == choice["observation_version"]


def test_accepted_metric_is_published_only_after_commit(
    comparison_db: Database,
) -> None:
    from prometheus_client import REGISTRY

    matching, proposal_id, _oldest = registered_proposal(comparison_db)
    eligible = str(matching.pending()[0]["auto_eligible"]).lower()
    labels = {"decision": "accepted", "auto_eligible": eligible}
    before = (
        REGISTRY.get_sample_value("moneybin_investment_match_decisions_total", labels)
        or 0
    )
    with operation():
        matching.decide(proposal_id, decision="accept", actor="test")
    assert (
        REGISTRY.get_sample_value("moneybin_investment_match_decisions_total", labels)
        or 0
    ) == before + 1


def test_review_batch_accept_plan_reports_acceptance(comparison_db: Database) -> None:
    from moneybin.protocol.write_contracts import (
        InvestmentMatchDecisionRequest,
    )
    from moneybin.services.review_decisions_service import ReviewDecisionsService

    _matching, proposal_id, _oldest = registered_proposal(comparison_db)
    request = InvestmentMatchDecisionRequest(
        kind="investment_match", decision_id=proposal_id, decision="accept"
    )
    batch = ReviewDecisionsService(comparison_db, actor="test")
    assert batch.plan_ordinary([request])[0].status == "accepted"


def test_acceptance_remaps_complete_election_from_consolidated_acquisition(
    comparison_db: Database,
) -> None:
    from datetime import date
    from decimal import Decimal

    from moneybin.investments.cost_basis import compute_lot_id
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo

    db = comparison_db
    seed_manual_event(db, "manual", quantity="2", amount="-200")
    seed_manual_event(db, "sell", type_="sell", quantity="-1", amount="100", day=20)
    install_comparison_models(db)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    seed_plaid_event(db, "native", quantity="2", amount="200")
    membership.reconcile(actor="test")
    rows = membership.project()
    plaid = next(r for r in rows if r["source_type"] == "plaid")
    manual = next(r for r in rows if r["native_reference"] == "manual")
    sell = next(r for r in rows if r["type"] == "sell")
    old_lot = compute_lot_id(
        "account", "security", date(2026, 1, 10), plaid["investment_transaction_id"]
    )
    new_lot = compute_lot_id(
        "account", "security", date(2026, 1, 10), manual["investment_transaction_id"]
    )
    selections = LotSelectionsRepo(db)
    selections.set_for_disposal(
        investment_transaction_id=sell["investment_transaction_id"],
        selections=[(old_lot, Decimal(1))],
        actor="test",
    )
    matching = InvestmentMatchingService(db)
    matching.run()
    with operation() as op:
        matching.decide(
            matching.pending()[0]["proposal_id"], decision="accept", actor="test"
        )
    assert selections.list_for_disposal(sell["investment_transaction_id"]) == [
        (new_lot, Decimal(1))
    ]
    events = AuditService(db).events_for_operation(op)
    curation = next(e for e in events if e.action == "lot_selections.set")
    assert curation.before_value is not None
    assert curation.before_value["selections"] == [
        {"lot_id": old_lot, "quantity": "1.0000000000"}
    ]


def test_acceptance_recomputes_opening_gap_before_any_publication(
    comparison_db: Database,
) -> None:
    from datetime import UTC, date, datetime
    from decimal import Decimal

    from moneybin.connectors.sync_models import SyncInvestmentTransaction
    from moneybin.extractors.plaid.extractor import PlaidExtractor
    from moneybin.investments.cost_basis import compute_lot_id
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
    from tests.moneybin.test_investments.test_investment_opening_membership import (
        seed_snapshot,
    )

    db = comparison_db
    seed_manual_event(db, "manual", day=1)
    seed_manual_event(db, "sell", type_="sell", quantity="-5", amount="500", day=2)
    txn = SyncInvestmentTransaction(
        investment_transaction_id="native",
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=date(2025, 12, 31),
        name="Aggregator context",
        quantity=Decimal(1),
        amount=Decimal(100),
        price=Decimal(100),
        fees=Decimal(0),
        type="buy",
        subtype="buy",
    )
    at = datetime(2026, 2, 1, tzinfo=UTC)
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # retained synthetic observation
        [txn], "original", at, at
    )
    seed_snapshot(db, "first")
    install_comparison_models(db)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    rows = membership.project()
    opening = next(r for r in rows if r["subtype"] == "opening_bootstrap")
    sell = next(r for r in rows if r["type"] == "sell")
    assert opening["quantity"] == Decimal(5)
    lot = compute_lot_id(
        "account", "security", date(2025, 12, 31), opening["investment_transaction_id"]
    )
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id=sell["investment_transaction_id"],
        selections=[(lot, Decimal(5))],
        actor="test",
    )
    matching = InvestmentMatchingService(db)
    matching.run()
    before = InvestmentEventMembersRepo(db).history()
    proposal = matching.pending()[0]
    with operation() as op:
        with pytest.raises(UserError, match="selection"):
            matching.decide(proposal["proposal_id"], decision="accept", actor="test")
    assert matching.pending()[0]["proposal_id"] == proposal["proposal_id"]
    assert InvestmentEventMembersRepo(db).history() == before
    assert membership.project() == rows
    assert AuditService(db).events_for_operation(op) == []


def test_review_acceptance_binds_complete_saved_curation_state(
    comparison_db: Database,
) -> None:
    from datetime import date
    from decimal import Decimal

    from moneybin.investments.cost_basis import compute_lot_id
    from moneybin.protocol.write_contracts import (
        InvestmentMatchDecisionRequest,
    )
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
    from moneybin.services.review_decisions_service import ReviewDecisionsService

    db = comparison_db
    matching, proposal_id, _oldest = registered_proposal(db)
    seed_manual_event(db, "sell", type_="sell", quantity="-1", amount="100", day=20)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    rows = membership.project()
    buy = next(r for r in rows if r["source_type"] == "plaid")
    sell = next(r for r in rows if r["type"] == "sell")
    request = InvestmentMatchDecisionRequest(
        kind="investment_match", decision_id=proposal_id, decision="accept"
    )
    batch = ReviewDecisionsService(db, actor="test")
    before = batch.plan_ordinary([request])
    lot = compute_lot_id(
        "account", "security", date(2026, 1, 10), buy["investment_transaction_id"]
    )
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id=sell["investment_transaction_id"],
        selections=[(lot, Decimal(1))],
        actor="test",
    )
    after = batch.plan_ordinary([request])
    assert before[0].before_state != after[0].before_state
    assert matching.pending()[0]["proposal_id"] == proposal_id


def test_third_source_replacement_retires_whole_prior_match(
    comparison_db: Database,
) -> None:
    from moneybin.repositories.account_links_repo import AccountLinksRepo

    db = comparison_db
    matching, original_id, oldest = registered_proposal(db)
    with operation():
        matching.decide(original_id, decision="accept", actor="test")
    AccountLinksRepo(db).insert(
        link_id="other_route",
        account_id="account",
        ref_kind="source_native",
        ref_value="native_account",
        source_type="plaid",
        source_origin="other",
        decided_by="user",
        actor="test",
    )
    seed_plaid_event(db, "third", source_origin="other")
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    matching.run()
    replacement = matching.pending()[0]
    assert replacement["supersedes_decision_ids"] == [original_id]
    with operation() as op:
        matching.decide(replacement["proposal_id"], decision="accept", actor="test")
    assert len(membership.project()) == 1
    assert membership.project()[0]["event_group_id"] == oldest["event_group_id"]
    active = InvestmentEventMembersRepo(db).active_members()
    assert len(active) == 3
    assert {m["decision_id"] for m in active} == {replacement["proposal_id"]}
    statuses = {r["proposal_id"]: r["status"] for r in matching.history()}
    assert statuses[original_id] == "reversed"
    assert statuses[replacement["proposal_id"]] == "accepted"
    events = AuditService(db).events_for_operation(op)
    assert sum(e.action == "investment_match.supersede" for e in events) == 1
    matching.run()
    assert matching.pending() == []


def test_stale_accepted_reinvest_reserves_entire_changed_current_event(
    comparison_db: Database,
) -> None:
    from datetime import UTC, date, datetime
    from decimal import Decimal

    from moneybin.connectors.sync_models import SyncInvestmentTransaction
    from moneybin.extractors.plaid.extractor import PlaidExtractor

    db = comparison_db
    seed_manual_event(
        db, "manual_acquire", type_="reinvest", subtype="dividend", group="group"
    )
    seed_manual_event(
        db,
        "manual_income",
        type_="dividend",
        group="group",
        quantity=None,
        amount="100",
        price=None,
    )
    seed_plaid_event(db, "acquire", type_="buy", subtype="dividend reinvestment")
    seed_plaid_event(
        db, "income_a", type_="cash", subtype="dividend", quantity=None, amount="-100"
    )
    install_comparison_models(db)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    matching = InvestmentMatchingService(db)
    matching.run()
    proposal_id = matching.pending()[0]["proposal_id"]
    with operation():
        matching.decide(proposal_id, decision="accept", actor="test")
    before = membership.project()
    assert len(before) == 2
    history = InvestmentEventMembersRepo(db).history()
    seed_plaid_event(
        db, "income_b", type_="cash", subtype="dividend", quantity=None, amount="-100"
    )
    changed = SyncInvestmentTransaction(
        investment_transaction_id="income_a",
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=date(2026, 1, 20),
        name="Corrected income",
        quantity=None,
        amount=Decimal(-100),
        fees=Decimal(0),
        type="cash",
        subtype="dividend",
    )
    at = datetime(2026, 2, 2, tzinfo=UTC)
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # retained synthetic correction
        [changed], "corrected_income", at, at
    )
    membership.reconcile(actor="test")
    assert matching.history()[0]["status"] == "stale"
    assert membership.project() == before
    assert InvestmentEventMembersRepo(db).history() == history
    matching.run()
    assert matching.pending() == []


@pytest.mark.parametrize(
    "response", ["accept", "decline", "graph_drift", "selection_drift"]
)
@pytest.mark.asyncio
async def test_mcp_acceptance_requires_exact_same_prompt_and_effects(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch, response: str
) -> None:
    from datetime import date
    from decimal import Decimal
    from unittest.mock import AsyncMock, MagicMock

    from fastmcp.server.elicitation import AcceptedElicitation, DeclinedElicitation

    from moneybin import error_codes
    from moneybin.investments.cost_basis import compute_lot_id
    from moneybin.mcp.tools.reviews import reviews_decide_coarse
    from moneybin.protocol.write_contracts import (
        InvestmentMatchDecisionRequest,
    )
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
    from tests.moneybin.test_investments.test_investment_decision_routes import (
        use_decision_database,
    )

    db = comparison_db
    matching, proposal_id, _oldest = registered_proposal(db)
    seed_manual_event(db, "sell", type_="sell", quantity="-1", amount="100", day=20)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    before = membership.project()
    use_decision_database(monkeypatch, db)
    messages: list[str] = []

    async def elicit(message: str, **kwargs: Any) -> Any:
        messages.append(message)
        if response == "graph_drift":
            seed_plaid_event(db, "alternative")
        if response == "selection_drift":
            buy = next(r for r in before if r["source_type"] == "plaid")
            sell = next(r for r in before if r["type"] == "sell")
            lot = compute_lot_id(
                "account",
                "security",
                date(2026, 1, 10),
                buy["investment_transaction_id"],
            )
            with operation():
                LotSelectionsRepo(db).set_for_disposal(
                    investment_transaction_id=sell["investment_transaction_id"],
                    selections=[(lot, Decimal(1))],
                    actor="test",
                )
        return (
            DeclinedElicitation()
            if response == "decline"
            else AcceptedElicitation(data=True)
        )

    context = MagicMock()
    context.elicit = AsyncMock(side_effect=elicit)
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: context)

    def supports(_context: object) -> bool:
        return True

    monkeypatch.setattr("moneybin.mcp.confirmation.supports_elicitation", supports)
    request = InvestmentMatchDecisionRequest(
        kind="investment_match", decision_id=proposal_id, decision="accept"
    )
    with operation() as op:
        result = await reviews_decide_coarse([request])
    assert len(messages) == 1 and proposal_id in messages[0]
    if response == "accept":
        assert result.data is not None and result.data.applied_count == 1
        assert matching.history()[0]["status"] == "accepted"
        assert len(membership.project()) == 2
    else:
        assert result.error is not None
        if response == "selection_drift":
            assert result.error.code == error_codes.MUTATION_CONFIRMATION_MISMATCH
        assert matching.pending()[0]["proposal_id"] == proposal_id
        assert membership.project() == before
        assert AuditService(db).events_for_operation(op) == []


@pytest.mark.parametrize("earlier_quantity", [3, 4])
def test_complete_batch_curation_proof_precedes_first_decision_write(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch, earlier_quantity: int
) -> None:
    from datetime import date
    from decimal import Decimal

    from moneybin.investments.cost_basis import compute_lot_id
    from moneybin.protocol.write_contracts import (
        InvestmentMatchDecisionRequest,
        OrdinaryReviewDecisionRequest,
    )
    from moneybin.repositories.investment_match_decisions_repo import (
        InvestmentMatchDecisionsRepo,
    )
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
    from moneybin.services.review_decisions_service import ReviewDecisionsService

    db = comparison_db
    seed_manual_event(db, "manual_a", day=1)
    seed_plaid_event(db, "plaid_a", day=1)
    seed_manual_event(db, "manual_b", quantity="2", amount="-200", day=2)
    seed_plaid_event(db, "plaid_b", quantity="2", amount="200", day=2)
    seed_manual_event(
        db, "selected_acquisition", quantity="1", price="300", amount="-300", day=3
    )
    seed_manual_event(
        db,
        "earlier_disposal",
        type_="sell",
        quantity=str(-earlier_quantity),
        amount=str(earlier_quantity * 100),
        day=4,
    )
    seed_manual_event(
        db, "selected_disposal", type_="sell", quantity="-1", amount="100", day=5
    )
    install_comparison_models(db)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    rows = membership.project()
    acquisition = next(
        r for r in rows if r["native_reference"] == "selected_acquisition"
    )
    disposal = next(r for r in rows if r["native_reference"] == "selected_disposal")
    lot = compute_lot_id(
        "account",
        "security",
        date(2026, 1, 3),
        acquisition["investment_transaction_id"],
    )
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id=disposal["investment_transaction_id"],
        selections=[(lot, Decimal(1))],
        actor="test",
    )
    matching = InvestmentMatchingService(db)
    matching.run()
    pending = matching.pending()
    assert len(pending) == 2
    requests: list[OrdinaryReviewDecisionRequest] = [
        InvestmentMatchDecisionRequest(
            kind="investment_match", decision_id=p["proposal_id"], decision="accept"
        )
        for p in pending
    ]
    batch = ReviewDecisionsService(db, actor="test")
    # Either consolidation alone leaves the elected lot untouched by FIFO.
    assert all(len(batch.plan_ordinary([request])) == 1 for request in requests)
    before = InvestmentEventMembersRepo(db).history()
    attempts: list[str] = []
    accept = InvestmentMatchDecisionsRepo.accept

    def observe(
        self: InvestmentMatchDecisionsRepo, proposal_id: str, **kwargs: Any
    ) -> Any:
        attempts.append(proposal_id)
        return accept(self, proposal_id, **kwargs)

    monkeypatch.setattr(InvestmentMatchDecisionsRepo, "accept", observe)
    with operation() as op:
        if earlier_quantity == 4:
            with pytest.raises(UserError, match="selection"):
                batch.apply_ordinary(requests)
        else:
            outcome = batch.apply_ordinary(requests)
            assert [item.status for item in outcome.items] == ["accepted", "accepted"]
    if earlier_quantity == 4:
        assert attempts == []
        assert InvestmentEventMembersRepo(db).history() == before
        assert matching.pending() == pending
        assert AuditService(db).events_for_operation(op) == []
    else:
        assert len(attempts) == 2
        assert matching.pending() == []
        ledger = membership.project()
        assert len(ledger) == 5
        assert sum(row["quantity"] for row in ledger) == 0
        assert (
            sum(
                e.action == "investment_match.accept"
                for e in AuditService(db).events_for_operation(op)
            )
            == 2
        )


def test_mixed_batch_failure_rolls_back_accepted_topology(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    from moneybin.protocol.write_contracts import (
        CategorizationDecisionRequest,
        InvestmentMatchDecisionRequest,
        OrdinaryReviewDecisionRequest,
    )
    from moneybin.repositories.categorization_decisions_repo import (
        categorization_decision_id,
    )
    from moneybin.services.review_decisions_service import ReviewDecisionsService
    from tests.moneybin.db_helpers import (
        CORE_FCT_TRANSACTIONS_DDL,
        seed_categories_view,
    )

    db = comparison_db
    matching, proposal_id, _oldest = registered_proposal(db)
    db.execute(CORE_FCT_TRANSACTIONS_DDL)
    db.execute("""INSERT INTO core.fct_transactions (transaction_id, description, account_id, amount)
        VALUES ('category_target', 'Synthetic Purchase', 'account', -10)""")
    seed_categories_view(db)
    requests: list[OrdinaryReviewDecisionRequest] = [
        InvestmentMatchDecisionRequest(
            kind="investment_match", decision_id=proposal_id, decision="accept"
        ),
        CategorizationDecisionRequest(
            kind="categorization",
            decision_id=categorization_decision_id("category_target"),
            decision="accept",
            category="Food & Drink",
        ),
    ]
    before = InvestmentEventMembersRepo(db).history()

    def fail(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("injected categorization failure")

    monkeypatch.setattr(
        "moneybin.services.categorization.CategorizationService.apply_review_categorization_in_active_txn",
        fail,
    )
    with operation() as op:
        with pytest.raises(RuntimeError, match="injected categorization"):
            ReviewDecisionsService(db, actor="test").apply_ordinary(requests)
    assert matching.pending()[0]["proposal_id"] == proposal_id
    assert InvestmentEventMembersRepo(db).history() == before
    assert AuditService(db).events_for_operation(op) == []
    assert db.execute("SELECT COUNT(*) FROM app.transaction_categories").fetchone() == (
        0,
    )


def test_new_rejection_constraint_blocks_displayed_acceptance(
    comparison_db: Database,
) -> None:
    from tests.moneybin.test_investments.test_investment_match_decisions import (
        planned_investment,
    )

    db = comparison_db
    matching, displayed_id = planned_investment(db, competing=True)
    InvestmentMembershipService(db).reconcile(actor="test")
    other = next(p for p in matching.pending() if p["proposal_id"] != displayed_id)
    with operation():
        matching.decide(other["proposal_id"], decision="reject", actor="test")
    before = InvestmentEventMembersRepo(db).history()
    with operation() as op:
        with pytest.raises(UserError, match="[Ss]tale"):
            matching.decide(displayed_id, decision="accept", actor="test")
    assert InvestmentEventMembersRepo(db).history() == before
    assert AuditService(db).events_for_operation(op) == []
