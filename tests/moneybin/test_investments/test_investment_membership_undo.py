"""Investment undo reconstructs complete current successors under one domain owner."""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest

from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.services.audit_service import AuditEvent, AuditService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_investments.comparison_helpers import seed_plaid_event
from tests.moneybin.test_investments.test_investment_match_acceptance import (
    registered_proposal,
)


def test_acceptance_undo_restores_complete_standalones_as_new_revisions(
    comparison_db: Database,
) -> None:
    matching, proposal, _ = registered_proposal(comparison_db)
    repo = InvestmentEventMembersRepo(comparison_db)
    before = repo.active_members()
    with operation() as accepted:
        matching.decide(proposal, decision="accept", actor="test")
    result = UndoService(comparison_db).undo(accepted, actor="test")
    after = repo.active_members()
    assert len(after) == 2 and {row["decision_id"] for row in after} == {None}
    assert {
        (row["event_group_id"], row["investment_transaction_id"]) for row in after
    } == {(row["event_group_id"], row["investment_transaction_id"]) for row in before}
    assert {row["membership_id"] for row in after}.isdisjoint(
        row["membership_id"] for row in before
    )
    assert matching.history()[0]["status"] == "reversed"
    events = AuditService(comparison_db).events_for_operation(result.undo_operation_id)
    assert any(
        event.is_undo and event.undoes_operation_id == accepted for event in events
    )
    assert not UndoService(comparison_db).get(accepted).can_undo


def test_undo_after_source_correction_uses_current_revision_and_former_ids(
    comparison_db: Database,
) -> None:
    matching, proposal, _ = registered_proposal(comparison_db)
    before = {
        row["source_type"]: row
        for row in InvestmentEventMembersRepo(comparison_db).active_members()
    }
    with operation() as accepted:
        matching.decide(proposal, decision="accept", actor="test")
    transaction = SyncInvestmentTransaction(
        investment_transaction_id="native",
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=date(2026, 1, 10),
        name="Current revision",
        quantity=Decimal(2),
        amount=Decimal(200),
        price=Decimal(100),
        fees=Decimal(0),
        type="buy",
        subtype="buy",
    )
    at = datetime(2026, 2, 2, tzinfo=UTC)
    PlaidExtractor(comparison_db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # synthetic correction
        [transaction], "new_revision", at, at
    )
    with operation():
        InvestmentMembershipService(comparison_db).reconcile(actor="test")
    assert matching.history()[0]["status"] == "stale"
    UndoService(comparison_db).undo(accepted, actor="test")
    projected = {
        row["source_type"]: row
        for row in InvestmentMembershipService(comparison_db).project()
    }
    assert projected["plaid"]["quantity"] == Decimal(2)
    assert projected["manual"]["quantity"] == Decimal(1)
    assert (
        projected["plaid"]["investment_transaction_id"]
        == before["plaid"]["investment_transaction_id"]
    )


@pytest.mark.parametrize("route_after_accept", [False, True])
def test_third_source_undo_restores_prior_match_and_removed_standalone(
    comparison_db: Database, route_after_accept: bool
) -> None:
    def route() -> None:
        AccountLinksRepo(comparison_db).insert(
            link_id="other_route",
            account_id="account",
            ref_kind="source_native",
            ref_value="native_account",
            source_type="plaid",
            source_origin="other",
            decided_by="user",
            actor="test",
        )

    if not route_after_accept:
        route()
    matching, original, _ = registered_proposal(comparison_db)
    with operation():
        matching.decide(original, decision="accept", actor="test")
    if route_after_accept:
        route()
    seed_plaid_event(comparison_db, "third", source_origin="other")
    membership = InvestmentMembershipService(comparison_db)
    membership.reconcile(actor="test")
    third = next(
        row
        for row in InvestmentEventMembersRepo(comparison_db).active_members()
        if row["native_reference"] == "third"
    )
    matching.run()
    replacement = matching.pending()[0]["proposal_id"]
    from prometheus_client import REGISTRY

    labels = {
        "decision": "reversed",
        "auto_eligible": str(
            next(row for row in matching.history() if row["proposal_id"] == original)[
                "auto_eligible"
            ]
        ).lower(),
    }
    reversals = (
        REGISTRY.get_sample_value("moneybin_investment_match_decisions_total", labels)
        or 0
    )
    with operation() as accepted:
        matching.decide(replacement, decision="accept", actor="test")
    assert (
        REGISTRY.get_sample_value("moneybin_investment_match_decisions_total", labels)
        or 0
    ) == reversals + 1
    UndoService(comparison_db).undo(accepted, actor="test")
    rows = InvestmentEventMembersRepo(comparison_db).active_members()
    assert len(rows) == 3
    assert {
        row["decision_id"] for row in rows if row["native_reference"] != "third"
    } == {original}
    restored_third = next(row for row in rows if row["native_reference"] == "third")
    assert restored_third["decision_id"] is None
    assert (
        restored_third["investment_transaction_id"]
        == third["investment_transaction_id"]
    )
    statuses = {row["proposal_id"]: row["status"] for row in matching.history()}
    assert statuses[original] == ("stale" if route_after_accept else "accepted")
    assert statuses[replacement] == "reversed"
    assert len(membership.project()) == 2


def curated_acceptance(db: Database) -> tuple[str, str, str]:
    from moneybin.investments.cost_basis import compute_lot_id
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
    from tests.moneybin.test_investments.comparison_helpers import seed_manual_event

    matching, _, _ = registered_proposal(db)
    seed_manual_event(db, "sell", type_="sell", quantity="-1", amount="100", day=20)
    membership = InvestmentMembershipService(db)
    membership.reconcile(actor="test")
    rows = membership.project()
    plaid = next(row for row in rows if row["source_type"] == "plaid")
    sell = next(row for row in rows if row["type"] == "sell")
    lot = compute_lot_id(
        "account", "security", date(2026, 1, 10), plaid["investment_transaction_id"]
    )
    LotSelectionsRepo(db).set_for_disposal(
        investment_transaction_id=sell["investment_transaction_id"],
        selections=[(lot, Decimal(1))],
        actor="test",
    )
    matching.run()
    with operation() as accepted:
        matching.decide(
            matching.pending()[0]["proposal_id"], decision="accept", actor="test"
        )
    return accepted, sell["investment_transaction_id"], lot


def test_undo_restores_complete_preaccept_selection_sets(
    comparison_db: Database,
) -> None:
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo

    accepted, disposal, old_lot = curated_acceptance(comparison_db)
    assert LotSelectionsRepo(comparison_db).list_for_disposal(disposal) != [
        (old_lot, Decimal(1))
    ]
    UndoService(comparison_db).undo(accepted, actor="test")
    assert LotSelectionsRepo(comparison_db).list_for_disposal(disposal) == [
        (old_lot, Decimal(1))
    ]


def test_newer_curation_blocks_undo_without_partial_topology(
    comparison_db: Database,
) -> None:
    from moneybin.errors import UserError
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo

    accepted, disposal, _ = curated_acceptance(comparison_db)
    selections = LotSelectionsRepo(comparison_db)
    current = selections.list_for_disposal(disposal)
    with operation():
        selections.set_for_disposal(
            investment_transaction_id=disposal,
            selections=[(current[0][0], Decimal("0.5"))],
            actor="test",
        )
    before = InvestmentEventMembersRepo(comparison_db).history()
    with pytest.raises(UserError) as caught:
        UndoService(comparison_db).undo(accepted, actor="test")
    assert caught.value.code == "undo_cascade_blocked"
    assert InvestmentEventMembersRepo(comparison_db).history() == before
    assert selections.list_for_disposal(disposal) == [(current[0][0], Decimal("0.5"))]


def test_undo_write_failure_rolls_back_lifecycle_topology_curation_and_audit(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    accepted, _, _ = curated_acceptance(comparison_db)
    repo = InvestmentEventMembersRepo(comparison_db)
    before = repo.history()
    before_audit = comparison_db.execute(
        "SELECT COUNT(*) FROM app.audit_log"
    ).fetchone()
    register = InvestmentEventMembersRepo.register
    calls = 0

    def fail(self: InvestmentEventMembersRepo, *args: Any, **kwargs: Any) -> str:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected successor failure")
        return register(self, *args, **kwargs)

    monkeypatch.setattr(InvestmentEventMembersRepo, "register", fail)
    with pytest.raises(RuntimeError, match="injected"):
        UndoService(comparison_db).undo(accepted, actor="test")
    assert repo.history() == before
    assert (
        comparison_db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
        == before_audit
    )
    assert comparison_db.execute(
        "SELECT status FROM app.investment_match_decisions WHERE accepted_at IS NOT NULL"
    ).fetchall() == [("accepted",)]


def test_undo_emits_reversal_only_after_commit(comparison_db: Database) -> None:
    from moneybin.metrics.registry import INVESTMENT_MATCH_DECISIONS_TOTAL

    matching, proposal, _ = registered_proposal(comparison_db)
    with operation() as accepted:
        matching.decide(proposal, decision="accept", actor="test")
    eligible = str(matching.history()[0]["auto_eligible"]).lower()
    # Counter values are process-local test evidence, never an authorization input.
    counter = INVESTMENT_MATCH_DECISIONS_TOTAL.labels(
        decision="reversed", auto_eligible=eligible
    )
    original = counter._value.get()  # pyright: ignore[reportPrivateUsage]  # Prometheus test counter read
    accepted_counter = INVESTMENT_MATCH_DECISIONS_TOTAL.labels(
        decision="accepted", auto_eligible=eligible
    )
    first_decisions = accepted_counter._value.get()  # pyright: ignore[reportPrivateUsage]  # Prometheus test counter read
    UndoService(comparison_db).undo(accepted, actor="test")
    assert counter._value.get() == original + 1  # pyright: ignore[reportPrivateUsage]  # Prometheus test counter read
    assert accepted_counter._value.get() == first_decisions  # pyright: ignore[reportPrivateUsage]  # Prometheus test counter read


def test_third_source_undo_follows_ratified_equivalent_successors(
    comparison_db: Database,
) -> None:
    from moneybin.investments.dependency_publication import (
        dependency_publication,
        record_equivalence,
    )
    from moneybin.repositories.security_links_repo import SecurityLinksRepo
    from tests.moneybin.test_investments.test_investment_membership_dependencies import (
        target_security,
    )

    AccountLinksRepo(comparison_db).insert(
        link_id="other_route",
        account_id="account",
        ref_kind="source_native",
        ref_value="native_account",
        source_type="plaid",
        source_origin="other",
        decided_by="user",
        actor="test",
    )
    matching, original, _ = registered_proposal(comparison_db)
    matching.decide(original, decision="accept", actor="test")
    seed_plaid_event(comparison_db, "third", source_origin="other")
    membership = InvestmentMembershipService(comparison_db)
    membership.reconcile(actor="test")
    matching.run()
    replacement = matching.pending()[0]["proposal_id"]
    with operation() as accepted:
        matching.decide(replacement, decision="accept", actor="test")
    link = target_security(comparison_db)
    comparison_db.begin()
    try:
        with operation(), dependency_publication(comparison_db):
            record_equivalence(
                comparison_db,
                kind="security",
                previous="security",
                proposed="other_security",
            )
            SecurityLinksRepo(comparison_db).repoint(
                link_id=link,
                new_security_id="other_security",
                decided_by="user",
                actor="test",
                in_outer_txn=True,
            )
            SecurityLinksRepo(comparison_db).insert(
                security_id="other_security",
                ref_kind="manual_investment_transaction_id",
                ref_value="manual",
                source_type="manual",
                decided_by="user",
                actor="test",
                in_outer_txn=True,
            )
        comparison_db.commit()
    except BaseException:
        comparison_db.rollback()
        raise
    UndoService(comparison_db).undo(accepted, actor="test")
    rows = InvestmentEventMembersRepo(comparison_db).active_members()
    assert {row["bound_security_id"] for row in rows} == {"other_security"}
    assert {
        row["decision_id"] for row in rows if row["native_reference"] != "third"
    } == {original}
    assert {row["proposal_id"]: row["status"] for row in matching.history()}[
        original
    ] == "accepted"


def test_reversal_recomputes_opening_gap_before_allowing_current_selections(
    comparison_db: Database,
) -> None:
    from moneybin.errors import UserError
    from moneybin.investments.cost_basis import compute_lot_id
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
    from moneybin.services.investment_matching_service import InvestmentMatchingService
    from tests.moneybin.test_investments.comparison_helpers import (
        install_comparison_models,
        seed_manual_event,
    )
    from tests.moneybin.test_investments.test_investment_opening_membership import (
        seed_snapshot,
    )

    seed_manual_event(comparison_db, "manual", day=1)
    seed_manual_event(
        comparison_db, "sell", type_="sell", quantity="-5", amount="500", day=2
    )

    def revision(day: date, receipt: str) -> None:
        tx = SyncInvestmentTransaction(
            investment_transaction_id="native",
            account_id="native_account",
            provider_item_id="origin",
            security_id="native_security",
            date=day,
            transaction_datetime=datetime(day.year, day.month, day.day, tzinfo=UTC),
            name="Synthetic purchase",
            quantity=Decimal(1),
            amount=Decimal(100),
            price=Decimal(100),
            fees=Decimal(0),
            type="buy",
            subtype="buy",
        )
        at = datetime(2026, 2, 1 if receipt == "original" else 2, tzinfo=UTC)
        PlaidExtractor(comparison_db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # retained synthetic revision
            [tx], receipt, at, at
        )

    revision(date(2025, 12, 31), "original")
    seed_snapshot(comparison_db, "first")
    install_comparison_models(comparison_db)
    membership = InvestmentMembershipService(comparison_db)
    membership.reconcile(actor="test")
    matching = InvestmentMatchingService(comparison_db)
    matching.run()
    proposal = matching.pending()[0]
    conflict = next(
        row for row in proposal["field_choices"] if row["field"] == "trade_date"
    )
    choice = next(row for row in conflict["choices"] if row["source_type"] == "plaid")
    with operation() as accepted:
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
    rows = membership.project()
    opening = next(row for row in rows if row["subtype"] == "opening_bootstrap")
    disposal = next(row for row in rows if row["type"] == "sell")
    assert opening["quantity"] == Decimal(5)
    selected = compute_lot_id(
        "account", "security", date(2025, 12, 31), opening["investment_transaction_id"]
    )
    LotSelectionsRepo(comparison_db).set_for_disposal(
        investment_transaction_id=disposal["investment_transaction_id"],
        selections=[(selected, Decimal(5))],
        actor="test",
    )
    revision(date(2026, 1, 1), "corrected")
    membership.reconcile(actor="test")
    before = InvestmentEventMembersRepo(comparison_db).history()
    audit_count = comparison_db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
    with pytest.raises(UserError, match="selection"):
        UndoService(comparison_db).undo(accepted, actor="test")
    assert InvestmentEventMembersRepo(comparison_db).history() == before
    assert (
        comparison_db.execute("SELECT COUNT(*) FROM app.audit_log").fetchone()
        == audit_count
    )
    assert matching.history()[0]["status"] == "stale"


def test_changed_reinvest_partner_mints_complete_current_successors(
    comparison_db: Database,
) -> None:
    from moneybin.services.investment_matching_service import InvestmentMatchingService
    from tests.moneybin.test_investments.comparison_helpers import (
        install_comparison_models,
        seed_manual_event,
    )
    from tests.moneybin.test_investments.test_investment_golden_projection import (
        install_model,
    )

    seed_manual_event(
        comparison_db,
        "manual_acquire",
        type_="reinvest",
        subtype="dividend",
        group="group",
    )
    seed_manual_event(
        comparison_db,
        "manual_income",
        type_="dividend",
        group="group",
        quantity=None,
        amount="100",
        price=None,
    )
    seed_plaid_event(
        comparison_db, "acquire", type_="buy", subtype="dividend reinvestment"
    )
    seed_plaid_event(
        comparison_db,
        "income_a",
        type_="cash",
        subtype="dividend",
        quantity=None,
        amount="-100",
    )
    install_comparison_models(comparison_db)
    membership = InvestmentMembershipService(comparison_db)
    membership.reconcile(actor="test")
    before = [
        row
        for row in InvestmentEventMembersRepo(comparison_db).active_members()
        if row["source_type"] == "plaid"
    ]
    matching = InvestmentMatchingService(comparison_db)
    matching.run()
    with operation() as accepted:
        matching.decide(
            matching.pending()[0]["proposal_id"], decision="accept", actor="test"
        )
    seed_plaid_event(
        comparison_db,
        "income_b",
        type_="cash",
        subtype="dividend",
        quantity=None,
        amount="-100",
    )
    tx = SyncInvestmentTransaction(
        investment_transaction_id="income_a",
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=date(2026, 1, 20),
        name="Changed partner",
        quantity=None,
        amount=Decimal(-100),
        fees=Decimal(0),
        type="cash",
        subtype="dividend",
    )
    at = datetime(2026, 2, 2, tzinfo=UTC)
    PlaidExtractor(comparison_db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]  # retained synthetic correction
        [tx], "changed", at, at
    )
    membership.reconcile(actor="test")
    UndoService(comparison_db).undo(accepted, actor="test")
    after = [
        row
        for row in InvestmentEventMembersRepo(comparison_db).active_members()
        if row["source_type"] == "plaid"
    ]
    assert len(after) == 3
    assert {row["investment_transaction_id"] for row in after}.isdisjoint(
        row["investment_transaction_id"] for row in before
    )
    assert len(membership.project()) == 5
    install_model(comparison_db, "core.alias_investment_event_ids")
    assert comparison_db.execute(
        "SELECT resolution_status FROM core.alias_investment_event_ids WHERE requested_event_group_id = ?",
        [before[0]["event_group_id"]],
    ).fetchone() == ("retired",)


@pytest.mark.parametrize("broken_capture", [False, True])
def test_entire_acceptance_batch_is_preflighted_before_first_undo_write(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch, broken_capture: bool
) -> None:
    from dataclasses import replace

    from moneybin.errors import UserError
    from moneybin.investments.cost_basis import compute_lot_id
    from moneybin.protocol.write_contracts import (
        InvestmentMatchDecisionRequest,
        OrdinaryReviewDecisionRequest,
    )
    from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
    from moneybin.services.investment_matching_service import InvestmentMatchingService
    from moneybin.services.review_decisions_service import ReviewDecisionsService
    from tests.moneybin.test_investments.comparison_helpers import (
        install_comparison_models,
        seed_manual_event,
    )

    for native, day in (("manual_a", 1), ("manual_b", 10)):
        seed_manual_event(comparison_db, native, day=day)
    seed_manual_event(
        comparison_db, "sell", type_="sell", quantity="-2", amount="200", day=20
    )
    install_comparison_models(comparison_db)
    membership = InvestmentMembershipService(comparison_db)
    membership.reconcile(actor="test")
    for native, day in (("plaid_a", 1), ("plaid_b", 10)):
        seed_plaid_event(comparison_db, native, day=day)
    membership.reconcile(actor="test")
    ledger = membership.project()
    disposal = next(
        row["investment_transaction_id"] for row in ledger if row["type"] == "sell"
    )
    original = sorted(
        (
            compute_lot_id(
                "account",
                "security",
                row["trade_date"],
                row["investment_transaction_id"],
            ),
            Decimal(1),
        )
        for row in ledger
        if row["source_type"] == "plaid"
    )
    selections = LotSelectionsRepo(comparison_db)
    selections.set_for_disposal(
        investment_transaction_id=disposal, selections=original, actor="test"
    )
    matching = InvestmentMatchingService(comparison_db)
    matching.run()
    assert len(matching.pending()) == 2
    requests: list[OrdinaryReviewDecisionRequest] = [
        InvestmentMatchDecisionRequest(
            kind="investment_match", decision_id=row["proposal_id"], decision="accept"
        )
        for row in matching.pending()
    ]
    with operation() as accepted:
        ReviewDecisionsService(comparison_db, actor="test").apply_ordinary(requests)
    before = InvestmentEventMembersRepo(comparison_db).history()
    attempts: list[str] = []
    register = InvestmentEventMembersRepo.register

    def observe(self: InvestmentEventMembersRepo, *args: Any, **kwargs: Any) -> str:
        attempts.append("register")
        return register(self, *args, **kwargs)

    monkeypatch.setattr(InvestmentEventMembersRepo, "register", observe)
    if broken_capture:
        read_events = AuditService.events_for_operation

        def incomplete(self: AuditService, operation_id: str) -> list[AuditEvent]:
            events = read_events(self, operation_id)
            if operation_id == accepted:
                first = next(
                    event.audit_id
                    for event in events
                    if event.action == "investment_match.accept"
                )
                return [
                    replace(event, context_json=None)
                    if event.audit_id == first
                    else event
                    for event in events
                ]
            return events

        monkeypatch.setattr(AuditService, "events_for_operation", incomplete)
        with pytest.raises(UserError):
            UndoService(comparison_db).undo(accepted, actor="test")
        assert attempts == []
        assert InvestmentEventMembersRepo(comparison_db).history() == before
    else:
        UndoService(comparison_db).undo(accepted, actor="test")
        assert selections.list_for_disposal(disposal) == original
        assert len(membership.project()) == 5
