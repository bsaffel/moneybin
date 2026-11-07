"""Algorithm upgrades require fresh review without rewriting reviewed membership."""

import json
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastmcp.server.elicitation import AcceptedElicitation

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments import event_fingerprints, event_planning
from moneybin.mcp.tools.reviews import reviews_decide_coarse
from moneybin.protocol.write_contracts import InvestmentMatchDecisionRequest
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.services.audit_service import AuditService
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.investment_membership_service import InvestmentMembershipService
from moneybin.services.mutation_context import operation
from tests.moneybin.test_investments.test_investment_decision_routes import (
    use_decision_database,
)
from tests.moneybin.test_investments.test_investment_golden_projection import (
    install_golden_models,
    ledger,
)
from tests.moneybin.test_investments.test_investment_match_acceptance import (
    registered_proposal,
)


def _previous_algorithm(monkeypatch: pytest.MonkeyPatch) -> None:
    old = event_fingerprints.ALGORITHM_VERSION
    previous = f"{old}_synthetic_previous"
    monkeypatch.setattr(event_fingerprints, "ALGORITHM_VERSION", previous)
    # Simulate an older deployed build, including its import-time default.
    constructor = event_planning.Proposal.__init__
    defaults = constructor.__defaults__
    assert defaults is not None
    assert old in defaults
    monkeypatch.setattr(
        constructor,
        "__defaults__",
        tuple(previous if value == old else value for value in defaults),
    )


@pytest.mark.asyncio
async def test_upgraded_pending_evidence_requires_fresh_human_confirmation(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = comparison_db
    with monkeypatch.context() as previous:
        _previous_algorithm(previous)
        matching, old_id, oldest = registered_proposal(db)
        original = matching.pending()[0]
    assert original["auto_eligible"] is True
    assert original["confidence_band"] == "exact"
    install_golden_models(db)
    before = ledger(db)
    members = InvestmentEventMembersRepo(db).history()
    assert len(before) == 2
    upgraded = event_fingerprints.ALGORITHM_VERSION
    reopened = InvestmentMatchingService(db)
    for decision in ("accept", "reject"):
        with pytest.raises(UserError, match="stale"):
            reopened.prepare_decision(old_id, decision=decision)
    result = reopened.run()
    assert (result.stale, result.proposed) == (1, 1)
    pending = InvestmentMatchingService(db).pending()
    assert len(pending) == 1
    current = pending[0]
    assert current["proposal_id"] != old_id
    assert current["algorithm_version"] == upgraded
    assert current["relationship_fingerprint"] != original["relationship_fingerprint"]
    assert (
        current["candidate_graph_fingerprint"]
        != original["candidate_graph_fingerprint"]
    )
    for field in ("members", "legs", "evidence", "auto_eligible", "field_choices"):
        assert current[field] == original[field]
    assert {leg["native_reference"] for leg in current["legs"]} == {"manual", "native"}
    assert {Decimal(leg["amount"]) for leg in current["legs"]} == {Decimal("-100")}
    stale = next(row for row in reopened.history() if row["proposal_id"] == old_id)
    assert stale["status"] == "stale"
    assert stale["algorithm_version"] == original["algorithm_version"]
    assert stale["auto_eligible"] is True
    assert stale["evidence"] == original["evidence"]
    repeated = reopened.run()
    assert (repeated.proposed, repeated.stale, repeated.unchanged) == (0, 0, 1)
    assert reopened.pending() == pending
    assert ledger(db) == before
    assert InvestmentEventMembersRepo(db).history() == members

    use_decision_database(monkeypatch, db)
    context = MagicMock()
    context.elicit = AsyncMock(return_value=AcceptedElicitation(data=True))
    monkeypatch.setattr("moneybin.mcp.confirmation._active_context", lambda: context)
    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", MagicMock(return_value=False)
    )
    request = InvestmentMatchDecisionRequest(
        kind="investment_match", decision_id=current["proposal_id"], decision="accept"
    )
    audit_count = AuditService(db).count_events()
    denied = await reviews_decide_coarse([request])
    assert denied.error is not None
    assert denied.error.code == error_codes.MUTATION_CONFIRMATION_REQUIRED
    context.elicit.assert_not_awaited()
    assert reopened.pending() == pending
    assert ledger(db) == before
    assert InvestmentEventMembersRepo(db).history() == members
    assert AuditService(db).count_events() == audit_count

    monkeypatch.setattr(
        "moneybin.mcp.confirmation.supports_elicitation", MagicMock(return_value=True)
    )
    accepted = await reviews_decide_coarse([request])
    assert accepted.data is not None and accepted.data.applied_count == 1
    context.elicit.assert_awaited_once()
    assert current["proposal_id"] in context.elicit.call_args.args[0]
    assert reopened.pending() == []
    accepted_row = next(
        row
        for row in reopened.history()
        if row["proposal_id"] == current["proposal_id"]
    )
    assert accepted_row["status"] == "accepted"
    assert accepted_row["auto_eligible"] is True
    assert accepted_row["algorithm_version"] == upgraded
    rows = ledger(db)
    assert len(rows) == 1
    assert rows[0]["amount"] == Decimal("-100")
    assert rows[0]["investment_transaction_id"] == oldest["investment_transaction_id"]
    assert rows[0]["event_group_id"] == oldest["event_group_id"]
    assert accepted.data.operation_id is not None
    acceptance_events = [
        event
        for event in AuditService(db).events_for_operation(accepted.data.operation_id)
        if event.action == "investment_match.accept"
    ]
    assert len(acceptance_events) == 1
    assert acceptance_events[0].target_id == current["proposal_id"]


def test_algorithm_upgrade_reopens_exact_rejection_without_erasing_its_evidence(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = comparison_db
    with monkeypatch.context() as previous:
        _previous_algorithm(previous)
        matching, old_id, _ = registered_proposal(db)
        before = InvestmentMembershipService(db).project()
        members = InvestmentEventMembersRepo(db).history()
        with operation():
            matching.decide(old_id, decision="reject", actor="test")
        rejected = matching.history()[0]
        assert rejected["status"] == "rejected"
        assert rejected["auto_eligible"] is True
        assert matching.run().suppressed == 1
        assert matching.pending() == []
    upgraded = event_fingerprints.ALGORITHM_VERSION
    reopened = InvestmentMatchingService(db)
    result = reopened.run()
    assert (result.proposed, result.suppressed) == (1, 0)
    current = reopened.pending()[0]
    assert current["proposal_id"] != old_id
    assert current["algorithm_version"] == upgraded
    assert current["relationship_fingerprint"] != rejected["relationship_fingerprint"]
    assert (
        current["candidate_graph_fingerprint"]
        != rejected["candidate_graph_fingerprint"]
    )
    for field in ("members", "legs", "evidence", "auto_eligible"):
        assert current[field] == rejected[field]
    assert reopened.history() == [rejected]
    assert (reopened.run().proposed, len(reopened.pending())) == (0, 1)
    assert InvestmentMembershipService(db).project() == before
    assert InvestmentEventMembersRepo(db).history() == members


def test_accepted_projection_stays_pinned_until_upgrade_replacement_reviewed(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = comparison_db
    with monkeypatch.context() as previous:
        _previous_algorithm(previous)
        matching, proposal_id, _ = registered_proposal(db)
        with operation():
            matching.decide(proposal_id, decision="accept", actor="test")
        accepted = matching.history()[0]
    install_golden_models(db)
    before = ledger(db)
    members = InvestmentEventMembersRepo(db).history()
    assert len(before) == 1
    assert before[0]["amount"] == Decimal("-100")
    assert len(InvestmentEventMembersRepo(db).active_members()) == 2
    InvestmentMembershipService(db).reconcile(actor="test")
    reopened = InvestmentMatchingService(db)
    result = reopened.run()
    # The versioned predicate needs new review; a locked component keeps its
    # exact reviewed projection until complete replacement acceptance (spec 441-451).
    assert result.proposed == 1
    pending = reopened.pending()
    assert len(pending) == 1
    replacement = pending[0]
    assert replacement["algorithm_version"] == event_fingerprints.ALGORITHM_VERSION
    assert replacement["algorithm_version"] != accepted["algorithm_version"]
    assert replacement["proposal_id"] != proposal_id
    assert replacement["auto_eligible"] is False
    assert replacement["supersedes_decision_ids"] == [proposal_id]
    assert replacement["members"] == accepted["members"]
    assert replacement["legs"] == accepted["legs"]
    assert replacement["evidence"] == accepted["evidence"]
    assert len(replacement["supersession"]) == 1
    superseded = replacement["supersession"][0]
    assert superseded["decision_id"] == proposal_id
    assert superseded["members"] == accepted["members"]
    active = sorted(
        InvestmentEventMembersRepo(db).active_members(),
        key=lambda row: row["membership_id"],
    )
    assert superseded["membership"] == json.loads(json.dumps(active, default=str))
    stale = reopened.history()[0]
    assert stale["status"] == "stale"
    immutable = accepted.keys() - {"status", "updated_at"}
    assert {field: stale[field] for field in immutable} == {
        field: accepted[field] for field in immutable
    }
    repeated = reopened.run()
    assert (repeated.proposed, repeated.stale, repeated.unchanged) == (0, 0, 1)
    assert reopened.pending() == pending
    assert ledger(db) == before
    assert InvestmentEventMembersRepo(db).history() == members
