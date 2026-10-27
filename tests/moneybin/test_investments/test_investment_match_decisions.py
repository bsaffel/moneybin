"""Human decisions retain exact evidence and atomically enforce their lifecycle."""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Literal

import pytest

from moneybin import error_codes
from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.services.audit_service import AuditService
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


def planned_investment(db: Database, *, competing: bool = False) -> tuple[Any, str]:
    seed_manual_event(db, "manual")
    seed_plaid_event(db, "native")
    if competing:
        seed_plaid_event(db, "alternative")
    install_comparison_models(db)
    service = InvestmentMatchingService(db)
    service.run()
    return service, service.pending()[0]["proposal_id"]


def test_reject_updates_existing_lifecycle_with_full_audit(
    comparison_db: Database,
) -> None:
    service, decision_id = planned_investment(comparison_db)
    before = service.pending()[0]
    raw = comparison_db.execute(
        "SELECT * FROM raw.manual_investment_transactions"
    ).fetchall()
    comparison_db.execute(
        "CREATE TABLE core.fct_investment_transactions AS SELECT 42 AS sentinel"
    )
    with operation() as op:
        service.decide(decision_id, decision="reject", actor="test")
    assert service.pending() == []
    after = service.history()[0]
    assert after["proposal_id"] == decision_id
    assert after["status"] == "rejected"
    assert after["decided_at"] is not None
    assert after["accepted_at"] is None
    assert after["relationship_fingerprint"] == before["relationship_fingerprint"]
    assert after["legs"] == before["legs"]
    events = AuditService(comparison_db).events_for_operation(op)
    assert len(events) == 1
    assert events[0].action == "investment_match.reject"
    assert events[0].before_value is not None
    assert events[0].after_value is not None
    assert events[0].before_value["status"] == "pending"
    assert events[0].after_value["status"] == "rejected"
    assert "proposal" in events[0].before_value
    assert (
        comparison_db.execute(
            "SELECT * FROM raw.manual_investment_transactions"
        ).fetchall()
        == raw
    )
    assert comparison_db.execute(
        "SELECT * FROM core.fct_investment_transactions"
    ).fetchall() == [(42,)]


def test_rejected_relationship_survives_new_alternative(
    comparison_db: Database,
) -> None:
    service, decision_id = planned_investment(comparison_db)
    original_members = service.pending()[0]["members"]
    service.decide(decision_id, decision="reject", actor="test")
    assert service.run().suppressed == 1
    seed_plaid_event(comparison_db, "alternative")
    result = service.run()
    assert result.suppressed == 1
    assert result.pending_unique == 1
    assert all(row["members"] != original_members for row in service.pending())


@pytest.mark.parametrize("decision", ["accept", "reject"])
@pytest.mark.parametrize("drift", ["alternative", "currency", "revision", "identity"])
def test_decision_rechecks_live_facts_without_needing_another_planner_run(
    comparison_db: Database,
    drift: str,
    decision: Literal["accept", "reject"],
) -> None:
    service, decision_id = planned_investment(comparison_db)
    if decision == "accept":
        from moneybin.services.investment_membership_service import (
            InvestmentMembershipService,
        )

        InvestmentMembershipService(comparison_db).reconcile(actor="test")
    if drift == "alternative":
        seed_plaid_event(comparison_db, "alternative")
    elif drift == "currency":
        comparison_db.execute("UPDATE core.dim_accounts SET currency_code = 'CAD'")
    elif drift == "identity":
        from moneybin.repositories.account_links_repo import AccountLinksRepo

        comparison_db.execute(
            "INSERT INTO core.dim_accounts VALUES ('other_account', 'USD')"
        )
        AccountLinksRepo(comparison_db).repoint(
            link_id="account_link",
            new_account_id="other_account",
            decided_by="user",
            actor="test",
        )
    else:
        # A second delivery appends a revision; never alter captured Raw evidence.
        revised = SyncInvestmentTransaction(
            investment_transaction_id="native",
            account_id="native_account",
            provider_item_id="origin",
            security_id="native_security",
            date=date(2026, 1, 11),
            name="Aggregator context",
            quantity=Decimal("1"),
            amount=Decimal("100"),
            price=Decimal("100"),
            fees=Decimal("0"),
            type="buy",
            subtype="buy",
        )
        PlaidExtractor(comparison_db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]
            [revised],
            "sync_revision",
            datetime(2026, 2, 2, tzinfo=UTC),
            datetime(2026, 2, 2, tzinfo=UTC),
        )
    with operation() as op:
        with pytest.raises(UserError, match="[Ss]tale"):
            service.decide(decision_id, decision=decision, actor="test")
    assert not any(
        row["status"] in {"accepted", "rejected"} for row in service.history()
    )
    assert AuditService(comparison_db).events_for_operation(op) == []


def test_stale_proposal_cannot_be_rejected(comparison_db: Database) -> None:
    service, decision_id = planned_investment(comparison_db)
    seed_plaid_event(comparison_db, "alternative")
    service.run()
    with pytest.raises(UserError):
        service.decide(decision_id, decision="reject", actor="test")
    assert all(row["status"] == "stale" for row in service.history())


def test_acceptance_without_registered_membership_writes_nothing(
    comparison_db: Database,
) -> None:
    service, decision_id = planned_investment(comparison_db)
    before = service.pending()
    with operation() as op:
        with pytest.raises(UserError, match="membership"):
            service.decide(decision_id, decision="accept", actor="test")
    assert service.pending() == before
    assert AuditService(comparison_db).events_for_operation(op) == []


def test_reject_forbids_field_choices(comparison_db: Database) -> None:
    service, decision_id = planned_investment(comparison_db)
    with pytest.raises(UserError) as exc:
        service.decide(
            decision_id,
            decision="reject",
            actor="test",
            field_choices=[{"conflict_id": "unknown", "choice_id": "unknown"}],
        )
    assert exc.value.code == error_codes.MUTATION_INVALID_INPUT
    assert service.pending()[0]["status"] == "pending"


def test_rejection_undo_restores_assignment_and_stales_new_pending(
    comparison_db: Database,
) -> None:
    service, decision_id = planned_investment(comparison_db, competing=True)
    assert len(service.pending()) == 2
    with operation() as op:
        service.decide(decision_id, decision="reject", actor="test")
    assert service.pending() == []
    service.run()
    unique = service.pending()[0]
    assert not unique["is_competing"]
    result = UndoService(comparison_db).undo(op, actor="test")
    assert result.undone_operation_id == op
    assert service.pending() == []
    assert not any(row["status"] == "rejected" for row in service.history())
    service.run()
    assert len(service.pending()) == 2
    assert all(row["is_competing"] for row in service.pending())
    assert all(row["proposal_id"] != unique["proposal_id"] for row in service.pending())


def test_commit_failure_rolls_back_rejection_and_audit(
    comparison_db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service, decision_id = planned_investment(comparison_db)
    before = service.pending()

    def fail_commit() -> None:
        raise RuntimeError("synthetic commit failure")

    monkeypatch.setattr(comparison_db, "commit", fail_commit)
    with operation() as op:
        with pytest.raises(RuntimeError, match="synthetic commit failure"):
            service.decide(decision_id, decision="reject", actor="test")
    assert service.pending() == before
    assert AuditService(comparison_db).events_for_operation(op) == []
