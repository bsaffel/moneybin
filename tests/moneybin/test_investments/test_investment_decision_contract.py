"""One strict request shape feeds human-bound investment review decisions."""

from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.protocol.write_contracts import ReviewDecisionRequest
from moneybin.services.audit_service import AuditService
from moneybin.services.mutation_context import operation
from moneybin.services.review_decisions_service import ReviewDecisionsService
from tests.moneybin.test_investments.test_investment_match_decisions import (
    planned_investment,
)


def investment_request(decision_id: str, **extra: Any) -> Any:
    return TypeAdapter(ReviewDecisionRequest).validate_python({
        "kind": "investment_match",
        "decision_id": decision_id,
        "decision": "reject",
        **extra,
    })


def test_investment_request_joins_existing_discriminated_union() -> None:
    request = investment_request("proposal")
    assert request.kind == "investment_match"
    accepted = investment_request(
        "proposal",
        decision="accept",
        field_choices=[
            {"conflict_id": "conflict", "choice_id": "choice"},
        ],
    )
    assert accepted.field_choices[0].choice_id == "choice"


@pytest.mark.parametrize(
    "extra",
    [
        {"field_choices": []},
        {
            "decision": "accept",
            "field_choices": [
                {"conflict_id": "a", "choice_id": "b"},
                {"conflict_id": "a", "choice_id": "c"},
            ],
        },
        {
            "decision": "accept",
            "field_choices": [
                {"conflict_id": "a", "choice_id": "b", "value": 100},
            ],
        },
    ],
)
def test_request_rejects_forbidden_choices_and_authored_values(
    extra: dict[str, Any],
) -> None:
    with pytest.raises(ValidationError):
        investment_request("proposal", **extra)


def test_atomic_batch_rejects_competing_relationships_together(
    comparison_db: Database,
) -> None:
    service, _ = planned_investment(comparison_db, competing=True)
    ids = [row["proposal_id"] for row in service.pending()]
    batch = ReviewDecisionsService(comparison_db, actor="test")
    with operation() as op:
        result = batch.apply_ordinary([investment_request(value) for value in ids])
    assert [item.status for item in result.items] == ["rejected", "rejected"]
    assert service.pending() == []
    assert sum(row["status"] == "rejected" for row in service.history()) == 2
    assert (
        sum(
            e.action == "investment_match.reject"
            for e in AuditService(comparison_db).events_for_operation(op)
        )
        == 2
    )


def test_batch_verification_runs_before_any_audit_write(
    comparison_db: Database,
) -> None:
    service, decision_id = planned_investment(comparison_db)
    before = service.pending()
    batch: Any = ReviewDecisionsService(comparison_db, actor="test")
    with operation() as op:

        def refuse(plan: Any) -> None:
            assert (
                plan[0].before_state["relationship_fingerprint"]
                == before[0]["relationship_fingerprint"]
            )
            assert AuditService(comparison_db).events_for_operation(op) == []
            raise UserError(
                "Synthetic confirmation drift",
                code=error_codes.MUTATION_CONFIRMATION_MISMATCH,
            )

        with pytest.raises(UserError, match="confirmation drift"):
            batch.apply_ordinary([investment_request(decision_id)], verify=refuse)
    assert service.pending() == before
    assert AuditService(comparison_db).events_for_operation(op) == []
