"""Only committed first human decisions contribute review-quality metrics."""

import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.services.mutation_context import operation
from moneybin.services.undo_service import UndoService
from tests.moneybin.test_investments.test_investment_match_decisions import (
    planned_investment,
)


def _count(auto_eligible: bool) -> float:
    return (
        REGISTRY.get_sample_value(
            "moneybin_investment_match_decisions_total",
            {"decision": "rejected", "auto_eligible": str(auto_eligible).lower()},
        )
        or 0
    )


def test_committed_rejection_counts_once_and_undo_does_not_recount(
    comparison_db: Database,
) -> None:
    service, decision_id = planned_investment(comparison_db)
    eligible = service.pending()[0]["auto_eligible"]
    before = _count(eligible)
    with operation() as op:
        service.decide(decision_id, decision="reject", actor="test")
    assert _count(eligible) == before + 1
    service.run()
    UndoService(comparison_db).undo(op, actor="test")
    assert _count(eligible) == before + 1


def test_failed_commit_does_not_count_a_decision(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, decision_id = planned_investment(comparison_db)
    eligible = service.pending()[0]["auto_eligible"]
    before = _count(eligible)

    def fail() -> None:
        raise RuntimeError("synthetic commit failure")

    monkeypatch.setattr(comparison_db, "commit", fail)
    with pytest.raises(RuntimeError):
        service.decide(decision_id, decision="reject", actor="test")
    assert _count(eligible) == before
    assert len(service.pending()) == 1
