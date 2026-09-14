"""Planning failure and generic undo cannot partially mutate review state."""

from typing import Any
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.investment_match_decisions_repo import (
    InvestmentMatchDecisionsRepo,
)
from moneybin.services.investment_matching_service import InvestmentMatchingService
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)


def test_repository_audit_failure_rolls_back_pending_insert(
    comparison_db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)

    def fail(*args: object, **kwargs: object) -> None:
        raise RuntimeError("injected audit failure")

    monkeypatch.setattr(InvestmentMatchDecisionsRepo, "_emit_audit", fail)
    with pytest.raises(RuntimeError, match="injected audit failure"):
        InvestmentMatchingService(comparison_db).run()
    assert comparison_db.execute(
        "SELECT COUNT(*) FROM app.investment_match_decisions"
    ).fetchone() == (0,)


def test_generic_undo_of_planner_audit_is_unavailable(comparison_db: Database) -> None:
    repo = InvestmentMatchDecisionsRepo(comparison_db)
    with pytest.raises(UserError, match="review-only"):
        repo.undo_event(MagicMock(), actor="test")


@pytest.mark.parametrize("durable_status", ["accepted", "stale"])
@pytest.mark.parametrize("supplied_status", [None, "accepted", "stale"])
def test_replacement_lifecycle_comes_from_durable_decision(
    comparison_db: Database,
    durable_status: str,
    supplied_status: str | None,
) -> None:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)
    service = InvestmentMatchingService(comparison_db)
    service.run()
    original = service.pending()[0]
    comparison_db.execute(
        """UPDATE app.investment_match_decisions
        SET status = ?, accepted_at = CURRENT_TIMESTAMP""",
        [durable_status],
    )
    topology: dict[str, Any] = {
        "decision_id": original["proposal_id"],
        "members": original["members"],
        "reserved_rows": [
            (leg["source_type"], leg["source_origin"], leg["native_reference"])
            for leg in original["legs"]
        ],
        "field_resolutions": [],
        "curation_impact": [],
    }
    if supplied_status is not None:
        topology["status"] = supplied_status
    result = service.run(locked_components=[topology])
    assert result.proposed == (1 if durable_status == "stale" else 0)


def test_rejection_exclusion_emits_its_bounded_metric_after_commit(
    comparison_db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)
    service = InvestmentMatchingService(comparison_db)
    service.run()
    comparison_db.execute(
        "UPDATE app.investment_match_decisions SET status = 'rejected'"
    )
    counter = MagicMock()
    monkeypatch.setattr(
        "moneybin.services.investment_matching_service.investment_match_proposals_total",
        counter,
    )
    result = service.run()
    assert result.suppressed == 1
    counter.labels.assert_called_once_with(band="exact", outcome="suppressed")
    counter.labels.return_value.inc.assert_called_once_with()


@pytest.mark.parametrize(
    "topology",
    [
        [],
        [
            {
                "decision_id": "unrelated",
                "members": [],
                "reserved_rows": [],
                "field_resolutions": [],
                "curation_impact": [],
            }
        ],
    ],
)
def test_supplied_topology_must_cover_every_durable_active_match(
    comparison_db: Database,
    topology: list[dict[str, object]],
) -> None:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)
    service = InvestmentMatchingService(comparison_db)
    service.run()
    # Acceptance is an input from the later membership slice, not the operation under test.
    comparison_db.execute("""UPDATE app.investment_match_decisions
        SET status = 'accepted', accepted_at = CURRENT_TIMESTAMP""")
    seed_plaid_event(comparison_db, "alternative")
    before = comparison_db.execute(
        "SELECT * FROM app.investment_match_decisions"
    ).fetchall()
    with pytest.raises(UserError, match="membership"):
        service.run(locked_components=topology)
    assert (
        comparison_db.execute("SELECT * FROM app.investment_match_decisions").fetchall()
        == before
    )
