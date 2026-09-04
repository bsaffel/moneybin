"""FX-accounting coherence after consolidated match decisions."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.protocol.write_contracts import MatchDecisionRequest
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.services.review_decisions_service import ReviewDecisionsService


def test_accepting_transfer_through_reviews_restates_fx_accounting(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    MatchDecisionsRepo(db).insert(
        match_id="review-fx-transfer",
        source_transaction_id_a="out",
        source_type_a="manual",
        source_origin_a="user",
        source_transaction_id_b="in",
        source_type_b="manual",
        source_origin_b="user",
        account_id="acct-a",
        account_id_b="acct-b",
        confidence_score=1.0,
        match_signals={},
        match_tier=None,
        match_type="transfer",
        match_status="pending",
        decided_by="matcher",
        actor="test",
    )
    restated: list[Database] = []

    def record_restatement(target_db: Database, **_kwargs: object) -> None:
        restated.append(target_db)

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
        record_restatement,
    )

    ReviewDecisionsService(db, actor="test").apply_ordinary([
        MatchDecisionRequest(
            kind="match",
            decision_id="review-fx-transfer",
            decision="accept",
        )
    ])

    assert restated == [db]
