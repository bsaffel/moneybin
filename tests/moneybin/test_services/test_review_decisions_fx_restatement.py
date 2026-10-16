"""Materialized-model coherence after consolidated match decisions."""

from __future__ import annotations

from collections.abc import Collection
from typing import Literal

import pytest

from moneybin.database import Database
from moneybin.protocol.write_contracts import MatchDecisionRequest
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.services.review_decisions_service import ReviewDecisionsService
from moneybin.tables import MATCH_DECISIONS, TableRef


def _insert_pending(db: Database, match_type: str) -> None:
    MatchDecisionsRepo(db).insert(
        match_id="review-decision",
        source_transaction_id_a="out",
        source_type_a="manual",
        source_origin_a="user",
        source_transaction_id_b="in",
        source_type_b="manual",
        source_origin_b="user",
        account_id="acct-a",
        account_id_b="acct-b" if match_type == "transfer" else None,
        confidence_score=1.0,
        match_signals={},
        match_tier=None,
        match_type=match_type,
        match_status="pending",
        decided_by="matcher",
        actor="test",
    )


def _record_restatements(
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[Database, list[TableRef], str]]:
    restated: list[tuple[Database, list[TableRef], str]] = []

    def record(
        target_db: Database, inputs: Collection[TableRef], *, committed_change: str
    ) -> None:
        restated.append((target_db, list(inputs), committed_change))

    monkeypatch.setattr("moneybin.services.restate.restate_after_write", record)
    return restated


def _decide(db: Database, decision: Literal["accept", "reject"]) -> None:
    ReviewDecisionsService(db, actor="test").apply_ordinary([
        MatchDecisionRequest(
            kind="match", decision_id="review-decision", decision=decision
        )
    ])


@pytest.mark.parametrize("match_type", ["transfer", "dedup"])
def test_accepting_a_match_through_reviews_restates_match_decisions(
    db: Database, monkeypatch: pytest.MonkeyPatch, match_type: str
) -> None:
    """Any accept restates, dedup included: the merged ledger reads it."""
    _insert_pending(db, match_type)
    restated = _record_restatements(monkeypatch)

    _decide(db, "accept")

    assert restated == [(db, [MATCH_DECISIONS], "match decision")]


def test_rejecting_a_pending_match_through_reviews_restates_nothing(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Models read only accepted decisions, so pending → rejected changes none."""
    _insert_pending(db, "transfer")
    restated = _record_restatements(monkeypatch)

    _decide(db, "reject")

    assert restated == []
