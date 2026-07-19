"""Tests for the canonical audited categorization-decision lifecycle."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.repositories.categorization_decisions_repo import (
    CategorizationDecisionsRepo,
    categorization_decision_id,
)


def _audit_rows(db: Database, decision_id: str) -> list[tuple[Any, ...]]:
    return db.execute(
        """
        SELECT action, before_value, after_value, actor
        FROM app.audit_log
        WHERE target_table = 'categorization_decisions' AND target_id = ?
        ORDER BY occurred_at, audit_id
        """,
        [decision_id],
    ).fetchall()


def test_deterministic_id_is_stable_and_transaction_bound() -> None:
    first = categorization_decision_id("txn-1")

    assert first == categorization_decision_id("txn-1")
    assert first != categorization_decision_id("txn-2")
    assert first.startswith("cat_")
    assert len(first) == 20


def test_ensure_pending_inserts_once_with_full_audit(db: Database) -> None:
    repo = CategorizationDecisionsRepo(db)

    first = repo.ensure_pending("txn-1", actor="mcp")
    second = repo.ensure_pending("txn-1", actor="mcp")

    assert first["decision_id"] == categorization_decision_id("txn-1")
    assert first == second
    assert first["status"] == "pending"
    audits = _audit_rows(db, first["decision_id"])
    assert len(audits) == 1
    assert audits[0][0] == "categorization_decision.insert"
    assert audits[0][1] is None
    assert json.loads(audits[0][2])["transaction_id"] == "txn-1"


def test_terminal_transition_persists_outcome_and_rejects_redecision(
    db: Database,
) -> None:
    repo = CategorizationDecisionsRepo(db)
    pending = repo.ensure_pending("txn-accept", actor="mcp")

    accepted = repo.update_status(
        pending["decision_id"],
        status="accepted",
        category_id="cat-food",
        merchant_id="merchant-1",
        decided_by="user",
        actor="mcp",
    )

    assert accepted["status"] == "accepted"
    assert accepted["category_id"] == "cat-food"
    assert accepted["merchant_id"] == "merchant-1"
    with pytest.raises(ValueError, match="terminal"):
        repo.update_status(
            pending["decision_id"],
            status="accepted",
            category_id="cat-food",
            merchant_id="merchant-1",
            decided_by="user",
            actor="mcp",
        )
    update_audit = _audit_rows(db, pending["decision_id"])[1]
    assert json.loads(update_audit[1])["status"] == "pending"
    assert json.loads(update_audit[2])["status"] == "accepted"


def test_reject_is_a_durable_terminal_transition(db: Database) -> None:
    repo = CategorizationDecisionsRepo(db)
    pending = repo.ensure_pending("txn-reject", actor="mcp")

    rejected = repo.update_status(
        pending["decision_id"],
        status="rejected",
        category_id=None,
        merchant_id=None,
        decided_by="user",
        actor="mcp",
    )

    assert rejected["status"] == "rejected"
    assert repo.fetch_by_id(pending["decision_id"]) == rejected
    assert repo.list_pending() == []
    assert repo.history()[0]["decision_id"] == pending["decision_id"]


def test_pending_insert_rolls_back_when_audit_fails(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("audit failed")
    repo = CategorizationDecisionsRepo(db, audit=audit)

    with pytest.raises(RuntimeError, match="audit failed"):
        repo.ensure_pending("txn-rollback", actor="mcp")

    assert repo.fetch_by_transaction_id("txn-rollback") is None
