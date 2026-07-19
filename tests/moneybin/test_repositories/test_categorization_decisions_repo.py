"""Tests for the canonical audited categorization-decision lifecycle."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest

from moneybin.database import Database
from moneybin.repositories.categorization_decisions_repo import (
    CategorizationDecisionsRepo,
    categorization_decision_id,
)
from moneybin.repositories.transaction_categories_repo import (
    TransactionCategoriesRepo,
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
    assert categorization_decision_id("txn-1", attempt_number=2) == f"{first}_a2"
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
    TransactionCategoriesRepo(db).set(
        "txn-accept",
        category="Food",
        subcategory=None,
        category_id="cat-food",
        actor="test",
    )

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


def test_new_attempt_follows_categorized_then_cleared_lifecycle(
    db: Database,
) -> None:
    repo = CategorizationDecisionsRepo(db)
    first = repo.ensure_pending("txn-versioned", actor="mcp")
    repo.update_status(
        first["decision_id"],
        status="rejected",
        category_id=None,
        merchant_id=None,
        decided_by="user",
        actor="mcp",
    )
    assert repo.project_pending_attempts(["txn-versioned"]) == {}
    categories = TransactionCategoriesRepo(db)
    categories.set(
        "txn-versioned",
        category="Food",
        subcategory=None,
        category_id="cat-food",
        actor="test",
    )
    categories.clear("txn-versioned", actor="test")

    projected = repo.project_pending_attempts(["txn-versioned"])
    expected_id = categorization_decision_id("txn-versioned", attempt_number=2)
    assert projected["txn-versioned"]["decision_id"] == expected_id
    second = repo.ensure_pending(
        "txn-versioned",
        expected_decision_id=expected_id,
        actor="mcp",
    )

    assert second["attempt_number"] == 2
    first_after = repo.fetch_by_id(first["decision_id"])
    assert first_after is not None
    assert first_after["status"] == "rejected"
    assert [row["decision_id"] for row in repo.history()] == [first["decision_id"]]


def test_stale_pending_attempt_is_superseded_not_reopened(db: Database) -> None:
    repo = CategorizationDecisionsRepo(db)
    first = repo.ensure_pending("txn-stale-pending", actor="mcp")
    categories = TransactionCategoriesRepo(db)
    categories.set(
        "txn-stale-pending",
        category="Food",
        subcategory=None,
        category_id="cat-food",
        actor="test",
    )
    categories.clear("txn-stale-pending", actor="test")
    expected_id = categorization_decision_id(
        "txn-stale-pending",
        attempt_number=2,
    )

    second = repo.ensure_pending(
        "txn-stale-pending",
        expected_decision_id=expected_id,
        actor="mcp",
    )

    assert second["attempt_number"] == 2
    first_after = repo.fetch_by_id(first["decision_id"])
    assert first_after is not None
    assert first_after["status"] == "superseded"
    assert {row["decision_id"] for row in repo.history()} == {first["decision_id"]}


def test_sql_constraints_align_pending_and_accepted_targets(db: Database) -> None:
    with pytest.raises(duckdb.ConstraintException):
        db.execute(
            """
            INSERT INTO app.categorization_decisions (
                decision_id, transaction_id, status, category_id
            ) VALUES ('bad-pending', 'txn-bad-pending', 'pending', 'cat-food')
            """
        )
    with pytest.raises(duckdb.ConstraintException):
        db.execute(
            """
            INSERT INTO app.categorization_decisions (
                decision_id, transaction_id, status, decided_at, decided_by
            ) VALUES (
                'bad-accepted', 'txn-bad-accepted', 'accepted',
                CURRENT_TIMESTAMP, 'user'
            )
            """
        )


def test_pending_insert_rolls_back_when_audit_fails(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("audit failed")
    repo = CategorizationDecisionsRepo(db, audit=audit)

    with pytest.raises(RuntimeError, match="audit failed"):
        repo.ensure_pending("txn-rollback", actor="mcp")

    assert repo.fetch_by_transaction_id("txn-rollback") is None
