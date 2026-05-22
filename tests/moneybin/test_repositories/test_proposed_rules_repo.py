"""Tests for ``ProposedRulesRepo``.

Covers the five proposed-rule mutation shapes (insert, reinforce, supersede,
approve, reject); each pairs its write with a full before/after audit row.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.proposed_rules_repo import ProposedRulesRepo


def _audit_rows_for(db: Database, target_id: str) -> list[tuple[Any, ...]]:
    return db.conn.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor, parent_audit_id
          FROM app.audit_log
         WHERE target_id = ?
         ORDER BY occurred_at ASC, audit_id ASC
        """,
        [target_id],
    ).fetchall()


def _metric(action: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "moneybin_app_mutation_audit_emitted_total",
            {"repository": "proposed_rules", "action": action},
        )
        or 0.0
    )


def _insert(repo: ProposedRulesRepo, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "merchant_pattern": "NETFLIX",
        "match_type": "contains",
        "category": "Subscriptions",
        "subcategory": "Streaming",
        "category_id": None,
        "status": "tracking",
        "sample_txn_ids": ["txn1"],
        "actor": "system",
    }
    kwargs.update(overrides)
    return repo.insert(**kwargs)


def test_insert_writes_proposal_and_audit_row(db: Database) -> None:
    repo = ProposedRulesRepo(db)
    before_metric = _metric("proposed_rule.insert")

    event = _insert(repo)
    pid = event.target_id
    assert pid is not None
    assert len(pid) == 12

    row = db.conn.execute(
        "SELECT merchant_pattern, status, trigger_count "
        "FROM app.proposed_rules WHERE proposed_rule_id = ?",
        [pid],
    ).fetchone()
    assert row == ("NETFLIX", "tracking", 1)

    audit = _audit_rows_for(db, pid)
    assert audit[0][0] == "proposed_rule.insert"
    assert audit[0][4] is None  # before
    assert json.loads(audit[0][5])["merchant_pattern"] == "NETFLIX"
    assert _metric("proposed_rule.insert") - before_metric == 1.0


def test_reinforce_captures_before_and_after(db: Database) -> None:
    repo = ProposedRulesRepo(db)
    pid = _insert(repo).target_id

    repo.reinforce(
        pid,
        trigger_count=3,
        sample_txn_ids=["txn1", "txn2", "txn3"],
        status="pending",
        category_id=None,
        actor="system",
    )

    reinforce = next(
        r for r in _audit_rows_for(db, pid) if r[0] == "proposed_rule.reinforce"
    )
    assert json.loads(reinforce[4])["trigger_count"] == 1
    after = json.loads(reinforce[5])
    assert after["trigger_count"] == 3
    assert after["status"] == "pending"


def test_supersede_marks_status(db: Database) -> None:
    repo = ProposedRulesRepo(db)
    pid = _insert(repo).target_id
    repo.supersede(pid, actor="system")
    row = db.conn.execute(
        "SELECT status FROM app.proposed_rules WHERE proposed_rule_id = ?", [pid]
    ).fetchone()
    assert row == ("superseded",)


def test_mark_approved_sets_rule_id_and_threads_parent(db: Database) -> None:
    repo = ProposedRulesRepo(db)
    pid = _insert(repo, status="pending").target_id

    event = repo.mark_approved(
        pid, rule_id="rule123", actor="cli", parent_audit_id="p9"
    )

    row = db.conn.execute(
        "SELECT status, rule_id, decided_by FROM app.proposed_rules "
        "WHERE proposed_rule_id = ?",
        [pid],
    ).fetchone()
    assert row == ("approved", "rule123", "user")
    assert event.parent_audit_id == "p9"
    approve = next(
        r for r in _audit_rows_for(db, pid) if r[0] == "proposed_rule.approve"
    )
    assert approve[7] == "p9"  # parent_audit_id column


def test_mark_rejected_sets_status(db: Database) -> None:
    repo = ProposedRulesRepo(db)
    pid = _insert(repo, status="pending").target_id
    repo.mark_rejected(pid, actor="cli")
    row = db.conn.execute(
        "SELECT status, decided_by FROM app.proposed_rules WHERE proposed_rule_id = ?",
        [pid],
    ).fetchone()
    assert row == ("rejected", "user")


def test_reinforce_raises_on_missing_proposal(db: Database) -> None:
    repo = ProposedRulesRepo(db)
    with pytest.raises(ValueError, match="not found"):
        repo.reinforce(
            "nope",
            trigger_count=2,
            sample_txn_ids=[],
            status="pending",
            category_id=None,
            actor="system",
        )
    assert _audit_rows_for(db, "nope") == []


def test_insert_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = ProposedRulesRepo(db, audit=audit)
    with pytest.raises(RuntimeError, match="simulated audit failure"):
        _insert(repo, merchant_pattern="GHOSTPROP")
    rows = db.conn.execute(
        "SELECT 1 FROM app.proposed_rules WHERE merchant_pattern = ?", ["GHOSTPROP"]
    ).fetchall()
    assert rows == []
