"""Tests for ``AccountLinkDecisionsRepo``.

Mirrors test_match_decisions_repo.py: the insert writes a pending proposal and a
paired ``app.audit_log`` row in one transaction, with ``match_signals`` stored as
JSON and decoded (not doubly-encoded) in the audit payload.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.account_link_decisions_repo import (
    AccountLinkDecisionsRepo,
)


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
            {"repository": "account_link_decisions", "action": action},
        )
        or 0.0
    )


def _insert(repo: AccountLinkDecisionsRepo, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "decision_id": "dec00000001",
        "provisional_account_id": "acct_prov_1",
        "candidate_account_id": "acct_cand_1",
        "confidence_score": 0.72,
        "match_signals": {"signal": "institution_last4", "value": "4267"},
        "status": "pending",
        "decided_by": "auto",
        "actor": "system",
    }
    kwargs.update(overrides)
    return repo.insert(**kwargs)


def test_insert_writes_row_and_audit_row(db: Database) -> None:
    repo = AccountLinkDecisionsRepo(db)
    before_metric = _metric("account_link_decision.insert")

    event = _insert(repo)
    assert event.target_id == "dec00000001"

    row = db.conn.execute(
        "SELECT status, decided_by, provisional_account_id, candidate_account_id "
        "FROM app.account_link_decisions WHERE decision_id = ?",
        ["dec00000001"],
    ).fetchone()
    assert row == ("pending", "auto", "acct_prov_1", "acct_cand_1")

    audit = _audit_rows_for(db, "dec00000001")
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "account_link_decision.insert"
    assert (schema, table, target_id) == (
        "app",
        "account_link_decisions",
        "dec00000001",
    )
    assert before is None
    after_json = json.loads(after)
    assert after_json["status"] == "pending"
    # match_signals decodes to a nested object in the audit payload.
    assert after_json["match_signals"]["signal"] == "institution_last4"
    assert actor == "system"

    assert _metric("account_link_decision.insert") - before_metric == 1.0


@pytest.mark.parametrize("bad_value", ["bogus", "system"])
def test_insert_rejects_invalid_decided_by(db: Database, bad_value: str) -> None:
    """decided_by is constrained to this table's domain (auto/user) — not 'system'."""
    repo = AccountLinkDecisionsRepo(db)
    with pytest.raises(duckdb.ConstraintException):
        _insert(repo, decided_by=bad_value)


def test_rejects_invalid_reversed_by(db: Database) -> None:
    """reversed_by is domain-constrained (auto/user) even though reverse() lands later."""
    repo = AccountLinkDecisionsRepo(db)
    _insert(repo)
    with pytest.raises(duckdb.ConstraintException):
        db.conn.execute(  # noqa: S608  # test input, not executing user SQL
            "UPDATE app.account_link_decisions SET reversed_by = 'bogus' "
            "WHERE decision_id = 'dec00000001'"
        )


def test_insert_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = AccountLinkDecisionsRepo(db, audit=audit)

    with pytest.raises(RuntimeError):
        _insert(repo, decision_id="ghost_decision")

    rows = db.conn.execute(
        "SELECT 1 FROM app.account_link_decisions WHERE decision_id = ?",
        ["ghost_decision"],
    ).fetchall()
    assert rows == []
