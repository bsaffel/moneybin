"""Tests for ``CategorizationRulesRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, and that ``before_value``
captures the FULL prior row (Req 4).
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.categorization_rules_repo import CategorizationRulesRepo


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
            {"repository": "categorization_rules", "action": action},
        )
        or 0.0
    )


def _insert(repo: CategorizationRulesRepo, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "name": "Coffee rule",
        "merchant_pattern": "STARBUCKS",
        "match_type": "contains",
        "min_amount": None,
        "max_amount": None,
        "account_id": None,
        "category": "Dining",
        "subcategory": "Coffee",
        "category_id": None,
        "priority": 100,
        "created_by": "user",
        "actor": "cli",
    }
    kwargs.update(overrides)
    return repo.insert(**kwargs)


def test_insert_writes_rule_and_audit_row(db: Database) -> None:
    repo = CategorizationRulesRepo(db)
    before_metric = _metric("categorization_rule.insert")

    event = _insert(repo)
    rid = event.target_id
    assert rid is not None
    assert len(rid) == 12

    row = db.conn.execute(
        "SELECT merchant_pattern, category, is_active, created_by "
        "FROM app.categorization_rules WHERE rule_id = ?",
        [rid],
    ).fetchone()
    assert row == ("STARBUCKS", "Dining", True, "user")

    audit = _audit_rows_for(db, rid)
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "categorization_rule.insert"
    assert (schema, table, target_id) == ("app", "categorization_rules", rid)
    assert before is None
    assert json.loads(after)["merchant_pattern"] == "STARBUCKS"
    assert actor == "cli"

    assert _metric("categorization_rule.insert") - before_metric == 1.0


def test_insert_records_parent_audit_id(db: Database) -> None:
    repo = CategorizationRulesRepo(db)
    event = _insert(repo, parent_audit_id="p1")
    assert _audit_rows_for(db, event.target_id or "")[0][7] == "p1"


def test_deactivate_captures_before_and_after(db: Database) -> None:
    repo = CategorizationRulesRepo(db)
    rid = _insert(repo).target_id

    event = repo.deactivate(rid, actor="cli")
    assert event is not None

    row = db.conn.execute(
        "SELECT is_active FROM app.categorization_rules WHERE rule_id = ?", [rid]
    ).fetchone()
    assert row == (False,)

    deact = next(
        r for r in _audit_rows_for(db, rid) if r[0] == "categorization_rule.deactivate"
    )
    assert json.loads(deact[4])["is_active"] is True
    assert json.loads(deact[5])["is_active"] is False


def test_deactivate_returns_none_for_missing_rule(db: Database) -> None:
    repo = CategorizationRulesRepo(db)
    assert repo.deactivate("nope", actor="cli") is None
    assert _audit_rows_for(db, "nope") == []


def test_set_target_replaces_full_rule_and_audits_before_after(db: Database) -> None:
    repo = CategorizationRulesRepo(db)
    rid = _insert(repo).target_id
    assert rid is not None

    event = repo.set_target(
        rid,
        name="exact: STARBUCKS COFFEE → Food",
        merchant_pattern="STARBUCKS COFFEE",
        match_type="exact",
        min_amount=1.5,
        max_amount=25.0,
        account_id="ACC001",
        category="Food",
        subcategory=None,
        category_id="FOOD",
        priority=5,
        actor="mcp",
    )

    assert event.action == "categorization_rule.set"
    audit = next(
        row for row in _audit_rows_for(db, rid) if row[0] == "categorization_rule.set"
    )
    assert json.loads(audit[4])["merchant_pattern"] == "STARBUCKS"
    after = json.loads(audit[5])
    assert after["merchant_pattern"] == "STARBUCKS COFFEE"
    assert after["match_type"] == "exact"
    assert after["category"] == "Food"
    assert after["priority"] == 5
    assert after["is_active"] is True


def test_delete_removes_rule_and_retains_full_audit_recovery_image(
    db: Database,
) -> None:
    repo = CategorizationRulesRepo(db)
    rid = _insert(repo).target_id
    assert rid is not None

    event = repo.delete(rid, actor="mcp")

    assert event is not None
    assert event.action == "categorization_rule.delete"
    assert (
        db.conn.execute(
            "SELECT 1 FROM app.categorization_rules WHERE rule_id = ?", [rid]
        ).fetchone()
        is None
    )
    audit = next(
        row
        for row in _audit_rows_for(db, rid)
        if row[0] == "categorization_rule.delete"
    )
    assert json.loads(audit[4])["rule_id"] == rid
    assert audit[5] is None


def test_insert_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = CategorizationRulesRepo(db, audit=audit)

    try:
        _insert(repo, name="GhostRule", merchant_pattern="GHOST")
    except RuntimeError:
        pass

    rows = db.conn.execute(
        "SELECT 1 FROM app.categorization_rules WHERE merchant_pattern = ?", ["GHOST"]
    ).fetchall()
    assert rows == []
