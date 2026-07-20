"""Tests for ``BudgetsRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, and that ``before_value``
captures the FULL prior row (Req 4) — budgets carry a ``DECIMAL`` amount, so
the full-row capture exercises the ``Decimal`` → ``str`` serialization path.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.budgets_repo import BudgetsRepo


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
            {"repository": "budgets", "action": action},
        )
        or 0.0
    )


def _insert(repo: BudgetsRepo, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "category": "Dining",
        "category_id": "cat_dining",
        "monthly_amount": Decimal("200.00"),
        "start_month": "2026-05",
        "actor": "mcp",
    }
    kwargs.update(overrides)
    return repo.insert(**kwargs)


def test_insert_writes_budget_and_audit(db: Database) -> None:
    repo = BudgetsRepo(db)
    before_metric = _metric("budget.set")

    event = _insert(repo)
    bid = event.target_id
    assert bid is not None
    assert len(bid) == 12

    row = db.conn.execute(
        "SELECT category, category_id, monthly_amount, start_month "
        "FROM app.budgets WHERE budget_id = ?",
        [bid],
    ).fetchone()
    assert row == ("Dining", "cat_dining", Decimal("200.00"), "2026-05")

    audit = _audit_rows_for(db, bid)
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "budget.set"
    assert (schema, table, target_id) == ("app", "budgets", bid)
    assert before is None
    assert json.loads(after)["monthly_amount"] == "200.00"  # Decimal serialized as str
    assert actor == "mcp"

    assert _metric("budget.set") - before_metric == 1.0


def test_update_captures_full_prior_row(db: Database) -> None:
    repo = BudgetsRepo(db)
    bid = _insert(repo, monthly_amount=Decimal("200.00")).target_id

    event = repo.update(
        bid, monthly_amount=Decimal("350.00"), category_id="cat_dining", actor="mcp"
    )
    assert event.target_id == bid

    row = db.conn.execute(
        "SELECT monthly_amount FROM app.budgets WHERE budget_id = ?", [bid]
    ).fetchone()
    assert row == (Decimal("350.00"),)

    update_audit = _audit_rows_for(db, bid)[1]
    before = json.loads(update_audit[4])
    after = json.loads(update_audit[5])
    assert before["monthly_amount"] == "200.00"
    assert after["monthly_amount"] == "350.00"


def test_update_raises_for_missing_budget(db: Database) -> None:
    repo = BudgetsRepo(db)
    with pytest.raises(ValueError, match="budget_id"):
        repo.update(
            "missing", monthly_amount=Decimal("1.00"), category_id=None, actor="mcp"
        )


def test_insert_records_parent_audit_id(db: Database) -> None:
    repo = BudgetsRepo(db)
    event = _insert(repo, parent_audit_id="p1")
    assert _audit_rows_for(db, event.target_id or "")[0][7] == "p1"


def test_delete_and_delete_by_category_preserve_audited_before_images(
    db: Database,
) -> None:
    repo = BudgetsRepo(db)
    one = _insert(repo, category_id="cat_target").target_id
    two = _insert(repo, category_id="cat_target").target_id
    other = _insert(repo, category_id="cat_other").target_id
    assert one is not None and two is not None and other is not None

    repo.delete(one, actor="mcp")
    events = repo.delete_by_category("cat_target", actor="mcp")

    assert [event.target_id for event in events] == [two]
    assert db.conn.execute(
        "SELECT budget_id FROM app.budgets ORDER BY budget_id"
    ).fetchall() == [(other,)]
    for target_id in (one, two):
        delete_audit = _audit_rows_for(db, target_id)[-1]
        assert delete_audit[0] == "budget.delete"
        assert json.loads(delete_audit[4])["category_id"] == "cat_target"
        assert delete_audit[5] is None


def test_insert_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = BudgetsRepo(db, audit=audit)

    with pytest.raises(RuntimeError):
        _insert(repo, category="GhostCat")

    rows = db.conn.execute(
        "SELECT 1 FROM app.budgets WHERE category = ?", ["GhostCat"]
    ).fetchall()
    assert rows == []
