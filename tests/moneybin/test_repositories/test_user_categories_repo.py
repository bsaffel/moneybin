"""Tests for ``UserCategoriesRepo`` + ``CategoryOverridesRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, and that ``before_value``
captures the FULL prior row (Req 4).
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.user_categories_repo import (
    CategoryOverridesRepo,
    UserCategoriesRepo,
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


def _metric(repository: str, action: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "moneybin_app_mutation_audit_emitted_total",
            {"repository": repository, "action": action},
        )
        or 0.0
    )


# ---------------------------------------------------------------------------
# UserCategoriesRepo.insert
# ---------------------------------------------------------------------------


def test_insert_writes_category_and_audit_row(db: Database) -> None:
    repo = UserCategoriesRepo(db)
    before_metric = _metric("user_categories", "user_category.insert")

    event = repo.insert(
        category="Dining",
        subcategory="Coffee",
        description="Cafés and coffee shops",
        actor="user",
    )
    cid = event.target_id
    assert cid is not None
    assert len(cid) == 12  # truncated UUID4 hex per identifiers.md

    rows = db.conn.execute(
        "SELECT category, subcategory, description, is_active "
        "FROM app.user_categories WHERE category_id = ?",
        [cid],
    ).fetchall()
    assert rows == [("Dining", "Coffee", "Cafés and coffee shops", True)]

    audit = _audit_rows_for(db, cid)
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, parent = audit[0]
    assert action == "user_category.insert"
    assert (schema, table, target_id) == ("app", "user_categories", cid)
    assert before is None
    after_decoded = json.loads(after)
    assert after_decoded["category"] == "Dining"
    assert after_decoded["subcategory"] == "Coffee"
    assert after_decoded["is_active"] is True
    assert actor == "user"
    assert parent is None

    assert _metric("user_categories", "user_category.insert") - before_metric == 1.0


def test_insert_records_parent_audit_id_when_supplied(db: Database) -> None:
    repo = UserCategoriesRepo(db)
    event = repo.insert(category="Travel", actor="user", parent_audit_id="parent123")
    audit = _audit_rows_for(db, event.target_id or "")
    assert audit[0][7] == "parent123"  # parent_audit_id column


# ---------------------------------------------------------------------------
# UserCategoriesRepo.update_active
# ---------------------------------------------------------------------------


def test_update_active_captures_before_and_after(db: Database) -> None:
    repo = UserCategoriesRepo(db)
    cid = repo.insert(category="Hobbies", actor="user").target_id
    assert cid is not None

    repo.update_active(cid, is_active=False, actor="user")

    row = db.conn.execute(
        "SELECT is_active FROM app.user_categories WHERE category_id = ?", [cid]
    ).fetchone()
    assert row == (False,)

    audit = _audit_rows_for(db, cid)
    assert [r[0] for r in audit] == [
        "user_category.insert",
        "user_category.update_active",
    ]
    update_row = audit[1]
    before = json.loads(update_row[4])
    after = json.loads(update_row[5])
    assert before["is_active"] is True
    assert after["is_active"] is False


# ---------------------------------------------------------------------------
# UserCategoriesRepo.delete
# ---------------------------------------------------------------------------


def test_delete_captures_full_before_value(db: Database) -> None:
    repo = UserCategoriesRepo(db)
    cid = repo.insert(
        category="Subscriptions", subcategory="Streaming", actor="user"
    ).target_id
    assert cid is not None

    repo.delete(cid, actor="user")

    gone = db.conn.execute(
        "SELECT 1 FROM app.user_categories WHERE category_id = ?", [cid]
    ).fetchone()
    assert gone is None

    delete_audit = next(
        r for r in _audit_rows_for(db, cid) if r[0] == "user_category.delete"
    )
    before = json.loads(delete_audit[4])
    assert before["category"] == "Subscriptions"
    assert before["subcategory"] == "Streaming"
    assert delete_audit[5] is None  # after_value


# ---------------------------------------------------------------------------
# Atomicity: audit failure rolls back the mutation
# ---------------------------------------------------------------------------


def test_update_active_raises_on_missing_row(db: Database) -> None:
    repo = UserCategoriesRepo(db)
    with pytest.raises(ValueError, match="not found"):
        repo.update_active("nope", is_active=False, actor="user")
    # No phantom audit row for the nonexistent id.
    assert _audit_rows_for(db, "nope") == []


def test_delete_raises_on_missing_row(db: Database) -> None:
    repo = UserCategoriesRepo(db)
    with pytest.raises(ValueError, match="not found"):
        repo.delete("nope", actor="user")
    assert _audit_rows_for(db, "nope") == []


def test_insert_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = UserCategoriesRepo(db, audit=audit)

    with pytest.raises(RuntimeError, match="simulated audit failure"):
        repo.insert(category="GhostCategory", actor="user")

    rows = db.conn.execute(
        "SELECT 1 FROM app.user_categories WHERE category = ?", ["GhostCategory"]
    ).fetchall()
    assert rows == []


# ---------------------------------------------------------------------------
# CategoryOverridesRepo.set_active
# ---------------------------------------------------------------------------


def test_override_set_active_insert_then_update(db: Database) -> None:
    repo = CategoryOverridesRepo(db)
    before_metric = _metric("category_overrides", "category_override.set_active")

    # First call: INSERT path — before is None.
    event1 = repo.set_active("seed-cat-1", is_active=False, actor="user")
    assert event1.target_id == "seed-cat-1"
    audit1 = _audit_rows_for(db, "seed-cat-1")
    assert len(audit1) == 1
    assert audit1[0][4] is None  # before_value (insert)
    assert json.loads(audit1[0][5])["is_active"] is False

    # Second call: ON CONFLICT UPDATE path — before captures prior row.
    repo.set_active("seed-cat-1", is_active=True, actor="user")
    row = db.conn.execute(
        "SELECT is_active FROM app.category_overrides WHERE category_id = ?",
        ["seed-cat-1"],
    ).fetchone()
    assert row == (True,)

    audit2 = _audit_rows_for(db, "seed-cat-1")
    assert len(audit2) == 2
    update_before = json.loads(audit2[1][4])
    assert update_before["is_active"] is False
    assert json.loads(audit2[1][5])["is_active"] is True

    assert (
        _metric("category_overrides", "category_override.set_active") - before_metric
        == 2.0
    )


def test_override_unique_category_id(db: Database) -> None:
    """category_overrides has category_id PRIMARY KEY — second raw insert collides."""
    repo = CategoryOverridesRepo(db)
    repo.set_active("seed-cat-2", is_active=False, actor="user")
    # set_active upserts, so a second call must NOT raise (ON CONFLICT).
    repo.set_active("seed-cat-2", is_active=True, actor="user")
    count = db.conn.execute(
        "SELECT COUNT(*) FROM app.category_overrides WHERE category_id = ?",
        ["seed-cat-2"],
    ).fetchone()
    assert count == (1,)
    # A raw duplicate INSERT still violates the PK (sanity that the PK exists).
    with pytest.raises(duckdb.ConstraintException):
        db.conn.execute(
            "INSERT INTO app.category_overrides (category_id, is_active) "  # noqa: S608  # test input, not executing user SQL
            "VALUES ('seed-cat-2', true)"
        )
