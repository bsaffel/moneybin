"""Audit-emission tests for category taxonomy writes.

Per ``app-integrity-invariant.md`` Invariant 10, ``create_category`` /
``toggle_category`` / ``delete_category`` must emit a paired ``app.audit_log``
row (these bypassed audit before the repository migration). The write-behavior
itself is covered in ``test_categorization_service_writes.py``; this module
pins the audit routing.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.services.categorization import CategorizationService
from tests.moneybin.db_helpers import create_core_tables, seed_categories_view


@pytest.fixture()
def db(tmp_path: Path) -> Database:
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-key"
    database = Database(
        tmp_path / "test.duckdb", secret_store=mock_store, no_auto_upgrade=True
    )
    create_core_tables(database)
    return database


def _audit(db: Database, target_id: str) -> list[tuple[Any, ...]]:
    return db.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor
          FROM app.audit_log
         WHERE target_id = ?
         ORDER BY occurred_at ASC, audit_id ASC
        """,
        [target_id],
    ).fetchall()


@pytest.mark.unit
def test_create_category_emits_audit(db: Database) -> None:
    cat_id = CategorizationService(db).create_category(
        "Childcare", subcategory="Daycare", actor="cli"
    )
    audit = _audit(db, cat_id)
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor = audit[0]
    assert action == "user_category.insert"
    assert (schema, table, target_id) == ("app", "user_categories", cat_id)
    assert before is None
    assert json.loads(after)["category"] == "Childcare"
    assert actor == "cli"


@pytest.mark.unit
def test_toggle_default_category_emits_override_audit(db: Database) -> None:
    seed_categories_view(db)
    CategorizationService(db).toggle_category("FND", is_active=False, actor="cli")
    audit = _audit(db, "FND")
    assert [r[0] for r in audit] == ["category_override.set_active"]
    assert json.loads(audit[0][5])["is_active"] is False
    assert audit[0][6] == "cli"


@pytest.mark.unit
def test_toggle_user_category_emits_update_active_audit(db: Database) -> None:
    seed_categories_view(db)
    db.execute("""
        INSERT INTO app.user_categories
        (category_id, category, subcategory, is_active)
        VALUES ('CUSTOM1', 'Childcare', 'Daycare', true)
    """)
    CategorizationService(db).toggle_category("CUSTOM1", is_active=False, actor="cli")
    audit = _audit(db, "CUSTOM1")
    assert [r[0] for r in audit] == ["user_category.update_active"]
    assert json.loads(audit[0][4])["is_active"] is True  # before
    assert json.loads(audit[0][5])["is_active"] is False  # after


@pytest.mark.unit
def test_delete_category_emits_audit_with_full_before(db: Database) -> None:
    svc = CategorizationService(db)
    cat_id = svc.create_category("TestCat", actor="cli")
    svc.delete_category(cat_id, actor="cli")
    delete_audit = next(r for r in _audit(db, cat_id) if r[0] == "user_category.delete")
    assert json.loads(delete_audit[4])["category"] == "TestCat"  # before
    assert delete_audit[5] is None  # after
