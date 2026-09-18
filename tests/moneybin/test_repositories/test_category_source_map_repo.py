"""Tests for audited category-source-map cascade ownership."""

from __future__ import annotations

import json

from moneybin.database import Database
from moneybin.repositories.category_source_map_repo import CategorySourceMapRepo
from moneybin.services.undo_dispatch import repo_for


def test_upsert_inserts_json_encoded_composite_code(db: Database) -> None:
    """category+subcategory encode into one opaque, delimiter-free code."""
    repo = CategorySourceMapRepo(db)

    event = repo.upsert(
        source_type="chase_credit",
        category="Groceries",
        subcategory="Produce",
        category_id="cat-groceries",
        actor="test",
    )

    assert event.action == "category_source_map.upsert"
    row = db.execute(
        "SELECT source_type, source_category_code, code_level, category_id "
        "FROM app.category_source_map"
    ).fetchone()
    assert row is not None
    source_type, source_category_code, code_level, category_id = row
    assert source_type == "chase_credit"
    assert code_level == "detailed"
    assert category_id == "cat-groceries"
    # Opaque but verbatim: the code round-trips to the exact input strings.
    assert json.loads(source_category_code) == {
        "category": "Groceries",
        "subcategory": "Produce",
    }


def test_upsert_encodes_a_missing_subcategory_as_null(db: Database) -> None:
    """A category with no subcategory still produces a stable, decodable code."""
    repo = CategorySourceMapRepo(db)

    repo.upsert(
        source_type="mint",
        category="Rent",
        subcategory=None,
        category_id="cat-rent",
        actor="test",
    )

    code = db.execute(
        "SELECT source_category_code FROM app.category_source_map "
        "WHERE source_type = 'mint'"
    ).fetchone()
    assert code is not None
    assert json.loads(code[0]) == {"category": "Rent", "subcategory": None}


def test_upsert_is_idempotent_and_updates_category_id(db: Database) -> None:
    """A second upsert on the same (source_type, category, subcategory) updates in place."""
    repo = CategorySourceMapRepo(db)
    repo.upsert(
        source_type="tiller",
        category="Dining",
        subcategory=None,
        category_id="cat-old",
        actor="test",
    )

    event = repo.upsert(
        source_type="tiller",
        category="Dining",
        subcategory=None,
        category_id="cat-new",
        source_taxonomy_version="v2",
        actor="test",
    )

    assert event.before_value is not None
    assert event.before_value["category_id"] == "cat-old"
    assert event.after_value is not None
    assert event.after_value["category_id"] == "cat-new"
    rows = db.execute(
        "SELECT category_id, source_taxonomy_version FROM app.category_source_map "
        "WHERE source_type = 'tiller'"
    ).fetchall()
    assert rows == [("cat-new", "v2")]


def test_upsert_is_audited_and_undoable(db: Database) -> None:
    repo = CategorySourceMapRepo(db)

    event = repo.upsert(
        source_type="chase_credit",
        category="Coffee",
        subcategory=None,
        category_id="cat-coffee",
        actor="mcp",
    )

    owner = repo_for("app", "category_source_map", db)
    owner.undo_event(event, actor="mcp")

    assert db.execute("SELECT COUNT(*) FROM app.category_source_map").fetchone() == (0,)


def test_delete_by_category_is_audited_and_undoable(db: Database) -> None:
    db.execute(
        """
        INSERT INTO app.category_source_map
            (source_type, source_category_code, code_level, category_id,
             source_taxonomy_version)
        VALUES ('plaid', 'FOOD_AND_DRINK', 'detailed', 'cat-task6', 'v2')
        """
    )
    repo = CategorySourceMapRepo(db)

    events = repo.delete_by_category("cat-task6", actor="mcp")

    assert len(events) == 1
    event = events[0]
    assert event.action == "category_source_map.delete"
    assert event.target_id == "plaid:FOOD_AND_DRINK"
    assert event.before_value is not None
    assert event.before_value["source_taxonomy_version"] == "v2"
    assert db.execute(
        "SELECT COUNT(*) FROM app.category_source_map WHERE category_id = 'cat-task6'"
    ).fetchone() == (0,)

    owner = repo_for("app", "category_source_map", db)
    owner.undo_event(event, actor="mcp")

    assert db.execute(
        "SELECT source_type, source_category_code, category_id "
        "FROM app.category_source_map"
    ).fetchall() == [("plaid", "FOOD_AND_DRINK", "cat-task6")]
