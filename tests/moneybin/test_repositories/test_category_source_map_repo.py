"""Tests for audited category-source-map cascade ownership."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.repositories.category_source_map_repo import CategorySourceMapRepo
from moneybin.services.undo_dispatch import repo_for


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
