"""Tests for audited category-source-map cascade ownership."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.repositories.category_source_map_repo import CategorySourceMapRepo
from moneybin.services.undo_dispatch import repo_for


def test_upsert_inserts_category_and_subcategory_as_the_real_key(
    db: Database,
) -> None:
    """category+subcategory land in their own real key columns, verbatim."""
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
        "SELECT source_type, source_category_code, source_subcategory_code, "
        "code_level, category_id FROM app.category_source_map"
    ).fetchone()
    assert row is not None
    (
        source_type,
        source_category_code,
        source_subcategory_code,
        code_level,
        category_id,
    ) = row
    assert source_type == "chase_credit"
    assert source_category_code == "Groceries"
    assert source_subcategory_code == "Produce"
    assert code_level == "detailed"
    assert category_id == "cat-groceries"


def test_upsert_normalizes_a_missing_subcategory_to_the_empty_sentinel(
    db: Database,
) -> None:
    """A category with no subcategory stores '' — never NULL, never a real value."""
    repo = CategorySourceMapRepo(db)

    repo.upsert(
        source_type="mint",
        category="Rent",
        subcategory=None,
        category_id="cat-rent",
        actor="test",
    )

    row = db.execute(
        "SELECT source_category_code, source_subcategory_code "
        "FROM app.category_source_map WHERE source_type = 'mint'"
    ).fetchone()
    assert row == ("Rent", "")


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


def test_upsert_treats_distinct_subcategories_as_distinct_keys(db: Database) -> None:
    """Two subcategories under the same (source_type, category) are separate rows.

    The key is the full (source_type, source_category_code,
    source_subcategory_code) triple — a second upsert with a different
    subcategory must not collide with or overwrite the first.
    """
    repo = CategorySourceMapRepo(db)
    repo.upsert(
        source_type="chase_credit",
        category="Auto",
        subcategory="Gas",
        category_id="cat-gas",
        actor="test",
    )

    repo.upsert(
        source_type="chase_credit",
        category="Auto",
        subcategory="Repair",
        category_id="cat-repair",
        actor="test",
    )

    rows = db.execute(
        "SELECT source_subcategory_code, category_id FROM app.category_source_map "
        "WHERE source_type = 'chase_credit' ORDER BY source_subcategory_code"
    ).fetchall()
    assert rows == [("Gas", "cat-gas"), ("Repair", "cat-repair")]


def test_upsert_with_none_category_id_writes_an_ignored_row(db: Database) -> None:
    """category_id=None writes a NULL row, audited with the .ignore action."""
    repo = CategorySourceMapRepo(db)

    event = repo.upsert(
        source_type="mint",
        category="Misc",
        subcategory=None,
        category_id=None,
        actor="test",
    )

    assert event.action == "category_source_map.ignore"
    row = db.execute(
        "SELECT category_id FROM app.category_source_map "
        "WHERE source_type = 'mint' AND source_category_code = 'Misc'"
    ).fetchone()
    assert row == (None,)


def test_upsert_ignored_row_can_be_remapped(db: Database) -> None:
    """A second upsert with a real category_id overwrites a prior NULL row."""
    repo = CategorySourceMapRepo(db)
    repo.upsert(
        source_type="mint",
        category="Misc",
        subcategory=None,
        category_id=None,
        actor="test",
    )

    event = repo.upsert(
        source_type="mint",
        category="Misc",
        subcategory=None,
        category_id="cat-misc",
        actor="test",
    )

    assert event.action == "category_source_map.upsert"
    assert event.before_value is not None
    assert event.before_value["category_id"] is None
    assert event.after_value is not None
    assert event.after_value["category_id"] == "cat-misc"
    row = db.execute(
        "SELECT category_id FROM app.category_source_map "
        "WHERE source_type = 'mint' AND source_category_code = 'Misc'"
    ).fetchone()
    assert row == ("cat-misc",)


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
            (source_type, source_category_code, source_subcategory_code,
             code_level, category_id, source_taxonomy_version)
        VALUES ('plaid', 'FOOD_AND_DRINK', '', 'detailed', 'cat-task6', 'v2')
        """
    )
    repo = CategorySourceMapRepo(db)

    events = repo.delete_by_category("cat-task6", actor="mcp")

    assert len(events) == 1
    event = events[0]
    assert event.action == "category_source_map.delete"
    assert event.target_id == "plaid:FOOD_AND_DRINK:"
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
