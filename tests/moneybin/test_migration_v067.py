"""V067: widen app.category_source_map's key with source_subcategory_code."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.sql.migrations.V067__add_source_subcategory_code import migrate
from tests.moneybin.migration_helpers import column_exists, run_migration

_OLD_CATEGORY_SOURCE_MAP_SQL = """
CREATE TABLE app.category_source_map (
    source_type VARCHAR NOT NULL,
    source_category_code VARCHAR NOT NULL,
    code_level VARCHAR NOT NULL DEFAULT 'detailed'
        CHECK (code_level IN ('detailed', 'primary')),
    category_id VARCHAR NOT NULL,
    source_taxonomy_version VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_type, source_category_code)
)
"""


def _seed_legacy_shape(db: Database) -> None:
    """Rebuild the pre-V067 two-column-key shape and populate it.

    Mirrors V061's test precedent (``seed_legacy_manual_identity_schema``):
    drop the fixture-built current-shape table and recreate the historical
    one so the migration has real pre-upgrade data to widen.
    """
    db.execute("DROP TABLE app.category_source_map")
    db.execute(_OLD_CATEGORY_SOURCE_MAP_SQL)
    db.execute("""
        INSERT INTO app.category_source_map
            (source_type, source_category_code, code_level, category_id,
             source_taxonomy_version)
        VALUES
            ('plaid', 'FOOD_AND_DRINK_COFFEE', 'detailed', 'FND-COF', 'plaid_pfc_v2'),
            ('plaid', 'TRANSPORTATION', 'primary', 'TRP', 'plaid_pfc_v2'),
            ('chase_credit', 'Groceries', 'detailed', 'cat-groceries', NULL)
    """)


def test_v067_widens_key_and_backfills_empty_sentinel(db: Database) -> None:
    """Existing rows keep their data and gain source_subcategory_code=''."""
    _seed_legacy_shape(db)
    assert not column_exists(
        db, "app", "category_source_map", "source_subcategory_code"
    )

    run_migration(db, migrate)

    assert column_exists(db, "app", "category_source_map", "source_subcategory_code")
    rows = db.execute(
        "SELECT source_type, source_category_code, source_subcategory_code, "
        "code_level, category_id, source_taxonomy_version "
        "FROM app.category_source_map ORDER BY source_type, source_category_code"
    ).fetchall()
    assert rows == [
        ("chase_credit", "Groceries", "", "detailed", "cat-groceries", None),
        ("plaid", "FOOD_AND_DRINK_COFFEE", "", "detailed", "FND-COF", "plaid_pfc_v2"),
        ("plaid", "TRANSPORTATION", "", "primary", "TRP", "plaid_pfc_v2"),
    ]


def test_v067_widened_key_accepts_distinct_subcategories(db: Database) -> None:
    """The new three-column primary key genuinely admits distinct subcategories."""
    _seed_legacy_shape(db)

    run_migration(db, migrate)

    db.execute(
        "INSERT INTO app.category_source_map "
        "(source_type, source_category_code, source_subcategory_code, "
        "code_level, category_id) "
        "VALUES ('chase_credit', 'Groceries', 'Produce', 'detailed', 'cat-produce')"
    )
    rows = db.execute(
        "SELECT source_subcategory_code, category_id FROM app.category_source_map "
        "WHERE source_type = 'chase_credit' AND source_category_code = 'Groceries' "
        "ORDER BY source_subcategory_code"
    ).fetchall()
    assert rows == [("", "cat-groceries"), ("Produce", "cat-produce")]


def test_v067_is_a_no_op_on_a_fresh_install(db: Database) -> None:
    """A freshly-initialized database already has the new shape -- no-op."""
    assert column_exists(db, "app", "category_source_map", "source_subcategory_code")

    run_migration(db, migrate)  # should not raise

    assert db.execute("SELECT COUNT(*) FROM app.category_source_map").fetchone() == (0,)


def test_v067_idempotent_on_second_run(db: Database) -> None:
    _seed_legacy_shape(db)

    run_migration(db, migrate)
    run_migration(db, migrate)  # should not raise

    assert db.execute("SELECT COUNT(*) FROM app.category_source_map").fetchone() == (3,)
