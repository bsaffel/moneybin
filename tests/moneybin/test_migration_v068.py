"""V068: key app.category_source_map by the row's source_type and source_origin."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.sql.migrations.V068__key_category_source_map_by_origin import migrate
from tests.moneybin.migration_helpers import column_exists, run_migration

_V067_CATEGORY_SOURCE_MAP_SQL = """
CREATE TABLE app.category_source_map (
    source_type VARCHAR NOT NULL,
    source_category_code VARCHAR NOT NULL,
    source_subcategory_code VARCHAR NOT NULL DEFAULT '',
    code_level VARCHAR NOT NULL DEFAULT 'detailed'
        CHECK (code_level IN ('detailed', 'primary')),
    category_id VARCHAR NOT NULL,
    source_taxonomy_version VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_type, source_category_code, source_subcategory_code)
)
"""


def _seed_v067_shape(db: Database) -> None:
    """Recreate the pre-V068 three-column-key table with provider rows."""
    db.execute("DROP TABLE app.category_source_map")
    db.execute(_V067_CATEGORY_SOURCE_MAP_SQL)
    db.execute("""
        INSERT INTO app.category_source_map
            (source_type, source_category_code, source_subcategory_code,
             code_level, category_id, source_taxonomy_version)
        VALUES
            ('plaid', 'FOOD_AND_DRINK_COFFEE', '', 'detailed', 'FND-COF', 'plaid_pfc_v2'),
            ('plaid', 'TRANSPORTATION', '', 'primary', 'TRP', 'plaid_pfc_v2'),
            ('plaid', 'GENERAL_MERCHANDISE', 'Online', 'detailed', 'GEN-ONL', NULL)
    """)


def test_v068_backfills_blank_origin_and_keeps_data(db: Database) -> None:
    """Existing rows are provider rows: they keep their data with origin ''."""
    _seed_v067_shape(db)
    assert not column_exists(db, "app", "category_source_map", "source_origin")

    run_migration(db, migrate)

    rows = db.execute(
        "SELECT source_type, source_origin, source_category_code, "
        "source_subcategory_code, code_level, category_id, source_taxonomy_version "
        "FROM app.category_source_map ORDER BY source_category_code"
    ).fetchall()
    assert rows == [
        (
            "plaid",
            "",
            "FOOD_AND_DRINK_COFFEE",
            "",
            "detailed",
            "FND-COF",
            "plaid_pfc_v2",
        ),
        ("plaid", "", "GENERAL_MERCHANDISE", "Online", "detailed", "GEN-ONL", None),
        ("plaid", "", "TRANSPORTATION", "", "primary", "TRP", "plaid_pfc_v2"),
    ]


def test_v068_key_admits_same_term_under_distinct_origins(db: Database) -> None:
    """The four-column key keeps one term separate per (type, origin)."""
    _seed_v067_shape(db)

    run_migration(db, migrate)

    db.execute(
        "INSERT INTO app.category_source_map "
        "(source_type, source_origin, source_category_code, "
        "source_subcategory_code, code_level, category_id) VALUES "
        "('csv', 'chase_credit', 'TRANSPORTATION', '', 'detailed', 'cat-a'), "
        "('excel', 'chase_credit', 'TRANSPORTATION', '', 'detailed', 'cat-b')"
    )
    assert db.execute("SELECT COUNT(*) FROM app.category_source_map").fetchone() == (5,)


def test_v068_is_a_no_op_on_a_fresh_install(db: Database) -> None:
    """A freshly-initialized database already has the new shape."""
    assert column_exists(db, "app", "category_source_map", "source_origin")

    run_migration(db, migrate)

    assert db.execute("SELECT COUNT(*) FROM app.category_source_map").fetchone() == (0,)


def test_v068_idempotent_on_second_run(db: Database) -> None:
    _seed_v067_shape(db)

    run_migration(db, migrate)
    run_migration(db, migrate)

    assert db.execute("SELECT COUNT(*) FROM app.category_source_map").fetchone() == (3,)
