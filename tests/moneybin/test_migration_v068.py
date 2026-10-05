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


def _seed_imported_mappings(db: Database) -> None:
    """Add old-shape imported mappings: the origin slug sits in source_type."""
    db.execute("""
        INSERT INTO app.category_source_map
            (source_type, source_category_code, source_subcategory_code,
             code_level, category_id)
        VALUES
            ('chase_credit', 'Coffee Shops', '', 'detailed', 'cat-coffee'),
            ('chase_credit', 'Travel', 'Air', 'detailed', 'cat-air'),
            ('user', 'Lunch', '', 'detailed', 'cat-lunch'),
            ('gone_exporter', 'Rent', '', 'detailed', 'cat-rent')
    """)


def _seed_raw_tabular_row(
    db: Database, transaction_id: str, source_type: str, source_origin: str
) -> None:
    db.execute(
        "INSERT INTO raw.tabular_transactions "
        "(transaction_id, account_id, transaction_date, amount, source_file, "
        "source_type, source_origin, import_id) "
        "VALUES (?, 'acct-1', '2026-01-05', -4.50, ?, ?, ?, 'imp-1')",
        [transaction_id, f"{transaction_id}.csv", source_type, source_origin],
    )


def _imported_keys(db: Database) -> list[tuple[str, str, str, str, str]]:
    return db.execute(
        "SELECT source_type, source_origin, source_category_code, "
        "source_subcategory_code, category_id FROM app.category_source_map "
        "WHERE source_type <> 'plaid' "
        "ORDER BY source_origin, source_category_code, source_type"
    ).fetchall()


def test_v068_rekeys_an_imported_mapping_per_carrying_source_type(
    db: Database,
) -> None:
    """The old key applied to every type of an origin, so each type keeps it."""
    _seed_v067_shape(db)
    _seed_imported_mappings(db)
    _seed_raw_tabular_row(db, "t1", "csv", "chase_credit")
    _seed_raw_tabular_row(db, "t2", "csv", "chase_credit")
    _seed_raw_tabular_row(db, "t3", "excel", "chase_credit")
    _seed_raw_tabular_row(db, "t4", "csv", "other_exporter")
    db.execute(
        "INSERT INTO raw.manual_transactions "
        "(source_transaction_id, import_id, account_id, transaction_date, "
        "amount, description, created_by) "
        "VALUES ('manual_1', 'imp-2', 'acct-1', '2026-01-06', -9.00, 'x', 'cli')"
    )

    run_migration(db, migrate)

    assert _imported_keys(db) == [
        ("csv", "chase_credit", "Coffee Shops", "", "cat-coffee"),
        ("excel", "chase_credit", "Coffee Shops", "", "cat-coffee"),
        ("csv", "chase_credit", "Travel", "Air", "cat-air"),
        ("excel", "chase_credit", "Travel", "Air", "cat-air"),
        ("manual", "user", "Lunch", "", "cat-lunch"),
    ]
    assert db.execute(
        "SELECT COUNT(*) FROM app.category_source_map WHERE source_type = 'plaid'"
    ).fetchone() == (3,)


def test_v068_keeps_a_plaid_row_for_an_import_whose_origin_is_plaid(
    db: Database,
) -> None:
    """The old key matched both Plaid rows and a `plaid`-origin import."""
    _seed_v067_shape(db)
    _seed_raw_tabular_row(db, "t1", "csv", "plaid")

    run_migration(db, migrate)

    assert db.execute(
        "SELECT source_type, source_origin, COUNT(*) FROM app.category_source_map "
        "GROUP BY ALL ORDER BY source_type"
    ).fetchall() == [("csv", "plaid", 3), ("plaid", "", 3)]


def test_v068_drops_an_imported_mapping_no_raw_row_carries(db: Database) -> None:
    """With no row of that origin there is no source type to key it under."""
    _seed_v067_shape(db)
    _seed_imported_mappings(db)

    run_migration(db, migrate)

    assert _imported_keys(db) == []
    assert db.execute("SELECT COUNT(*) FROM app.category_source_map").fetchone() == (3,)


def test_v068_idempotent_on_second_run(db: Database) -> None:
    _seed_v067_shape(db)

    run_migration(db, migrate)
    run_migration(db, migrate)

    assert db.execute("SELECT COUNT(*) FROM app.category_source_map").fetchone() == (3,)
