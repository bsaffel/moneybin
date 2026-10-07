"""V069: app.category_source_map.category_id accepts NULL (an ignored term)."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V069__allow_ignored_category_source_mapping import (
    migrate,
)
from tests.moneybin.migration_helpers import column_info, run_migration

_V068_CATEGORY_SOURCE_MAP_SQL = """
CREATE TABLE app.category_source_map (
    source_type VARCHAR NOT NULL,
    source_origin VARCHAR NOT NULL,
    source_category_code VARCHAR NOT NULL,
    source_subcategory_code VARCHAR NOT NULL DEFAULT '',
    code_level VARCHAR NOT NULL DEFAULT 'detailed'
        CHECK (code_level IN ('detailed', 'primary')),
    category_id VARCHAR NOT NULL,
    source_taxonomy_version VARCHAR,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (
        source_type, source_origin, source_category_code, source_subcategory_code
    )
)
"""

_ROWS = [
    ("csv", "chase_credit", "Coffee Shops", "", "detailed", "cat-coffee", None),
    ("csv", "chase_credit", "Travel", "Air", "detailed", "cat-air", None),
    ("plaid", "", "FOOD_AND_DRINK_COFFEE", "", "detailed", "FND-COF", "plaid_pfc_v2"),
    ("plaid", "", "TRANSPORTATION", "", "primary", "TRP", "plaid_pfc_v2"),
]


@pytest.fixture
def v068_db(db: Database) -> Database:
    """A populated table in the V068 shape, where category_id is NOT NULL."""
    db.execute("DROP TABLE app.category_source_map")
    db.execute(_V068_CATEGORY_SOURCE_MAP_SQL)
    db.execute("""
        INSERT INTO app.category_source_map
            (source_type, source_origin, source_category_code,
             source_subcategory_code, code_level, category_id,
             source_taxonomy_version)
        VALUES
            ('csv', 'chase_credit', 'Coffee Shops', '', 'detailed', 'cat-coffee', NULL),
            ('csv', 'chase_credit', 'Travel', 'Air', 'detailed', 'cat-air', NULL),
            ('plaid', '', 'FOOD_AND_DRINK_COFFEE', '', 'detailed', 'FND-COF',
             'plaid_pfc_v2'),
            ('plaid', '', 'TRANSPORTATION', '', 'primary', 'TRP', 'plaid_pfc_v2')
    """)
    assert column_info(db, "app", "category_source_map", "category_id")[1] is False
    return db


def _rows(db: Database) -> list[tuple[object, ...]]:
    return db.execute(
        "SELECT source_type, source_origin, source_category_code, "
        "source_subcategory_code, code_level, category_id, source_taxonomy_version "
        "FROM app.category_source_map "
        "ORDER BY source_type, source_category_code"
    ).fetchall()


def test_v069_makes_category_id_nullable_and_keeps_every_row(
    v068_db: Database,
) -> None:
    run_migration(v068_db, migrate)

    assert column_info(v068_db, "app", "category_source_map", "category_id") == (
        "VARCHAR",
        True,
    )
    assert _rows(v068_db) == _ROWS


def test_v069_keeps_the_primary_key(v068_db: Database) -> None:
    """Dropping the constraint in place must not lose the four-column key."""
    run_migration(v068_db, migrate)

    with pytest.raises(Exception, match="(?i)duplicate key|primary key"):
        v068_db.execute(
            "INSERT INTO app.category_source_map "
            "(source_type, source_origin, source_category_code, category_id) "
            "VALUES ('csv', 'chase_credit', 'Coffee Shops', 'cat-other')"
        )


def test_v069_admits_an_ignored_row(v068_db: Database) -> None:
    run_migration(v068_db, migrate)

    v068_db.execute(
        "INSERT INTO app.category_source_map "
        "(source_type, source_origin, source_category_code, category_id) "
        "VALUES ('csv', 'chase_credit', 'Uncategorized', NULL)"
    )
    assert v068_db.execute(
        "SELECT COUNT(*) FROM app.category_source_map WHERE category_id IS NULL"
    ).fetchone() == (1,)


def test_v069_idempotent_on_second_run(v068_db: Database) -> None:
    run_migration(v068_db, migrate)
    run_migration(v068_db, migrate)

    assert _rows(v068_db) == _ROWS


def test_v069_is_a_no_op_on_a_fresh_install(db: Database) -> None:
    """A freshly-initialized database already has the nullable column."""
    assert column_info(db, "app", "category_source_map", "category_id")[1] is True

    run_migration(db, migrate)

    assert db.execute("SELECT COUNT(*) FROM app.category_source_map").fetchone() == (0,)
