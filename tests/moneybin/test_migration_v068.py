"""V068: allow app.category_source_map.category_id to be NULL."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V068__category_source_map_category_id_nullable import (
    migrate,
)
from tests.moneybin.migration_helpers import column_info, run_migration

# Pre-V068 shape (post-V067): category_id NOT NULL. Rebuilt explicitly rather
# than relying on the fixture template, which already carries this PR's
# schema — mirrors V067's test precedent for widening a shipped shape.
_PRE_V068_CATEGORY_SOURCE_MAP_SQL = """
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

_SEED = """
INSERT INTO app.category_source_map
    (source_type, source_category_code, source_subcategory_code,
     code_level, category_id, source_taxonomy_version)
VALUES
    ('plaid', 'FOOD_AND_DRINK_COFFEE', '', 'detailed', 'FND-COF', 'plaid_pfc_v2'),
    ('plaid', 'TRANSPORTATION', '', 'primary', 'TRP', 'plaid_pfc_v2'),
    ('chase_credit', 'Groceries', '', 'detailed', 'cat-groceries', NULL)
"""


@pytest.fixture
def pre_v068_db(db: Database) -> Database:
    """A populated table still carrying category_id NOT NULL."""
    db.execute("DROP TABLE app.category_source_map")
    db.execute(_PRE_V068_CATEGORY_SOURCE_MAP_SQL)
    db.execute(_SEED)
    _, is_nullable = column_info(db, "app", "category_source_map", "category_id")
    assert is_nullable is False, "fixture must start with category_id NOT NULL"
    return db


@pytest.mark.unit
def test_migrate_drops_the_not_null_constraint(pre_v068_db: Database) -> None:
    """After V068 the column accepts NULL."""
    run_migration(pre_v068_db, migrate)

    _, is_nullable = column_info(
        pre_v068_db, "app", "category_source_map", "category_id"
    )
    assert is_nullable is True


@pytest.mark.unit
def test_migrate_leaves_every_existing_row_untouched(pre_v068_db: Database) -> None:
    """No backfill: every pre-existing category_id keeps its value."""
    run_migration(pre_v068_db, migrate)

    rows = pre_v068_db.execute(
        "SELECT source_type, source_category_code, category_id "
        "FROM app.category_source_map ORDER BY source_type, source_category_code"
    ).fetchall()
    assert rows == [
        ("chase_credit", "Groceries", "cat-groceries"),
        ("plaid", "FOOD_AND_DRINK_COFFEE", "FND-COF"),
        ("plaid", "TRANSPORTATION", "TRP"),
    ]


@pytest.mark.unit
def test_after_migrate_a_new_row_can_carry_null_category_id(
    pre_v068_db: Database,
) -> None:
    """The point of the migration: a NULL category_id row can now be written."""
    run_migration(pre_v068_db, migrate)

    pre_v068_db.execute(
        "INSERT INTO app.category_source_map "
        "(source_type, source_category_code, source_subcategory_code, "
        " code_level, category_id) "
        "VALUES ('chase_credit', 'Misc', '', 'detailed', NULL)"
    )
    row = pre_v068_db.execute(
        "SELECT category_id FROM app.category_source_map "
        "WHERE source_type = 'chase_credit' AND source_category_code = 'Misc'"
    ).fetchone()
    assert row is not None
    assert row[0] is None


@pytest.mark.unit
def test_migrate_is_idempotent(pre_v068_db: Database) -> None:
    """Re-running against an already-migrated table is a no-op, not an error."""
    run_migration(pre_v068_db, migrate)
    run_migration(pre_v068_db, migrate)

    _, is_nullable = column_info(
        pre_v068_db, "app", "category_source_map", "category_id"
    )
    assert is_nullable is True
