"""Tests for V032: add app.category_source_map and app.user_categories.class.

V032 lays the schema foundation for the category-source bridge (M1V): a
provider category-code -> canonical MoneyBin category mapping table, plus an
accounting `class` column on the user-category dimension. Fresh installs get
both from the schema DDL; existing installs get them via this migration.

DuckDB rejects both `ADD COLUMN ... NOT NULL` and `ADD CONSTRAINT CHECK` in a
single ALTER, so `app.user_categories` is rebuilt wholesale: snapshot to a
tmp table, DROP, CREATE with the full target shape (`class NOT NULL` + its
CHECK baked in), then re-INSERT via an explicit column list. Per
`.claude/rules/database.md`, the test drives `migrate()` through the shared
`run_migration()` helper to reproduce the runner's enclosing BEGIN/COMMIT
transaction rather than calling `migrate(db._conn)` bare — the whole rebuild
must run inside that single transaction.
"""

from __future__ import annotations

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V032__add_category_source_map_and_class import migrate
from tests.moneybin.migration_helpers import column_exists, column_info, run_migration

pytestmark = pytest.mark.fresh_db

_BRIDGE_COLUMNS = {
    "source_type",
    "source_category_code",
    "code_level",
    "category_id",
    "source_taxonomy_version",
    "created_at",
    "updated_at",
}


def _table_exists(db: Database, schema: str, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = ? AND table_name = ?",
        [schema, table],
    ).fetchone()
    return row is not None


def _recreate_pre_v032_state(db: Database) -> None:
    """Reverse the V032 end-state: drop the bridge table and the `class` columns.

    On a fresh DB, `init_schemas` already creates `app.category_source_map`
    and `app.user_categories.class` (this task's own schema edits), so to
    exercise the migration meaningfully we roll the DB back to its pre-V032
    shape first. `class` sits after the `category_id` PK column, so a plain
    `ALTER TABLE ... DROP COLUMN` is allowed (unlike V030's case where the
    new column preceded a PK-indexed column).

    `seeds.categories.class` needs no DROP here: `_ensure_seed_tables_exist`
    bootstraps the table in frozen V014's original shape (`plaid_detailed`,
    no `class`) rather than the post-V032 shape, so the table is already in
    its pre-V032 state — the test only seeds representative rows (one per
    class-rule prefix) to exercise the prefix-derived backfill in migrate().
    """
    db.execute("DROP TABLE IF EXISTS app.category_source_map")
    db.execute("ALTER TABLE app.user_categories DROP COLUMN class")
    db.execute(
        "INSERT INTO app.user_categories "
        "(category_id, category, subcategory, description, is_active, "
        "created_at, updated_at) VALUES "
        "('u_a1b2c3d4e5f6', 'Side Gig', 'Consulting', '', true, "
        "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP), "
        "('u_b2c3d4e5f6a1', 'Hobby', 'Models', '', true, "
        "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP), "
        "('u_c3d4e5f6a1b2', 'Gifts', 'Given', '', false, "
        "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
    )
    db.execute(
        "INSERT INTO seeds.categories "
        "(category_id, category, subcategory, description) VALUES "
        "('INC-TST', 'Income', 'Test', ''), "
        "('TRN-TST', 'Transfer', 'Test', ''), "
        "('LNP-TST', 'Loan Payments', 'Test', ''), "
        "('FND-TST', 'Food & Drink', 'Test', '')"
    )


def _seed_category_classes(db: Database) -> dict[str, str]:
    return dict(
        db.execute(
            "SELECT category_id, class FROM seeds.categories "
            "WHERE category_id LIKE '%-TST' ORDER BY category_id"
        ).fetchall()
    )


def test_v032_creates_bridge_and_class(db: Database) -> None:
    _recreate_pre_v032_state(db)
    assert not _table_exists(db, "app", "category_source_map")
    assert not column_exists(db, "app", "user_categories", "class")
    assert not column_exists(db, "seeds", "categories", "class")

    run_migration(db, migrate)

    # class backfilled to 'expense' on existing rows, NOT NULL.
    classes = [
        row[0]
        for row in db.execute(
            "SELECT class FROM app.user_categories ORDER BY category_id"
        ).fetchall()
    ]
    assert classes == ["expense", "expense", "expense"]
    _, is_nullable = column_info(db, "app", "user_categories", "class")
    assert is_nullable is False

    # bridge table exists with the expected columns.
    cols = {
        row[1]
        for row in db.execute("PRAGMA table_info('app.category_source_map')").fetchall()
    }
    assert _BRIDGE_COLUMNS <= cols

    # seeds.categories.class backfilled by category_id prefix (BLOCK B rule).
    assert _seed_category_classes(db) == {
        "INC-TST": "income",
        "TRN-TST": "transfer",
        "LNP-TST": "debt",
        "FND-TST": "expense",
    }


def test_v032_is_idempotent(db: Database) -> None:
    _recreate_pre_v032_state(db)

    run_migration(db, migrate)
    run_migration(db, migrate)

    classes = [
        row[0]
        for row in db.execute(
            "SELECT class FROM app.user_categories ORDER BY category_id"
        ).fetchall()
    ]
    assert classes == ["expense", "expense", "expense"]
    _, is_nullable = column_info(db, "app", "user_categories", "class")
    assert is_nullable is False
    cols = {
        row[1]
        for row in db.execute("PRAGMA table_info('app.category_source_map')").fetchall()
    }
    assert _BRIDGE_COLUMNS <= cols
    assert _seed_category_classes(db) == {
        "INC-TST": "income",
        "TRN-TST": "transfer",
        "LNP-TST": "debt",
        "FND-TST": "expense",
    }


def test_v032_idempotent_on_fresh_install(db: Database) -> None:
    """On a fresh DB where init_schemas already produced the end-state, migrate() is a no-op."""
    # No _recreate_pre_v032_state call — db comes from init_schemas with the
    # final shape (bridge table + NOT NULL class already present).
    run_migration(db, migrate)

    assert _table_exists(db, "app", "category_source_map")
    _, is_nullable = column_info(db, "app", "user_categories", "class")
    assert is_nullable is False
    assert column_exists(db, "seeds", "categories", "class")


def test_v032_class_check_constraint_enforced(db: Database) -> None:
    """The rebuilt app.user_categories rejects a class outside the four values."""
    _recreate_pre_v032_state(db)
    run_migration(db, migrate)

    with pytest.raises(duckdb.ConstraintException):
        db.execute(
            "INSERT INTO app.user_categories (category_id, category, class) "
            "VALUES (?, ?, ?)",
            ["u_bogus0000001", "BogusClass", "bogus"],
        )


def test_v032_code_level_check_constraint_enforced(db: Database) -> None:
    """app.category_source_map rejects a code_level outside detailed/primary."""
    _recreate_pre_v032_state(db)
    run_migration(db, migrate)

    with pytest.raises(duckdb.ConstraintException):
        db.execute(
            "INSERT INTO app.category_source_map "
            "(source_type, source_category_code, code_level, category_id) "
            "VALUES (?, ?, ?, ?)",
            ["plaid", "FOOD_AND_DRINK", "bogus_level", "FND-TST"],
        )


def test_v032_idempotent_run_preserves_class_check(db: Database) -> None:
    """A second migrate() call must not fail, and the CHECK must survive it."""
    _recreate_pre_v032_state(db)
    run_migration(db, migrate)
    run_migration(db, migrate)

    with pytest.raises(duckdb.ConstraintException):
        db.execute(
            "INSERT INTO app.user_categories (category_id, category, class) "
            "VALUES (?, ?, ?)",
            ["u_bogus0000002", "BogusClass2", "bogus"],
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
