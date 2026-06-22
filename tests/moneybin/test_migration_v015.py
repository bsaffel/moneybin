"""V015: drop UNIQUE(category, subcategory) from app.user_categories.

Phase 2 of the category-text -> category_id FK migration. With V014's FK
columns in place across every consumer, ``(category, subcategory)`` text
is a display snapshot — uniqueness is enforced by ``category_id`` PK.
This migration drops the legacy UNIQUE constraint so duplicate text with
distinct IDs (future merge/split flows, separate users with the same
custom label) is permitted at the DB layer. Service-layer code
(MatchApplier.create_category) retains the duplicate-text check for the
cases that should remain disallowed today.

Populated-fixture pattern per ``.claude/rules/database.md``: V015 touches
existing data (DROP CONSTRAINT via table rebuild), so the test seeds
existing user_categories rows and verifies (a) the constraint was
actually present pre-migration, (b) duplicate-text inserts succeed after
migration, (c) the migration is idempotent.
"""

from __future__ import annotations

import logging

import duckdb
import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V015__relax_user_categories_text_unique import migrate
from tests.moneybin.migration_helpers import run_migration

pytestmark = pytest.mark.fresh_db


@pytest.fixture()
def v015_db(db: Database) -> Database:
    """Database with three existing user_categories rows under the legacy schema.

    Realistic shapes per `.claude/rules/database.md` >=3-row rule. The
    fresh-install DDL already matches the post-V015 shape, so the fixture
    drops and recreates ``app.user_categories`` with the V014-era
    ``UNIQUE (category, subcategory)`` constraint — that is the state
    V015 must migrate away from.
    """
    db.execute("DROP TABLE app.user_categories")
    db.execute(
        """
        CREATE TABLE app.user_categories (
            category_id VARCHAR PRIMARY KEY,
            category VARCHAR NOT NULL,
            subcategory VARCHAR,
            description VARCHAR,
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (category, subcategory)
        )
        """
    )
    db.execute(
        "INSERT INTO app.user_categories "
        "(category_id, category, subcategory, is_active) "
        "VALUES ('USR0000001A', 'Childcare', 'Daycare', true), "
        "       ('USR0000002B', 'Childcare', NULL, true), "
        "       ('USR0000003C', 'Groceries', NULL, true)"
    )
    return db


@pytest.mark.unit
class TestV015:
    """V015 drops the (category, subcategory) UNIQUE constraint."""

    def test_constraint_present_before_migration(self, v015_db: Database) -> None:
        """Sanity check: the UNIQUE constraint actually exists pre-migration.

        Without this, the post-migration assertions could pass against an
        already-relaxed schema and the test would be meaningless.
        """
        with pytest.raises(duckdb.ConstraintException):
            v015_db.execute(
                "INSERT INTO app.user_categories "
                "(category_id, category, subcategory, is_active) "
                "VALUES ('USR0000099X', 'Childcare', 'Daycare', true)"
            )

    def test_duplicate_text_with_distinct_ids_allowed_after_migration(
        self, v015_db: Database
    ) -> None:
        run_migration(v015_db, migrate)

        # Duplicate of an existing (category, subcategory) pair but with a
        # distinct category_id — must succeed post-migration.
        v015_db.execute(
            "INSERT INTO app.user_categories "
            "(category_id, category, subcategory, is_active) "
            "VALUES ('USR0000004D', 'Childcare', 'Daycare', true)"
        )
        # Duplicate top-level (NULL subcategory).
        v015_db.execute(
            "INSERT INTO app.user_categories "
            "(category_id, category, subcategory, is_active) "
            "VALUES ('USR0000005E', 'Childcare', NULL, true)"
        )

        row = v015_db.execute(
            "SELECT COUNT(*) FROM app.user_categories WHERE category = 'Childcare'"
        ).fetchone()
        assert row == (4,)

    def test_existing_rows_preserved(self, v015_db: Database) -> None:
        run_migration(v015_db, migrate)
        rows = v015_db.execute(
            "SELECT category_id, category, subcategory FROM app.user_categories "
            "ORDER BY category_id"
        ).fetchall()
        assert rows == [
            ("USR0000001A", "Childcare", "Daycare"),
            ("USR0000002B", "Childcare", None),
            ("USR0000003C", "Groceries", None),
        ]

    def test_primary_key_still_enforced(self, v015_db: Database) -> None:
        """category_id PK is the surviving uniqueness contract."""
        run_migration(v015_db, migrate)
        with pytest.raises(duckdb.ConstraintException):
            v015_db.execute(
                "INSERT INTO app.user_categories "
                "(category_id, category, subcategory, is_active) "
                "VALUES ('USR0000001A', 'Different', 'Text', true)"
            )

    def test_idempotent(
        self, v015_db: Database, caplog: pytest.LogCaptureFixture
    ) -> None:
        run_migration(v015_db, migrate)
        # Second call must hit the early-return path, not rebuild again.
        migration_logger = (
            "moneybin.sql.migrations.V015__relax_user_categories_text_unique"
        )
        with caplog.at_level(logging.INFO, logger=migration_logger):
            run_migration(v015_db, migrate)
        assert any(
            "skipping rebuild" in record.message
            for record in caplog.records
            if record.name == migration_logger
        ), "second migrate() call should log the no-op early-return"
        v015_db.execute(
            "INSERT INTO app.user_categories "
            "(category_id, category, subcategory, is_active) "
            "VALUES ('USR0000006F', 'Childcare', NULL, true)"
        )
        row = v015_db.execute(
            "SELECT COUNT(*) FROM app.user_categories WHERE category = 'Childcare'"
        ).fetchone()
        assert row == (3,)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
