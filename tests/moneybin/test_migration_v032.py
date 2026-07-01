"""V032: add source_type to app.transaction_categories.

Splits the origin aggregator out of categorized_by (which becomes
method-only). Populated-fixture pattern per ``.claude/rules/database.md``:
V032 touches existing data (``ADD COLUMN`` + backfill ``UPDATE`` + ``SET NOT
NULL``), so the test seeds existing rows and verifies (a) the column lands
NOT NULL with every existing row backfilled to 'internal', (b) the migration
is idempotent and the NOT NULL tightening survives replay.
"""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V032__add_transaction_categories_source_type import (
    migrate,
)
from tests.moneybin.migration_helpers import column_exists, column_info, run_migration

pytestmark = pytest.mark.fresh_db

_PRE_V032_DDL = """
    CREATE TABLE app.transaction_categories (
        transaction_id VARCHAR PRIMARY KEY,
        category VARCHAR NOT NULL,
        subcategory VARCHAR,
        category_id VARCHAR,
        categorized_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        categorized_by VARCHAR DEFAULT 'ai',
        merchant_id VARCHAR,
        confidence DECIMAL(3, 2),
        rule_id VARCHAR
    )
"""


@pytest.fixture()
def v032_db(db: Database) -> Database:
    """Database with three existing transaction_categories rows under the pre-V032 shape.

    Realistic shapes per the >=3-row rule: mixed ``categorized_by`` methods
    (rule, ai, user) so the backfill is proven across more than one row shape.
    """
    db.execute("DROP TABLE app.transaction_categories")
    db.execute(_PRE_V032_DDL)
    db.execute(
        "INSERT INTO app.transaction_categories "
        "(transaction_id, category, subcategory, categorized_by, confidence, rule_id) "
        "VALUES "
        "('txn_pre_001', 'Food & Drink', 'Coffee', 'rule', NULL, 'rul-abc123'), "
        "('txn_pre_002', 'Shopping', NULL, 'ai', 0.82, NULL), "
        "('txn_pre_003', 'Groceries', NULL, 'user', NULL, NULL)"
    )
    return db


@pytest.mark.unit
class TestV032:
    """V032 adds source_type and backfills existing rows to 'internal'."""

    def test_column_absent_before_migration(self, v032_db: Database) -> None:
        """Sanity check: source_type doesn't exist pre-migration."""
        assert not column_exists(
            v032_db, "app", "transaction_categories", "source_type"
        )

    def test_adds_source_type_column(self, v032_db: Database) -> None:
        run_migration(v032_db, migrate)
        assert column_exists(v032_db, "app", "transaction_categories", "source_type")
        _data_type, is_nullable = column_info(
            v032_db, "app", "transaction_categories", "source_type"
        )
        assert is_nullable is False

    def test_backfills_existing_rows_to_internal(self, v032_db: Database) -> None:
        run_migration(v032_db, migrate)
        rows = v032_db.execute(
            "SELECT transaction_id, source_type FROM app.transaction_categories "
            "ORDER BY transaction_id"
        ).fetchall()
        assert rows == [
            ("txn_pre_001", "internal"),
            ("txn_pre_002", "internal"),
            ("txn_pre_003", "internal"),
        ]

    def test_new_rows_default_to_internal(self, v032_db: Database) -> None:
        """Fresh inserts after the migration pick up the column's DEFAULT."""
        run_migration(v032_db, migrate)
        v032_db.execute(
            "INSERT INTO app.transaction_categories "
            "(transaction_id, category, categorized_by) "
            "VALUES ('txn_post_001', 'Utilities', 'user')"
        )
        row = v032_db.execute(
            "SELECT source_type FROM app.transaction_categories "
            "WHERE transaction_id = 'txn_post_001'"
        ).fetchone()
        assert row == ("internal",)

    def test_idempotent(self, v032_db: Database) -> None:
        run_migration(v032_db, migrate)
        # Second call must not error and must not disturb existing rows.
        run_migration(v032_db, migrate)
        rows = v032_db.execute(
            "SELECT transaction_id, source_type FROM app.transaction_categories "
            "ORDER BY transaction_id"
        ).fetchall()
        assert rows == [
            ("txn_pre_001", "internal"),
            ("txn_pre_002", "internal"),
            ("txn_pre_003", "internal"),
        ]
        # NOT NULL tightening must survive replay, not just the first run.
        _data_type, is_nullable = column_info(
            v032_db, "app", "transaction_categories", "source_type"
        )
        assert is_nullable is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
