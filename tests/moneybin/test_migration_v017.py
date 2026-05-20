"""V017: add deleted_from_source_at to raw.tabular_transactions.

Pure additive ADD COLUMN with no DEFAULT — fixture seeds three realistic
rows so a hypothetical backfill bug would surface, but the migration is
expected to leave them NULL (currently-present-in-source semantic).

Populated-fixture pattern per ``.claude/rules/database.md``: even pure
additive DDL is tested against ≥3 rows so the assertion "existing rows
are unchanged" has real data behind it.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.sql.migrations.V017__add_deleted_from_source_at_to_tabular import (
    migrate,
)
from tests.moneybin.migration_helpers import column_exists, run_migration

CSV_TXN_ID = "csv_aaaa11112222bbbb"
TSV_TXN_ID = "tsv_cccc33334444dddd"
EXCEL_TXN_ID = "xls_eeee55556666ffff"


@pytest.fixture()
def v017_db(db: Database) -> Database:
    """Three realistic raw.tabular_transactions rows from different sources."""
    rows = [
        (
            CSV_TXN_ID,
            "acct-chase-001",
            "2025-11-01",
            Decimal("-42.50"),
            "STARBUCKS #1234",
            "/imports/chase.csv",
            "csv",
            "chase_credit",
            "imp-001",
        ),
        (
            TSV_TXN_ID,
            "acct-amex-002",
            "2025-11-05",
            Decimal("-128.00"),
            "AMAZON.COM",
            "/imports/amex.tsv",
            "tsv",
            "amex_blue",
            "imp-002",
        ),
        (
            EXCEL_TXN_ID,
            "acct-fido-003",
            "2025-11-10",
            Decimal("2500.00"),
            "PAYROLL DEPOSIT",
            "/imports/fidelity.xlsx",
            "excel",
            "fidelity_brokerage",
            "imp-003",
        ),
    ]
    for txn_id, acct, date, amount, desc, src_file, src_type, src_origin, imp in rows:
        db.execute(
            """
            INSERT INTO raw.tabular_transactions
                (transaction_id, account_id, transaction_date, amount,
                 description, source_file, source_type, source_origin, import_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [txn_id, acct, date, amount, desc, src_file, src_type, src_origin, imp],
        )
    return db


class TestV017AddDeletedFromSourceAt:
    """V017 adds deleted_from_source_at to raw.tabular_transactions."""

    def test_column_exists_after_migration(self, v017_db: Database) -> None:
        run_migration(v017_db, migrate)
        assert column_exists(
            v017_db, "raw", "tabular_transactions", "deleted_from_source_at"
        )

    def test_column_is_nullable_timestamp(self, v017_db: Database) -> None:
        run_migration(v017_db, migrate)
        row = v017_db.execute(
            "SELECT data_type, is_nullable FROM duckdb_columns() "
            "WHERE schema_name = 'raw' "
            "AND table_name = 'tabular_transactions' "
            "AND column_name = 'deleted_from_source_at'"
        ).fetchone()
        assert row is not None
        data_type, is_nullable = row
        assert data_type == "TIMESTAMP"
        assert bool(is_nullable) is True

    def test_existing_rows_get_null(self, v017_db: Database) -> None:
        """Pre-existing rows stay NULL — semantic = 'present in source'."""
        run_migration(v017_db, migrate)
        rows = v017_db.execute(
            "SELECT deleted_from_source_at FROM raw.tabular_transactions "
            "ORDER BY transaction_id"
        ).fetchall()
        assert len(rows) == 3
        assert all(r[0] is None for r in rows)

    def test_column_accepts_timestamp_writes(self, v017_db: Database) -> None:
        """Writers can stamp the column on the soft-delete diff path."""
        run_migration(v017_db, migrate)
        v017_db.execute(
            "UPDATE raw.tabular_transactions "
            "SET deleted_from_source_at = TIMESTAMP '2025-11-20 12:00:00' "
            "WHERE transaction_id = ?",
            [CSV_TXN_ID],
        )
        row = v017_db.execute(
            "SELECT deleted_from_source_at FROM raw.tabular_transactions "
            "WHERE transaction_id = ?",
            [CSV_TXN_ID],
        ).fetchone()
        assert row is not None
        assert row[0] is not None

    def test_idempotent(self, v017_db: Database) -> None:
        """Re-running the migration on an already-migrated DB is harmless."""
        run_migration(v017_db, migrate)
        run_migration(v017_db, migrate)
        assert column_exists(
            v017_db, "raw", "tabular_transactions", "deleted_from_source_at"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
