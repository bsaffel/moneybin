"""Tests for V003: add import_id/source_type/source_origin to raw.ofx_* tables."""

from moneybin.database import Database
from moneybin.sql.migrations.V003__ofx_import_batch_columns import migrate


class TestV003Migration:
    """V003 migration adds import_id/source_type/source_origin to raw.ofx_* tables."""

    def test_adds_columns_to_ofx_transactions(self, db: Database) -> None:
        # Drop the new columns to simulate pre-migration state.
        for col in ("import_id", "source_type", "source_origin"):
            try:
                db.execute(f"ALTER TABLE raw.ofx_transactions DROP COLUMN {col}")  # noqa: S608  # test input, not executing SQL
            except Exception:  # noqa: BLE001, S110  # DuckDB raises untyped errors on missing columns
                pass  # column already absent

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        cols = {
            row[0]
            for row in db.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'raw' AND table_name = 'ofx_transactions'"
            ).fetchall()
        }
        assert "import_id" in cols
        assert "source_type" in cols
        assert "source_origin" in cols

    def test_idempotent_on_second_run(self, db: Database) -> None:
        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]
        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]  # should not raise

    def test_backfills_source_type_for_existing_rows(self, db: Database) -> None:
        # Drop columns to simulate pre-migration state, then insert a legacy row.
        for col in ("import_id", "source_type", "source_origin"):
            try:
                db.execute(f"ALTER TABLE raw.ofx_transactions DROP COLUMN {col}")  # noqa: S608  # test input, not executing SQL
            except Exception:  # noqa: BLE001, S110  # DuckDB raises untyped errors on missing columns
                pass
        db.execute(
            """
            INSERT INTO raw.ofx_transactions (
                source_transaction_id, account_id, transaction_type, date_posted,
                amount, payee, memo, check_number, source_file, extracted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "LEGACY1",
                "checking",
                "DEBIT",
                "2025-12-01",
                "-10.00",
                "Test",
                None,
                None,
                "/tmp/legacy.ofx",  # noqa: S108
                "2025-12-01 12:00:00",
            ],
        )

        migrate(db._conn)  # pyright: ignore[reportPrivateUsage]

        row = db.execute(
            "SELECT source_type, import_id FROM raw.ofx_transactions "
            "WHERE source_transaction_id = 'LEGACY1'"
        ).fetchone()
        assert row is not None
        assert row[0] == "ofx"
        assert row[1] is None  # legacy rows have NULL import_id, by design
