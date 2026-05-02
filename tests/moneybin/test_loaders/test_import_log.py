"""Tests for the generic import_log module."""

import json

import pytest

from moneybin.database import Database
from moneybin.loaders import import_log


class TestBeginImport:
    """begin_import creates a 'importing' status row and returns a UUID."""

    def test_returns_uuid_string(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/test.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking", "savings"],
        )
        assert len(import_id) == 36
        assert import_id.count("-") == 4

    def test_writes_pending_row(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/test.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        row = db.execute(
            "SELECT source_file, source_type, source_origin, status, account_names "
            "FROM raw.import_log WHERE import_id = ?",
            [import_id],
        ).fetchone()
        assert row is not None
        assert row[0] == "/tmp/test.ofx"  # noqa: S108  # test fixture path in assertion
        assert row[1] == "ofx"
        assert row[2] == "wells_fargo"
        assert row[3] == "importing"
        assert json.loads(row[4]) == ["checking"]


class TestFinalizeImport:
    """finalize_import updates status, counts, and completed_at."""

    def test_marks_complete(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/test.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            db,
            import_id,
            status="complete",
            rows_total=100,
            rows_imported=100,
        )
        row = db.execute(
            "SELECT status, rows_imported, completed_at "
            "FROM raw.import_log WHERE import_id = ?",
            [import_id],
        ).fetchone()
        assert row is not None
        assert row[0] == "complete"
        assert row[1] == 100
        assert row[2] is not None


class TestRevertImport:
    """revert_import deletes from the right tables for the import's source_type."""

    def test_returns_not_found_for_missing_id(self, db: Database) -> None:
        result = import_log.revert_import(db, "00000000-0000-0000-0000-000000000000")
        assert result["status"] == "not_found"

    def test_reverts_ofx_batch(self, db: Database) -> None:
        """Verifies revert clears all four raw.ofx_* tables, not just transactions."""
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/test.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        # 1 transaction
        db.execute(
            """
            INSERT INTO raw.ofx_transactions (
                source_transaction_id, account_id, transaction_type, date_posted,
                amount, payee, memo, check_number, source_file, extracted_at,
                import_id, source_type, source_origin
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "FITID001",
                "checking",
                "DEBIT",
                "2026-01-15",
                "-50.00",
                "Coffee",
                None,
                None,
                "/tmp/test.ofx",  # noqa: S108  # test fixture path
                "2026-01-15 10:00:00",
                import_id,
                "ofx",
                "wells_fargo",
            ],
        )
        # 1 account
        db.execute(
            """
            INSERT INTO raw.ofx_accounts (
                account_id, routing_number, account_type, institution_org,
                institution_fid, source_file, extracted_at, import_id, source_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "checking",
                "12345",
                "CHECKING",
                "Wells Fargo",
                "3000",
                "/tmp/test.ofx",  # noqa: S108  # test fixture path
                "2026-01-15 10:00:00",
                import_id,
                "ofx",
            ],
        )
        # 1 balance
        db.execute(
            """
            INSERT INTO raw.ofx_balances (
                account_id, statement_start_date, statement_end_date,
                ledger_balance, ledger_balance_date, available_balance,
                source_file, extracted_at, import_id, source_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "checking",
                "2026-01-01",
                "2026-01-31",
                "1000.00",
                "2026-01-31 23:59:59",
                "950.00",
                "/tmp/test.ofx",  # noqa: S108  # test fixture path
                "2026-01-15 10:00:00",
                import_id,
                "ofx",
            ],
        )
        # 1 institution
        db.execute(
            """
            INSERT INTO raw.ofx_institutions (
                organization, fid, source_file, extracted_at, import_id, source_type
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                "Wells Fargo",
                "3000",
                "/tmp/test.ofx",  # noqa: S108  # test fixture path
                "2026-01-15 10:00:00",
                import_id,
                "ofx",
            ],
        )
        import_log.finalize_import(
            db, import_id, status="complete", rows_total=4, rows_imported=4
        )

        result = import_log.revert_import(db, import_id)
        assert result["status"] == "reverted"
        assert result["rows_deleted"] == 4

        # All four raw.ofx_* tables must be empty for this import_id.
        for table in (
            "raw.ofx_transactions",
            "raw.ofx_accounts",
            "raw.ofx_balances",
            "raw.ofx_institutions",
        ):
            count_row = db.execute(
                f"SELECT COUNT(*) FROM {table} WHERE import_id = ?",  # noqa: S608  # test input string from a closed set
                [import_id],
            ).fetchone()
            assert count_row is not None
            assert count_row[0] == 0, f"{table} not emptied after revert"

    def test_already_reverted_returns_status(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/test.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            db, import_id, status="complete", rows_total=0, rows_imported=0
        )
        import_log.revert_import(db, import_id)
        result = import_log.revert_import(db, import_id)
        assert result["status"] == "already_reverted"


class TestFindExistingImport:
    """find_existing_import detects prior imports of the same source_file."""

    def test_returns_none_for_new_file(self, db: Database) -> None:
        result = import_log.find_existing_import(db, "/tmp/never_imported.ofx")  # noqa: S108  # test fixture path
        assert result is None

    def test_returns_import_id_and_status_for_imported_file(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/once.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            db, import_id, status="complete", rows_total=1, rows_imported=1
        )
        result = import_log.find_existing_import(db, "/tmp/once.ofx")  # noqa: S108  # test fixture path
        assert result == (import_id, "complete")

    def test_returns_importing_status_for_in_progress_batch(self, db: Database) -> None:
        """A crashed/in-progress batch is detectable so callers can craft a clear error."""
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/in_progress.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        # Don't finalize — simulate a crash mid-import.
        result = import_log.find_existing_import(db, "/tmp/in_progress.ofx")  # noqa: S108  # test fixture path
        assert result == (import_id, "importing")

    def test_skips_reverted_imports(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/reverted.ofx",  # noqa: S108  # test fixture path
            source_type="ofx",
            source_origin="wells_fargo",
            account_names=["checking"],
        )
        import_log.finalize_import(
            db, import_id, status="complete", rows_total=0, rows_imported=0
        )
        import_log.revert_import(db, import_id)
        result = import_log.find_existing_import(db, "/tmp/reverted.ofx")  # noqa: S108  # test fixture path
        assert result is None


class TestRevertImportTabular:
    """revert_import dispatches correctly to raw.tabular_* tables for tabular imports."""

    def test_reverts_csv_batch(self, db: Database) -> None:
        import_id = import_log.begin_import(
            db,
            source_file="/tmp/test.csv",  # noqa: S108  # test fixture path
            source_type="csv",
            source_origin="tiller",
            account_names=["checking"],
        )
        db.execute(
            """
            INSERT INTO raw.tabular_transactions (
                transaction_id, account_id, transaction_date, amount, description,
                source_file, source_type, source_origin, import_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "csv_abc123",
                "checking",
                "2026-01-15",
                "-50.00",
                "Coffee",
                "/tmp/test.csv",  # noqa: S108  # test fixture path
                "csv",
                "tiller",
                import_id,
            ],
        )
        import_log.finalize_import(
            db, import_id, status="complete", rows_total=1, rows_imported=1
        )

        result = import_log.revert_import(db, import_id)
        assert result["status"] == "reverted"
        assert result["rows_deleted"] == 1

        count_row = db.execute(
            "SELECT COUNT(*) FROM raw.tabular_transactions WHERE import_id = ?",
            [import_id],
        ).fetchone()
        assert count_row is not None
        assert count_row[0] == 0


class TestBeginImportValidatesSourceType:
    """begin_import raises ValueError for unrecognized source_type values."""

    def test_rejects_unknown_source_type(self, db: Database) -> None:
        with pytest.raises(ValueError, match="Unknown source_type"):
            import_log.begin_import(
                db,
                source_file="/tmp/x",  # noqa: S108  # test fixture path
                source_type="nope",  # type: ignore[arg-type]  # intentional: testing runtime validation
                source_origin="x",
                account_names=[],
            )
