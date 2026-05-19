"""Tests for the generic import_log module."""

import json

import pytest

from moneybin.database import Database
from moneybin.loaders import import_log
from moneybin.services.import_service import ImportService


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
        # Revert lives on the service, not the loader; use it as setup so the
        # real assertion (find_existing_import skips reverted batches) stays
        # focused on this module's behavior.
        ImportService(db).revert(import_id)
        result = import_log.find_existing_import(db, "/tmp/reverted.ofx")  # noqa: S108  # test fixture path
        assert result is None


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
