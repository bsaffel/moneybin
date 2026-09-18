"""Tests for the ImportService class shape."""

from unittest.mock import MagicMock

from moneybin.database import Database
from moneybin.services.import_service import ImportService


class TestImportServiceShape:
    """Verify ImportService matches the AccountService/CategorizationService pattern."""

    def test_constructor_accepts_database(self) -> None:
        db = MagicMock(spec=Database)
        service = ImportService(db)
        assert service is not None

    def test_exposes_import_file_method(self) -> None:
        db = MagicMock(spec=Database)
        service = ImportService(db)
        assert callable(service.import_file)

    def test_exposes_run_transforms_method(self) -> None:
        db = MagicMock(spec=Database)
        service = ImportService(db)
        assert callable(service.run_transforms)


class TestFinalizeTabularBatch:
    """Status-derivation logic relocated from TabularExtractor.finalize_import_batch (MB-248)."""

    def test_zero_rows_imported_is_failed(self) -> None:
        """A zero-imported outcome must never report 'complete'."""
        db = MagicMock(spec=Database)
        service = ImportService(db)
        service._finalize_tabular_batch(  # pyright: ignore[reportPrivateUsage]
            "imp1", rows_total=10, rows_imported=0, rows_rejected=10
        )
        params = db.execute.call_args[0][1]
        assert params[0] == "failed"

    def test_no_rejections_is_complete(self) -> None:
        db = MagicMock(spec=Database)
        service = ImportService(db)
        service._finalize_tabular_batch(  # pyright: ignore[reportPrivateUsage]
            "imp1", rows_total=100, rows_imported=100, rows_rejected=0
        )
        params = db.execute.call_args[0][1]
        assert params[0] == "complete"

    def test_some_rejections_is_partial(self) -> None:
        db = MagicMock(spec=Database)
        service = ImportService(db)
        service._finalize_tabular_batch(  # pyright: ignore[reportPrivateUsage]
            "imp1", rows_total=100, rows_imported=95, rows_rejected=5
        )
        params = db.execute.call_args[0][1]
        assert params[0] == "partial"
