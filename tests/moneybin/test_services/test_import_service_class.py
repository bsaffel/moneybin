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
