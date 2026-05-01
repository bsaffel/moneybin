"""Tests for the tabular loader (Stage 5)."""

import json
from unittest.mock import MagicMock

import pytest

from moneybin.loaders.tabular_loader import TabularLoader


@pytest.fixture()
def mock_db() -> MagicMock:
    """Mock Database instance for unit tests."""
    db = MagicMock()
    db.execute.return_value.fetchone.return_value = None
    db.execute.return_value.fetchall.return_value = []
    return db


class TestCreateImportBatch:
    """Tests for import batch creation."""

    def test_creates_import_log_entry(self, mock_db: MagicMock) -> None:
        loader = TabularLoader(mock_db)
        import_id = loader.create_import_batch(
            source_file="/tmp/test.csv",  # noqa: S108  # test fixture path, not real temp file
            source_type="csv",
            source_origin="test_bank",
            account_names=["Test Checking"],
        )
        assert import_id  # Non-empty UUID
        assert len(import_id) == 36  # UUID format
        assert mock_db.execute.called

    def test_import_id_is_unique(self, mock_db: MagicMock) -> None:
        loader = TabularLoader(mock_db)
        id1 = loader.create_import_batch(
            source_file="/tmp/test1.csv",  # noqa: S108  # test fixture path, not real temp file
            source_type="csv",
            source_origin="test_bank",
            account_names=["Test"],
        )
        id2 = loader.create_import_batch(
            source_file="/tmp/test2.csv",  # noqa: S108  # test fixture path, not real temp file
            source_type="csv",
            source_origin="test_bank",
            account_names=["Test"],
        )
        assert id1 != id2


class TestFinalizeImportBatch:
    """Tests for import batch finalization."""

    def test_finalize_sets_partial_status(self, mock_db: MagicMock) -> None:
        loader = TabularLoader(mock_db)
        loader.finalize_import_batch(
            import_id="test-123",
            rows_total=100,
            rows_imported=95,
            rows_rejected=5,
            rows_skipped_trailing=2,
            detection_confidence="high",
            number_format="us",
            date_format="%m/%d/%Y",
            sign_convention="negative_is_expense",
            balance_validated=True,
        )
        params = mock_db.execute.call_args[0][1]
        assert params[0] == "partial"  # rows_rejected=5 → "partial"

    def test_finalize_sets_complete_status(self, mock_db: MagicMock) -> None:
        loader = TabularLoader(mock_db)
        loader.finalize_import_batch(
            import_id="test-123",
            rows_total=100,
            rows_imported=100,
            rows_rejected=0,
        )
        params = mock_db.execute.call_args[0][1]
        assert params[0] == "complete"

    def test_finalize_sets_failed_status(self, mock_db: MagicMock) -> None:
        loader = TabularLoader(mock_db)
        loader.finalize_import_batch(
            import_id="test-123",
            rows_total=10,
            rows_imported=0,
            rows_rejected=10,
        )
        params = mock_db.execute.call_args[0][1]
        assert params[0] == "failed"

    def test_finalize_persists_rejection_details(self, mock_db: MagicMock) -> None:
        loader = TabularLoader(mock_db)
        loader.finalize_import_batch(
            import_id="test-123",
            rows_total=10,
            rows_imported=8,
            rows_rejected=2,
            rejection_details=[
                {"row_number": "3", "reason": "Unparseable date: '32/01/2024'"},
                {"row_number": "7", "reason": "Unparseable amount: 'n/a'"},
            ],
        )
        call_args = mock_db.execute.call_args
        params = call_args[0][1]
        # rejection_details is the 6th parameter (index 5)
        rejection_json = params[5]
        parsed = json.loads(rejection_json)
        assert len(parsed) == 2
        assert parsed[0]["row_number"] == "3"
        assert parsed[1]["reason"] == "Unparseable amount: 'n/a'"


class TestRevertImport:
    """Tests for import reverting."""

    def test_revert_deletes_rows(self, mock_db: MagicMock) -> None:
        call_count = 0

        def side_effect(*_args: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            m = MagicMock()
            if call_count == 1:
                # import_log lookup — (source_type, status, source_file, started_at)
                m.fetchone.return_value = (
                    "csv",
                    "complete",
                    "/tmp/f.csv",  # noqa: S108  # mock value, not a real temp file
                    "2024-01-01",
                )
            elif call_count == 2:
                # COUNT(*) raw.tabular_transactions = 3
                m.fetchone.return_value = (3,)
            elif call_count == 3:
                # COUNT(*) raw.tabular_accounts = 2 — total 5 across both tables
                m.fetchone.return_value = (2,)
            return m

        mock_db.execute.side_effect = side_effect
        loader = TabularLoader(mock_db)
        result = loader.revert_import("test-123")
        assert result["status"] == "reverted"
        assert result["rows_deleted"] == 5

    def test_revert_already_reverted(self, mock_db: MagicMock) -> None:
        mock_db.execute.return_value.fetchone.return_value = (
            "csv",
            "reverted",
            "/tmp/f.csv",  # noqa: S108  # mock value, not a real temp file
            "2024-01-01",
        )
        loader = TabularLoader(mock_db)
        result = loader.revert_import("test-123")
        assert result["status"] == "already_reverted"

    def test_revert_not_found(self, mock_db: MagicMock) -> None:
        mock_db.execute.return_value.fetchone.return_value = None
        loader = TabularLoader(mock_db)
        result = loader.revert_import("nonexistent")
        assert result["status"] == "not_found"

    def test_revert_superseded_by_reimport(self, mock_db: MagicMock) -> None:
        """When a file was re-imported, reverting the old batch returns superseded."""
        # First call: import_log lookup returns a valid row
        # Second call: COUNT(*) returns 0 (no rows with old import_id)
        # Third call: re-import check finds a newer import
        call_count = 0

        def side_effect(*_args: object) -> MagicMock:
            nonlocal call_count
            call_count += 1
            result = MagicMock()
            if call_count == 1:
                # import_log lookup — (source_type, status, source_file, started_at)
                result.fetchone.return_value = (
                    "csv",
                    "complete",
                    "/tmp/f.csv",  # noqa: S108  # mock value, not a real temp file
                    "2024-01-01",
                )
            elif call_count == 2:
                # COUNT(*) raw.tabular_transactions — no rows
                result.fetchone.return_value = (0,)
            elif call_count == 3:
                # COUNT(*) raw.tabular_accounts — no rows
                result.fetchone.return_value = (0,)
            elif call_count == 4:
                # re-import check — found a newer import
                result.fetchone.return_value = ("new-import-id-1234",)
            return result

        mock_db.execute.side_effect = side_effect
        loader = TabularLoader(mock_db)
        result = loader.revert_import("old-import-id")
        assert result["status"] == "superseded"
        assert "re-imported" in str(result.get("reason", ""))
