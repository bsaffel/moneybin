"""Tests for matching integration in import flow."""

from pathlib import Path
from unittest.mock import MagicMock, patch


class TestImportMatchingIntegration:
    """Tests that matching is hooked into the import pipeline."""

    @patch("moneybin.services.import_service.run_transforms")
    @patch("moneybin.services.import_service._run_matching")
    @patch("moneybin.services.import_service._apply_categorization")
    @patch("moneybin.services.import_service._import_ofx")
    @patch("moneybin.services.import_service._detect_file_type", return_value="ofx")
    def test_matching_runs_before_transforms(
        self,
        mock_detect: MagicMock,
        mock_import_ofx: MagicMock,
        mock_categorize: MagicMock,
        mock_matching: MagicMock,
        mock_transforms: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify _run_matching is called before run_transforms during import."""
        from moneybin.services.import_service import ImportResult, import_file

        qfx = tmp_path / "test.qfx"
        qfx.touch()
        mock_import_ofx.return_value = ImportResult(
            file_path=str(qfx), file_type="ofx", transactions=3, accounts=1
        )
        mock_transforms.return_value = True

        db = MagicMock()
        db.path = tmp_path / "test.duckdb"
        import_file(db, qfx, apply_transforms=True)

        mock_matching.assert_called_once_with(db)
        mock_transforms.assert_called_once()

    @patch("moneybin.services.import_service.run_transforms")
    @patch("moneybin.services.import_service._run_matching")
    @patch("moneybin.services.import_service._apply_categorization")
    @patch("moneybin.services.import_service._import_ofx")
    @patch("moneybin.services.import_service._detect_file_type", return_value="ofx")
    def test_apply_transforms_false_skips_matching(
        self,
        mock_detect: MagicMock,
        mock_import_ofx: MagicMock,
        mock_categorize: MagicMock,
        mock_matching: MagicMock,
        mock_transforms: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify matching is skipped when apply_transforms=False."""
        from moneybin.services.import_service import ImportResult, import_file

        qfx = tmp_path / "test.qfx"
        qfx.touch()
        mock_import_ofx.return_value = ImportResult(
            file_path=str(qfx), file_type="ofx", transactions=3, accounts=1
        )

        db = MagicMock()
        db.path = tmp_path / "test.duckdb"
        import_file(db, qfx, apply_transforms=False)

        mock_matching.assert_not_called()
        mock_transforms.assert_not_called()

    @patch("moneybin.services.import_service.run_transforms")
    @patch("moneybin.services.import_service._run_matching")
    @patch("moneybin.services.import_service._apply_categorization")
    @patch("moneybin.services.import_service._import_ofx")
    @patch("moneybin.services.import_service._detect_file_type", return_value="ofx")
    def test_matching_failure_does_not_abort_import(
        self,
        mock_detect: MagicMock,
        mock_import_ofx: MagicMock,
        mock_categorize: MagicMock,
        mock_matching: MagicMock,
        mock_transforms: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify matching errors are swallowed (best-effort)."""
        from moneybin.services.import_service import ImportResult, import_file

        qfx = tmp_path / "test.qfx"
        qfx.touch()
        mock_import_ofx.return_value = ImportResult(
            file_path=str(qfx), file_type="ofx", transactions=3, accounts=1
        )
        mock_matching.side_effect = RuntimeError("views don't exist yet")
        mock_transforms.return_value = True

        db = MagicMock()
        db.path = tmp_path / "test.duckdb"
        result = import_file(db, qfx, apply_transforms=True)

        # Import should succeed despite matching failure
        assert result.transactions == 3
        mock_transforms.assert_called_once()
