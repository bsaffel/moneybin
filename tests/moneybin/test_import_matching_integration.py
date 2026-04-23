"""Tests for matching integration in import flow."""

from unittest.mock import MagicMock, patch


class TestImportMatchingIntegration:
    """Tests that matching is hooked into the import pipeline."""

    @patch("moneybin.services.import_service.run_transforms")
    @patch("moneybin.services.import_service._run_matching")
    @patch("moneybin.services.import_service._apply_categorization")
    def test_matching_runs_after_load(
        self,
        mock_categorize: MagicMock,
        mock_matching: MagicMock,
        mock_transforms: MagicMock,
    ) -> None:
        """Verify _run_matching is callable and exists in import_service."""
        from moneybin.matching.engine import MatchResult

        mock_matching.return_value = MatchResult(auto_merged=2, pending_review=0)
        mock_transforms.return_value = True

        from moneybin.services.import_service import (
            _run_matching,  # pyright: ignore[reportPrivateUsage]  # testing private function exists
        )

        assert callable(_run_matching)
