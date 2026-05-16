"""Tests for refresh-pipeline integration in the import flow."""

from pathlib import Path
from unittest.mock import MagicMock, patch


class TestImportRefreshIntegration:
    """Tests that the refresh pipeline is hooked into the import flow."""

    @patch("moneybin.services.import_service._refresh")
    @patch("moneybin.services.import_service.ImportService._import_ofx")
    @patch("moneybin.services.import_service._detect_file_type", return_value="ofx")
    def test_refresh_runs_after_load(
        self,
        mock_detect: MagicMock,
        mock_import_ofx: MagicMock,
        mock_refresh: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify refresh() is called once after a successful import."""
        from moneybin.services.import_service import ImportResult, ImportService
        from moneybin.services.refresh import RefreshResult

        qfx = tmp_path / "test.qfx"
        qfx.touch()
        mock_import_ofx.return_value = ImportResult(
            file_path=str(qfx), file_type="ofx", transactions=3, accounts=1
        )
        mock_refresh.return_value = RefreshResult(applied=True, duration_seconds=0.0)

        db = MagicMock()
        db.path = tmp_path / "test.duckdb"
        ImportService(db).import_file(qfx, refresh=True)

        mock_refresh.assert_called_once_with(db)

    @patch("moneybin.services.import_service._refresh")
    @patch("moneybin.services.import_service.ImportService._import_ofx")
    @patch("moneybin.services.import_service._detect_file_type", return_value="ofx")
    def test_refresh_false_skips_pipeline(
        self,
        mock_detect: MagicMock,
        mock_import_ofx: MagicMock,
        mock_refresh: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Verify the refresh pipeline is skipped when refresh=False."""
        from moneybin.services.import_service import ImportResult, ImportService

        qfx = tmp_path / "test.qfx"
        qfx.touch()
        mock_import_ofx.return_value = ImportResult(
            file_path=str(qfx), file_type="ofx", transactions=3, accounts=1
        )

        db = MagicMock()
        db.path = tmp_path / "test.duckdb"
        ImportService(db).import_file(qfx, refresh=False)

        mock_refresh.assert_not_called()

    @patch("moneybin.services.import_service._refresh")
    @patch("moneybin.services.import_service.ImportService._import_ofx")
    @patch("moneybin.services.import_service._detect_file_type", return_value="ofx")
    def test_refresh_failure_raises(
        self,
        mock_detect: MagicMock,
        mock_import_ofx: MagicMock,
        mock_refresh: MagicMock,
        tmp_path: Path,
    ) -> None:
        """Single-file import path is fail-loud: SQLMesh error propagates."""
        import pytest

        from moneybin.services.import_service import ImportResult, ImportService
        from moneybin.services.refresh import RefreshResult

        qfx = tmp_path / "test.qfx"
        qfx.touch()
        mock_import_ofx.return_value = ImportResult(
            file_path=str(qfx), file_type="ofx", transactions=3, accounts=1
        )
        mock_refresh.return_value = RefreshResult(
            applied=False, duration_seconds=None, error="plan failed"
        )

        db = MagicMock()
        db.path = tmp_path / "test.duckdb"
        with pytest.raises(RuntimeError, match="SQLMesh transforms failed"):
            ImportService(db).import_file(qfx, refresh=True)


class TestRefreshCategorizationProposalSummary:
    """Tests that pending auto-rule proposals appear in the refresh log output."""

    @patch("moneybin.services.auto_rule_service.AutoRuleService")
    @patch("moneybin.services.categorization.CategorizationService")
    @patch("moneybin.services.transform_service.TransformService.apply")
    @patch("moneybin.matching.engine.TransactionMatcher")
    @patch("moneybin.matching.priority.seed_source_priority")
    def test_logs_proposal_count_when_pending(
        self,
        mock_seed: MagicMock,
        mock_matcher_cls: MagicMock,
        mock_apply: MagicMock,
        mock_cat_cls: MagicMock,
        mock_auto_cls: MagicMock,
        caplog: object,
    ) -> None:
        """Pending proposals trigger a hint line referencing auto-review."""
        import logging

        from moneybin.services.refresh import refresh
        from moneybin.services.transform_service import ApplyResult

        mock_apply.return_value = ApplyResult(applied=True, duration_seconds=0.0)
        cat = mock_cat_cls.return_value
        cat.categorize_pending.return_value = {
            "total": 5,
            "merchant": 3,
            "rule": 2,
        }
        from moneybin.services.auto_rule_service import AutoStatsResult

        mock_auto_cls.return_value.stats.return_value = AutoStatsResult(
            pending_proposals=4
        )

        with caplog.at_level(logging.INFO, logger="moneybin.services.refresh"):  # type: ignore[attr-defined]
            refresh(MagicMock())

        text = "\n".join(r.message for r in caplog.records)  # type: ignore[attr-defined]
        assert "4 new auto-rule proposals" in text
        assert "auto review" in text

    @patch("moneybin.services.auto_rule_service.AutoRuleService")
    @patch("moneybin.services.categorization.CategorizationService")
    @patch("moneybin.services.transform_service.TransformService.apply")
    @patch("moneybin.matching.engine.TransactionMatcher")
    @patch("moneybin.matching.priority.seed_source_priority")
    def test_no_proposal_line_when_zero(
        self,
        mock_seed: MagicMock,
        mock_matcher_cls: MagicMock,
        mock_apply: MagicMock,
        mock_cat_cls: MagicMock,
        mock_auto_cls: MagicMock,
        caplog: object,
    ) -> None:
        """No pending proposals → no hint line."""
        import logging

        from moneybin.services.refresh import refresh
        from moneybin.services.transform_service import ApplyResult

        mock_apply.return_value = ApplyResult(applied=True, duration_seconds=0.0)
        cat = mock_cat_cls.return_value
        cat.categorize_pending.return_value = {
            "total": 1,
            "merchant": 1,
            "rule": 0,
        }
        from moneybin.services.auto_rule_service import AutoStatsResult

        mock_auto_cls.return_value.stats.return_value = AutoStatsResult(
            pending_proposals=0
        )

        with caplog.at_level(logging.INFO, logger="moneybin.services.refresh"):  # type: ignore[attr-defined]
            refresh(MagicMock())

        text = "\n".join(r.message for r in caplog.records)  # type: ignore[attr-defined]
        assert "auto-rule proposals" not in text
        assert "auto review" not in text
