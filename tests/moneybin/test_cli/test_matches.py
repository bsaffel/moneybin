"""Tests for matches CLI commands."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.matches import app

runner = CliRunner()


class TestMatchesRun:
    """Tests for the matches run command."""

    @patch("moneybin.cli.commands.matches.get_database")
    @patch("moneybin.cli.commands.matches.TransactionMatcher")
    @patch("moneybin.matching.priority.seed_source_priority")
    @patch("moneybin.config.get_settings")
    def test_run_succeeds(
        self,
        mock_get_settings: MagicMock,
        mock_seed: MagicMock,
        mock_matcher_cls: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        from moneybin.matching.engine import MatchResult

        mock_db = MagicMock()
        mock_get_db.return_value = mock_db
        mock_settings = MagicMock()
        mock_get_settings.return_value = mock_settings
        mock_matcher = MagicMock()
        mock_matcher.run.return_value = MatchResult(auto_merged=3, pending_review=1)
        mock_matcher_cls.return_value = mock_matcher

        result = runner.invoke(app, ["run", "--skip-transform"])
        assert result.exit_code == 0
        mock_matcher.run.assert_called_once()


class TestMatchesHistory:
    """Tests for the matches history command."""

    @patch("moneybin.cli.commands.matches.get_database")
    @patch("moneybin.cli.commands.matches.get_match_log")
    def test_history_empty(self, mock_log: MagicMock, mock_get_db: MagicMock) -> None:
        mock_get_db.return_value = MagicMock()
        mock_log.return_value = []
        result = runner.invoke(app, ["history"])
        assert result.exit_code == 0


class TestMatchesUndo:
    """Tests for the matches undo command."""

    @patch("moneybin.cli.commands.matches.get_database")
    @patch("moneybin.cli.commands.matches.undo_match")
    def test_undo_calls_persistence(
        self, mock_undo: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value = MagicMock()
        result = runner.invoke(app, ["undo", "abc123", "--yes"])
        assert result.exit_code == 0
        mock_undo.assert_called_once()
