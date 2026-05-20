"""Tests for matches CLI commands."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.transactions.matches import app

runner = CliRunner()


class TestMatchesRun:
    """Tests for the matches run command."""

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.run")
    def test_run_succeeds(
        self,
        mock_run: MagicMock,
        mock_get_db: MagicMock,
    ) -> None:
        from moneybin.matching.engine import MatchResult

        mock_get_db.return_value = MagicMock()
        mock_run.return_value = MatchResult(auto_merged=3, pending_review=1)

        result = runner.invoke(app, ["run", "--skip-transform"])
        assert result.exit_code == 0
        mock_run.assert_called_once_with(auto_accept_transfers=False)


class TestMatchesHistory:
    """Tests for the matches history command."""

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.get_log")
    def test_history_empty(self, mock_log: MagicMock, mock_get_db: MagicMock) -> None:
        mock_get_db.return_value = MagicMock()
        mock_log.return_value = []
        result = runner.invoke(app, ["history"])
        assert result.exit_code == 0


class TestMatchesUndo:
    """Tests for the matches undo command."""

    @patch("moneybin.cli.commands.transactions.matches.get_database")
    @patch("moneybin.services.matching_service.MatchingService.undo")
    def test_undo_calls_service(
        self, mock_undo: MagicMock, mock_get_db: MagicMock
    ) -> None:
        mock_get_db.return_value = MagicMock()
        result = runner.invoke(app, ["undo", "abc123", "--yes"])
        assert result.exit_code == 0
        mock_undo.assert_called_once()
