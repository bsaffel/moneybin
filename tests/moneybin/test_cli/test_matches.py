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


class TestMatchesReview:
    """Tests for the matches review command."""

    @patch("moneybin.cli.commands.matches._run_transforms_after_match_change")
    @patch("moneybin.cli.commands.matches.get_database")
    @patch("moneybin.matching.persistence.update_match_status")
    def test_accept_single_runs_transform(
        self,
        mock_update: MagicMock,
        mock_get_db: MagicMock,
        mock_run_transforms: MagicMock,
    ) -> None:
        mock_get_db.return_value = MagicMock()

        result = runner.invoke(
            app,
            ["review", "--match-id", "abc123", "--decision", "accept"],
        )

        assert result.exit_code == 0
        mock_update.assert_called_once_with(
            mock_get_db.return_value, "abc123", status="accepted", decided_by="user"
        )
        mock_run_transforms.assert_called_once()

    @patch("moneybin.cli.commands.matches._run_transforms_after_match_change")
    @patch("moneybin.cli.commands.matches.get_database")
    @patch("moneybin.matching.persistence.update_match_status")
    def test_reject_single_does_not_run_transform(
        self,
        mock_update: MagicMock,
        mock_get_db: MagicMock,
        mock_run_transforms: MagicMock,
    ) -> None:
        mock_get_db.return_value = MagicMock()

        result = runner.invoke(
            app,
            ["review", "--match-id", "abc123", "--decision", "reject"],
        )

        assert result.exit_code == 0
        mock_update.assert_called_once_with(
            mock_get_db.return_value, "abc123", status="rejected", decided_by="user"
        )
        mock_run_transforms.assert_not_called()

    @patch("moneybin.cli.commands.matches._run_transforms_after_match_change")
    @patch("moneybin.cli.commands.matches.get_database")
    @patch("moneybin.matching.persistence.get_pending_matches")
    @patch("moneybin.matching.persistence.update_match_status")
    def test_accept_all_runs_transform_once(
        self,
        mock_update: MagicMock,
        mock_pending: MagicMock,
        mock_get_db: MagicMock,
        mock_run_transforms: MagicMock,
    ) -> None:
        mock_get_db.return_value = MagicMock()
        mock_pending.return_value = [
            {"match_id": "abc123"},
            {"match_id": "def456"},
        ]

        result = runner.invoke(app, ["review", "--accept-all"])

        assert result.exit_code == 0
        assert mock_update.call_count == 2
        mock_run_transforms.assert_called_once()

    @patch("moneybin.cli.commands.matches._run_transforms_after_match_change")
    @patch("moneybin.cli.commands.matches.get_database")
    @patch("moneybin.matching.persistence.update_match_status")
    def test_accept_single_can_skip_transform(
        self,
        mock_update: MagicMock,
        mock_get_db: MagicMock,
        mock_run_transforms: MagicMock,
    ) -> None:
        mock_get_db.return_value = MagicMock()

        result = runner.invoke(
            app,
            [
                "review",
                "--match-id",
                "abc123",
                "--decision",
                "accept",
                "--skip-transform",
            ],
        )

        assert result.exit_code == 0
        mock_update.assert_called_once()
        mock_run_transforms.assert_not_called()


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
