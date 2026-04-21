"""Tests for transform CLI commands."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.transform import app

runner = CliRunner()


class TestTransformStatus:
    """Test transform status command."""

    @patch("moneybin.cli.commands.transform.Context")
    def test_status_succeeds(self, mock_ctx_cls: MagicMock) -> None:
        """Transform status calls SQLMesh info."""
        mock_ctx = mock_ctx_cls.return_value
        mock_ctx.state_reader.get_environment.return_value = None
        result = runner.invoke(app, ["status"])
        assert result.exit_code == 0


class TestTransformValidate:
    """Test transform validate command."""

    @patch("moneybin.cli.commands.transform.Context")
    def test_validate_succeeds(self, mock_ctx_cls: MagicMock) -> None:
        """Transform validate runs plan in dry-run mode."""
        mock_ctx = mock_ctx_cls.return_value
        result = runner.invoke(app, ["validate"])
        assert result.exit_code == 0
        mock_ctx.plan.assert_called_once()


class TestTransformAudit:
    """Test transform audit command."""

    @patch("moneybin.cli.commands.transform.Context")
    def test_audit_succeeds(self, mock_ctx_cls: MagicMock) -> None:
        """Transform audit runs SQLMesh audit."""
        mock_ctx = mock_ctx_cls.return_value
        result = runner.invoke(
            app, ["audit", "--start", "2026-01-01", "--end", "2026-01-31"]
        )
        assert result.exit_code == 0
        mock_ctx.audit.assert_called_once()


class TestTransformRestate:
    """Test transform restate command."""

    @patch("moneybin.cli.commands.transform.Context")
    def test_restate_requires_confirmation(self, mock_ctx_cls: MagicMock) -> None:
        """Transform restate prompts for confirmation."""
        mock_ctx = mock_ctx_cls.return_value
        result = runner.invoke(
            app,
            ["restate", "--model", "core.fct_transactions", "--start", "2026-01-01"],
            input="n\n",
        )
        assert result.exit_code == 0
        mock_ctx.plan.assert_not_called()

    @patch("moneybin.cli.commands.transform.Context")
    def test_restate_with_yes(self, mock_ctx_cls: MagicMock) -> None:
        """Transform restate --yes skips confirmation."""
        result = runner.invoke(
            app,
            [
                "restate",
                "--model",
                "core.fct_transactions",
                "--start",
                "2026-01-01",
                "--yes",
            ],
        )
        assert result.exit_code == 0
