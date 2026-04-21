"""Tests for CLI restructure: removed, moved, and promoted commands."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


class TestRemovedCommands:
    """Removed commands should not exist."""

    def test_config_group_removed(self) -> None:
        """Config command group no longer exists."""
        result = runner.invoke(app, ["config", "show"])
        assert result.exit_code != 0

    def test_data_extract_removed(self) -> None:
        """Data extract subgroup no longer exists."""
        result = runner.invoke(app, ["data", "extract", "ofx", "test.ofx"])
        assert result.exit_code != 0

    def test_data_group_removed(self) -> None:
        """Data command group no longer exists."""
        result = runner.invoke(app, ["data", "--help"])
        assert result.exit_code != 0


class TestPromotedCommands:
    """Commands promoted from data subgroup to top-level."""

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_categorize_at_top_level(self, mock_profile: MagicMock) -> None:
        """Categorize is a top-level command group."""
        result = runner.invoke(app, ["categorize", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_transform_at_top_level(self, mock_profile: MagicMock) -> None:
        """Transform is a top-level command group."""
        result = runner.invoke(app, ["transform", "--help"])
        assert result.exit_code == 0
        assert "plan" in result.output
        assert "apply" in result.output

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_profile_at_top_level(self, mock_profile: MagicMock) -> None:
        """Profile is a top-level command group."""
        result = runner.invoke(app, ["profile", "--help"])
        assert result.exit_code == 0
        assert "create" in result.output
        assert "list" in result.output


class TestMovedCommands:
    """Commands moved between groups."""

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_db_ps_exists(self, mock_profile: MagicMock) -> None:
        """Db ps command exists."""
        result = runner.invoke(app, ["db", "ps", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_db_kill_exists(self, mock_profile: MagicMock) -> None:
        """Db kill command exists."""
        result = runner.invoke(app, ["db", "kill", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_mcp_show_removed(self, mock_profile: MagicMock) -> None:
        """Mcp show no longer exists."""
        result = runner.invoke(app, ["mcp", "show"])
        assert result.exit_code != 0

    @patch("moneybin.cli.main.ensure_default_profile", return_value="test")
    def test_mcp_kill_removed(self, mock_profile: MagicMock) -> None:
        """Mcp kill no longer exists."""
        result = runner.invoke(app, ["mcp", "kill"])
        assert result.exit_code != 0
