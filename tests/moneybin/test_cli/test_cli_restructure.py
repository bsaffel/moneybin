"""Tests for CLI restructure: removed, moved, and promoted commands."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _isolated_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    """Create a temporary 'test' profile directory and point get_base_dir at it.

    Uses tmp_path so the directory is automatically cleaned up after the test
    and doesn't interfere with the real profiles/ directory.
    """
    monkeypatch.delenv("MONEYBIN_PROFILE", raising=False)
    profile_dir = tmp_path / "profiles" / "test"
    profile_dir.mkdir(parents=True)
    monkeypatch.setattr("moneybin.config.get_base_dir", lambda: tmp_path)


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

    def test_top_level_categorize_removed(self) -> None:
        """Top-level categorize group no longer exists (moved under transactions)."""
        result = runner.invoke(app, ["categorize", "--help"])
        assert result.exit_code != 0


class TestPromotedCommands:
    """Commands promoted from data subgroup to top-level."""

    @patch("moneybin.cli.utils.ensure_default_profile", return_value="test")
    def test_transactions_categorize_exists(self, mock_profile: MagicMock) -> None:
        """Categorize workflow is under transactions categorize."""
        result = runner.invoke(app, ["transactions", "categorize", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.utils.ensure_default_profile", return_value="test")
    def test_categories_at_top_level(self, mock_profile: MagicMock) -> None:
        """Categories is a top-level command group."""
        result = runner.invoke(app, ["categories", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.utils.ensure_default_profile", return_value="test")
    def test_merchants_at_top_level(self, mock_profile: MagicMock) -> None:
        """Merchants is a top-level command group."""
        result = runner.invoke(app, ["merchants", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.utils.ensure_default_profile", return_value="test")
    def test_transform_at_top_level(self, mock_profile: MagicMock) -> None:
        """Transform is a top-level command group."""
        result = runner.invoke(app, ["transform", "--help"])
        assert result.exit_code == 0
        assert "plan" in result.output
        assert "apply" in result.output

    @patch("moneybin.cli.utils.ensure_default_profile", return_value="test")
    def test_profile_at_top_level(self, mock_profile: MagicMock) -> None:
        """Profile is a top-level command group."""
        result = runner.invoke(app, ["profile", "--help"])
        assert result.exit_code == 0
        assert "create" in result.output
        assert "list" in result.output


class TestMovedCommands:
    """Commands moved between groups."""

    @patch("moneybin.cli.utils.ensure_default_profile", return_value="test")
    def test_db_ps_exists(self, mock_profile: MagicMock) -> None:
        """Db ps command exists."""
        result = runner.invoke(app, ["db", "ps", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.utils.ensure_default_profile", return_value="test")
    def test_db_kill_exists(self, mock_profile: MagicMock) -> None:
        """Db kill command exists."""
        result = runner.invoke(app, ["db", "kill", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.utils.ensure_default_profile", return_value="test")
    def test_mcp_show_removed(self, mock_profile: MagicMock) -> None:
        """Mcp show no longer exists."""
        result = runner.invoke(app, ["mcp", "show"])
        assert result.exit_code != 0

    @patch("moneybin.cli.utils.ensure_default_profile", return_value="test")
    def test_mcp_kill_removed(self, mock_profile: MagicMock) -> None:
        """Mcp kill no longer exists."""
        result = runner.invoke(app, ["mcp", "kill"])
        assert result.exit_code != 0


class TestStubbedCommands:
    """Stubbed commands show 'not implemented' messages."""

    @patch("moneybin.cli.utils.ensure_default_profile", return_value="test")
    def test_matches_stubbed(self, mock_profile: MagicMock) -> None:
        """Matches group exists under transactions."""
        result = runner.invoke(app, ["transactions", "matches", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.utils.ensure_default_profile", return_value="test")
    def test_track_stubbed(self, mock_profile: MagicMock) -> None:
        """Track group exists but shows not-implemented."""
        result = runner.invoke(app, ["track", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.utils.ensure_default_profile", return_value="test")
    def test_export_stubbed(self, mock_profile: MagicMock) -> None:
        """Export group exists."""
        result = runner.invoke(app, ["export", "--help"])
        assert result.exit_code == 0

    @patch("moneybin.cli.utils.ensure_default_profile", return_value="test")
    def test_stats_stubbed(self, mock_profile: MagicMock) -> None:
        """Stats command exists."""
        result = runner.invoke(app, ["stats", "--help"])
        assert result.exit_code == 0
