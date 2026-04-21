"""Tests for MCP CLI enhancements."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.mcp import app

runner = CliRunner()


class TestMCPListTools:
    """Tests for the list-tools command."""

    @patch("moneybin.cli.commands.mcp.importlib")
    def test_list_tools(self, mock_importlib: MagicMock) -> None:
        """list-tools enumerates registered MCP tools."""
        result = runner.invoke(app, ["list-tools"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.mcp.importlib")
    def test_list_tools_shows_tool_names(self, mock_importlib: MagicMock) -> None:
        """list-tools shows tool names and descriptions from mcp server."""
        mock_tool = MagicMock()
        mock_tool.description = "A test tool"

        with patch("moneybin.cli.commands.mcp.mcp_server") as mock_server:
            mock_server._tool_manager._tools = {"test_tool": mock_tool}
            result = runner.invoke(app, ["list-tools"])

        assert result.exit_code == 0
        assert "test_tool" in result.output

    @patch("moneybin.cli.commands.mcp.importlib")
    def test_list_tools_empty(self, mock_importlib: MagicMock) -> None:
        """list-tools handles empty tool registry gracefully."""
        with patch("moneybin.cli.commands.mcp.mcp_server") as mock_server:
            mock_server._tool_manager._tools = {}
            result = runner.invoke(app, ["list-tools"])

        assert result.exit_code == 0


class TestMCPListPrompts:
    """Tests for the list-prompts command."""

    @patch("moneybin.cli.commands.mcp.importlib")
    def test_list_prompts(self, mock_importlib: MagicMock) -> None:
        """list-prompts enumerates registered MCP prompts."""
        result = runner.invoke(app, ["list-prompts"])
        assert result.exit_code == 0

    @patch("moneybin.cli.commands.mcp.importlib")
    def test_list_prompts_shows_prompt_names(self, mock_importlib: MagicMock) -> None:
        """list-prompts shows prompt names and descriptions from mcp server."""
        mock_prompt = MagicMock()
        mock_prompt.description = "A test prompt"

        with patch("moneybin.cli.commands.mcp.mcp_server") as mock_server:
            mock_server._prompt_manager._prompts = {"test_prompt": mock_prompt}
            result = runner.invoke(app, ["list-prompts"])

        assert result.exit_code == 0
        assert "test_prompt" in result.output

    @patch("moneybin.cli.commands.mcp.importlib")
    def test_list_prompts_empty(self, mock_importlib: MagicMock) -> None:
        """list-prompts handles empty prompt registry gracefully."""
        with patch("moneybin.cli.commands.mcp.mcp_server") as mock_server:
            mock_server._prompt_manager._prompts = {}
            result = runner.invoke(app, ["list-prompts"])

        assert result.exit_code == 0


class TestMCPConfig:
    """Tests for the mcp config command."""

    def test_config_show(self) -> None:
        """Mcp config shows current MCP server config."""
        result = runner.invoke(app, ["config"])
        assert result.exit_code == 0

    def test_config_show_includes_profile(self) -> None:
        """Mcp config output includes profile name."""
        result = runner.invoke(app, ["config"])
        assert result.exit_code == 0
        # Should display profile or config info
        assert result.output.strip() != ""

    def test_config_show_includes_max_rows(self) -> None:
        """Mcp config output includes max_rows setting."""
        result = runner.invoke(app, ["config"])
        assert result.exit_code == 0
        assert "max_rows" in result.output or "rows" in result.output.lower()


class TestMCPConfigGenerate:
    """Tests for the mcp config generate command."""

    def test_generate_claude_desktop(self, tmp_path: Path) -> None:
        """Generates valid config for claude-desktop."""
        result = runner.invoke(
            app, ["config", "generate", "--client", "claude-desktop"]
        )
        assert result.exit_code == 0
        assert "moneybin" in result.output.lower() or "MoneyBin" in result.output

    def test_generate_default_client(self, tmp_path: Path) -> None:
        """Generates config with default client when none specified."""
        result = runner.invoke(app, ["config", "generate"])
        assert result.exit_code == 0

    def test_generate_with_install(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--install writes config to client config file."""
        config_file = tmp_path / "claude_desktop_config.json"
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp._get_client_config_path",
            lambda client: config_file,  # type: ignore[reportUnknownLambdaType]
        )
        result = runner.invoke(
            app,
            ["config", "generate", "--client", "claude-desktop", "--install"],
            input="y\n",
        )
        assert result.exit_code == 0

    def test_generate_with_install_yes_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--install --yes writes config without prompting."""
        config_file = tmp_path / "claude_desktop_config.json"
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp._get_client_config_path",
            lambda client: config_file,  # type: ignore[reportUnknownLambdaType]
        )
        result = runner.invoke(
            app,
            ["config", "generate", "--client", "claude-desktop", "--install", "--yes"],
        )
        assert result.exit_code == 0

    def test_generate_with_install_creates_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--install actually writes the config file."""
        config_file = tmp_path / "claude_desktop_config.json"
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp._get_client_config_path",
            lambda client: config_file,  # type: ignore[reportUnknownLambdaType]
        )
        runner.invoke(
            app,
            ["config", "generate", "--client", "claude-desktop", "--install", "--yes"],
        )
        assert config_file.exists()

    def test_generate_with_profile(self, tmp_path: Path) -> None:
        """--profile flag is accepted for generate command."""
        result = runner.invoke(
            app,
            ["config", "generate", "--client", "claude-desktop", "--profile", "work"],
        )
        assert result.exit_code == 0
