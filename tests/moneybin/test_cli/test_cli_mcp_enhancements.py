"""Tests for MCP CLI enhancements."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.mcp import app

runner = CliRunner()


class TestMCPListTools:
    """Tests for the list-tools command."""

    def test_list_tools(self) -> None:
        """list-tools enumerates registered MCP tools."""

        async def fake_get_tools() -> list[MagicMock]:
            tool = MagicMock()
            tool.name = "test.tool"
            tool.description = "A test tool"
            return [tool]

        with (
            patch("moneybin.mcp.server.init_db"),
            patch("moneybin.mcp.server.mcp._list_tools", new=fake_get_tools),
        ):
            result = runner.invoke(app, ["list-tools"])
        assert result.exit_code == 0
        assert "test.tool" in result.output

    def test_list_tools_shows_tool_names(self) -> None:
        """list-tools shows tool names and descriptions."""

        async def fake_get_tools() -> list[MagicMock]:
            tool = MagicMock()
            tool.name = "spending_summary"
            tool.description = "Monthly spending"
            return [tool]

        with (
            patch("moneybin.mcp.server.init_db"),
            patch("moneybin.mcp.server.mcp._list_tools", new=fake_get_tools),
        ):
            result = runner.invoke(app, ["list-tools"])

        assert result.exit_code == 0
        assert "spending_summary" in result.output
        assert "Monthly spending" in result.output

    def test_list_tools_empty(self) -> None:
        """list-tools handles empty tool registry gracefully."""

        async def fake_get_tools() -> list[MagicMock]:
            return []

        with (
            patch("moneybin.mcp.server.init_db"),
            patch("moneybin.mcp.server.mcp._list_tools", new=fake_get_tools),
        ):
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

        async def fake_list_prompts(*, run_middleware: bool = True) -> list[object]:
            mock_prompt = MagicMock()
            mock_prompt.name = "test_prompt"
            mock_prompt.description = "A test prompt"
            return [mock_prompt]

        with patch("moneybin.mcp.server.mcp") as mock_server:
            mock_server.list_prompts = fake_list_prompts
            result = runner.invoke(app, ["list-prompts"])

        assert result.exit_code == 0
        assert "test_prompt" in result.output

    @patch("moneybin.cli.commands.mcp.importlib")
    def test_list_prompts_empty(self, mock_importlib: MagicMock) -> None:
        """list-prompts handles empty prompt registry gracefully."""

        async def fake_list_prompts(*, run_middleware: bool = True) -> list[object]:
            return []

        with patch("moneybin.mcp.server.mcp") as mock_server:
            mock_server.list_prompts = fake_list_prompts
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


class TestMCPInstall:
    """Tests for the mcp install command."""

    def test_install_print_claude_desktop(self, tmp_path: Path) -> None:
        """--print emits a valid snippet for claude-desktop without writing."""
        result = runner.invoke(
            app, ["install", "--client", "claude-desktop", "--print"]
        )
        assert result.exit_code == 0
        assert "moneybin" in result.output.lower() or "MoneyBin" in result.output

    def test_install_print_default_client(self, tmp_path: Path) -> None:
        """--print works with the default client when none specified."""
        result = runner.invoke(app, ["install", "--print"])
        assert result.exit_code == 0

    def test_install_default_writes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Install (no --print) writes config to client config file after confirm."""
        config_file = tmp_path / "claude_desktop_config.json"
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp._get_client_config_path",
            lambda client: config_file,  # type: ignore[reportUnknownLambdaType]
        )
        result = runner.invoke(
            app,
            ["install", "--client", "claude-desktop"],
            input="y\n",
        )
        assert result.exit_code == 0

    def test_install_yes_skips_prompt(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--yes writes config without prompting."""
        config_file = tmp_path / "claude_desktop_config.json"
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp._get_client_config_path",
            lambda client: config_file,  # type: ignore[reportUnknownLambdaType]
        )
        result = runner.invoke(
            app,
            ["install", "--client", "claude-desktop", "--yes"],
        )
        assert result.exit_code == 0

    def test_install_creates_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Install actually writes the config file by default."""
        config_file = tmp_path / "claude_desktop_config.json"
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp._get_client_config_path",
            lambda client: config_file,  # type: ignore[reportUnknownLambdaType]
        )
        runner.invoke(
            app,
            ["install", "--client", "claude-desktop", "--yes"],
        )
        assert config_file.exists()

    def test_install_print_does_not_write(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--print emits the snippet but never touches the config file."""
        config_file = tmp_path / "claude_desktop_config.json"
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp._get_client_config_path",
            lambda client: config_file,  # type: ignore[reportUnknownLambdaType]
        )
        result = runner.invoke(
            app,
            ["install", "--client", "claude-desktop", "--print"],
        )
        assert result.exit_code == 0
        assert not config_file.exists()

    def test_install_with_profile(self, tmp_path: Path) -> None:
        """--profile flag is accepted for the install command."""
        result = runner.invoke(
            app,
            ["install", "--client", "claude-desktop", "--profile", "work", "--print"],
        )
        assert result.exit_code == 0

    def test_install_unknown_client_errors(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Unknown client → usage error (exit 2), matching `mcp config path`."""
        result = runner.invoke(app, ["install", "--client", "bogus", "--print"])
        assert result.exit_code == 2
        assert "Unknown client" in caplog.text

    def test_install_claude_code_print_emits_launch_hint(self, tmp_path: Path) -> None:
        """claude-code --print emits the snippet plus the `claude --mcp-config` launch line."""
        result = runner.invoke(app, ["install", "--client", "claude-code", "--print"])
        assert result.exit_code == 0
        assert "mcpServers" in result.output
        assert "--strict-mcp-config" in result.output
        assert "--mcp-config" in result.output
        assert "claude-code-mcp.json" in result.output

    def test_install_claude_code_writes_to_profile_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """claude-code install writes to <base>/profiles/<profile>/claude-code-mcp.json."""
        monkeypatch.setattr("moneybin.cli.commands.mcp.get_base_dir", lambda: tmp_path)
        result = runner.invoke(
            app,
            [
                "install",
                "--client",
                "claude-code",
                "--profile",
                "work",
                "--yes",
            ],
        )
        assert result.exit_code == 0
        expected = tmp_path / "profiles" / "work" / "claude-code-mcp.json"
        assert expected.exists()
        import json as _json

        payload = _json.loads(expected.read_text())
        assert "mcpServers" in payload

    def test_install_chatgpt_desktop_prints_instructions(self) -> None:
        """chatgpt-desktop emits the snippet plus Connector setup steps."""
        result = runner.invoke(app, ["install", "--client", "chatgpt-desktop"])
        assert result.exit_code == 0
        assert "mcpServers" in result.output
        assert "Connectors" in result.output
        assert "Command:" in result.output
        assert "Arguments:" in result.output

    def test_install_chatgpt_desktop_print_implicit(self) -> None:
        """chatgpt-desktop has no programmatic install — --print is implicit (no error)."""
        result = runner.invoke(
            app, ["install", "--client", "chatgpt-desktop", "--print"]
        )
        assert result.exit_code == 0
        assert "Connectors" in result.output

    def test_old_config_generate_command_removed(self) -> None:
        """The old `mcp config generate` command no longer exists."""
        result = runner.invoke(app, ["config", "generate", "--help"])
        assert result.exit_code != 0


class TestMCPConfigPath:
    """Tests for the `mcp config path` command."""

    def test_path_claude_code_under_profile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """claude-code path is <base>/profiles/<profile>/claude-code-mcp.json."""
        monkeypatch.setattr("moneybin.cli.commands.mcp.get_base_dir", lambda: tmp_path)
        result = runner.invoke(
            app, ["config", "path", "--client", "claude-code", "--profile", "alice"]
        )
        assert result.exit_code == 0
        assert (
            str(tmp_path / "profiles" / "alice" / "claude-code-mcp.json")
            in result.output
        )

    def test_path_chatgpt_desktop_exits_one(self) -> None:
        """chatgpt-desktop has no JSON config file — exit 1, no output."""
        result = runner.invoke(app, ["config", "path", "--client", "chatgpt-desktop"])
        assert result.exit_code == 1

    def test_path_unknown_client_exits_two(self) -> None:
        """Unknown client → usage error (exit 2)."""
        result = runner.invoke(app, ["config", "path", "--client", "bogus"])
        assert result.exit_code == 2

    def test_path_fixed_path_client_returns_canonical_location(self) -> None:
        """Fixed-path clients (e.g. cursor) resolve to their `_CLIENT_CONFIG_PATHS` entry."""
        result = runner.invoke(
            app, ["config", "path", "--client", "cursor", "--profile", "alice"]
        )
        assert result.exit_code == 0
        # Cursor's canonical install path is ~/.cursor/mcp.json — independent of profile.
        assert ".cursor" in result.output
        assert "mcp.json" in result.output

    def test_path_fixed_path_client_no_profile_required(self) -> None:
        """Fixed-path clients work without --profile and without an active profile."""
        result = runner.invoke(app, ["config", "path", "--client", "cursor"])
        assert result.exit_code == 0
        assert "mcp.json" in result.output

    def test_path_vscode_outside_repo_exits_one(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Vscode config path errors with a diagnostic when no repo root is found."""
        monkeypatch.setattr("moneybin.cli.commands.mcp.find_repo_root", lambda: None)
        result = runner.invoke(
            app, ["config", "path", "--client", "vscode", "--profile", "alice"]
        )
        assert result.exit_code == 1


class TestMCPInstallCodex:
    """Codex emits a TOML [mcp_servers] block and installs via tomlkit round-trip."""

    def test_install_codex_print_emits_toml_block(self) -> None:
        result = runner.invoke(
            app,
            ["install", "--client", "codex", "--profile", "alice", "--print"],
        )
        assert result.exit_code == 0
        assert "[mcp_servers." in result.output
        assert 'command = "uv"' in result.output
        assert "args = [" in result.output
        # No mcpServers JSON header — codex output is TOML, not JSON.
        assert "mcpServers" not in result.output

    def test_install_codex_writes_toml(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        target = tmp_path / "config.toml"
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp._get_client_config_path",
            lambda client: target,  # type: ignore[reportUnknownLambdaType]
        )
        result = runner.invoke(
            app,
            [
                "install",
                "--client",
                "codex",
                "--profile",
                "alice",
                "--yes",
            ],
        )
        assert result.exit_code == 0
        assert target.exists()

        import tomllib

        parsed = tomllib.loads(target.read_text())
        assert "mcp_servers" in parsed
        assert "MoneyBin (alice)" in parsed["mcp_servers"]
        entry = parsed["mcp_servers"]["MoneyBin (alice)"]
        assert entry["command"] == "uv"
        assert "moneybin" in entry["args"]
        # Concurrency guardrail must fire on per-invocation client installs.
        assert "auto-loads" in result.output

    def test_install_codex_cancelled_skips_warning(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Declining the install prompt suppresses the auto-load warning."""
        target = tmp_path / "config.toml"
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp._get_client_config_path",
            lambda client: target,  # type: ignore[reportUnknownLambdaType]
        )
        result = runner.invoke(
            app,
            [
                "install",
                "--client",
                "codex",
                "--profile",
                "alice",
            ],
            input="n\n",
        )
        assert result.exit_code == 0
        # User declined → file not written, warning suppressed.
        assert not target.exists()
        assert "auto-loads" not in result.output

    def test_install_codex_preserves_existing_keys_and_comments(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Round-trip merge keeps unrelated settings and inline comments intact."""
        target = tmp_path / "config.toml"
        target.write_text(
            "# user preference\n"
            'model = "gpt-5"  # default model\n'
            "\n"
            "[mcp_servers.other]\n"
            'command = "node"\n'
            'args = ["server.js"]\n'
        )
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp._get_client_config_path",
            lambda client: target,  # type: ignore[reportUnknownLambdaType]
        )
        result = runner.invoke(
            app,
            [
                "install",
                "--client",
                "codex",
                "--profile",
                "alice",
                "--yes",
            ],
        )
        assert result.exit_code == 0
        text = target.read_text()
        # Pre-existing comments and the unrelated server entry survive.
        assert "# user preference" in text
        assert "# default model" in text
        assert "[mcp_servers.other]" in text
        # New entry is present.
        assert "MoneyBin (alice)" in text or '"MoneyBin (alice)"' in text


class TestMCPInstallVSCode:
    """VS Code uses workspace-local .vscode/mcp.json with `servers` key."""

    def test_install_vscode_print_uses_servers_key(self) -> None:
        result = runner.invoke(
            app,
            ["install", "--client", "vscode", "--profile", "alice", "--print"],
        )
        assert result.exit_code == 0
        assert '"servers"' in result.output
        assert '"type": "stdio"' in result.output
        # Standard `mcpServers` key must NOT be present in vscode output.
        assert "mcpServers" not in result.output

    def test_install_vscode_writes_workspace_file(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp.find_repo_root", lambda: tmp_path
        )
        result = runner.invoke(
            app,
            [
                "install",
                "--client",
                "vscode",
                "--profile",
                "alice",
                "--yes",
            ],
        )
        assert result.exit_code == 0
        target = tmp_path / ".vscode" / "mcp.json"
        assert target.exists()
        import json as _json

        payload = _json.loads(target.read_text())
        assert "servers" in payload
        entry = next(iter(payload["servers"].values()))
        assert entry["type"] == "stdio"
        assert entry["command"] == "uv"

    def test_install_vscode_outside_repo_errors(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("moneybin.cli.commands.mcp.find_repo_root", lambda: None)
        result = runner.invoke(
            app,
            ["install", "--client", "vscode", "--yes"],
        )
        assert result.exit_code == 1


class TestMCPInstallGeminiCLI:
    """gemini-cli installs to a fixed user-level path with the standard `mcpServers` shape."""

    def test_install_gemini_cli_print_emits_mcp_servers(self) -> None:
        result = runner.invoke(
            app,
            ["install", "--client", "gemini-cli", "--profile", "alice", "--print"],
        )
        assert result.exit_code == 0
        assert "mcpServers" in result.output

    def test_install_gemini_cli_writes_settings(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        target = tmp_path / "settings.json"
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp._get_client_config_path",
            lambda client: target,  # type: ignore[reportUnknownLambdaType]
        )
        result = runner.invoke(
            app,
            [
                "install",
                "--client",
                "gemini-cli",
                "--profile",
                "alice",
                "--yes",
            ],
        )
        assert result.exit_code == 0
        assert target.exists()
        import json as _json

        payload = _json.loads(target.read_text())
        assert "mcpServers" in payload
        # Concurrency guardrail must fire on per-invocation client installs.
        assert "auto-loads" in result.output
