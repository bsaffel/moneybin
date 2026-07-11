"""Tests for MCP CLI enhancements."""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import typer
from typer.testing import CliRunner

from moneybin.cli.commands.mcp import (
    _gate_network_transport,  # pyright: ignore[reportPrivateUsage]
    app,
)

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

    def test_install_chatgpt_desktop_writes_the_shared_codex_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ChatGPT Desktop hosts Codex, and Codex reads ~/.codex/config.toml.

        Per OpenAI's docs: "The ChatGPT desktop app, Codex CLI, and IDE extension
        support MCP servers and share MCP configuration for the same Codex host",
        and the desktop app's Settings → MCP servers → Add server offers STDIO. So
        this client takes a real local install — the same TOML `--client codex`
        writes — and must not be refused.
        """
        target = tmp_path / "config.toml"
        monkeypatch.setattr(
            "moneybin.cli.commands.mcp._get_client_config_path",
            lambda client: target,  # type: ignore[reportUnknownLambdaType]
        )
        result = runner.invoke(
            app, ["install", "--client", "chatgpt-desktop", "--profile", "alice", "-y"]
        )
        assert result.exit_code == 0
        assert target.exists()

        import tomllib

        entry = tomllib.loads(target.read_text())["mcp_servers"]["MoneyBin (alice)"]
        assert Path(entry["command"]).name == "uv"
        assert entry["startup_timeout_sec"] == 30

    def test_chatgpt_desktop_config_path_is_the_codex_one(self) -> None:
        """Same Codex host, same file — installing for one covers the other."""
        from moneybin.cli.commands.mcp import (
            _CLIENT_CONFIG_PATHS,  # pyright: ignore[reportPrivateUsage]
        )

        assert _CLIENT_CONFIG_PATHS["chatgpt-desktop"] == _CLIENT_CONFIG_PATHS["codex"]

    def test_install_chatgpt_desktop_emits_toml_not_json(self) -> None:
        """It's a Codex-shaped config, so it must not print the mcpServers JSON."""
        result = runner.invoke(
            app, ["install", "--client", "chatgpt-desktop", "--print"]
        )
        assert result.exit_code == 0
        assert "[mcp_servers." in result.stdout
        assert "mcpServers" not in result.stdout

    def test_install_chatgpt_desktop_says_web_cannot_reach_a_local_server(self) -> None:
        """The desktop app can; ChatGPT web cannot. Don't let the user conflate them.

        "ChatGPT web doesn't read local Codex configuration files" — so a user who
        installs this and then asks chatgpt.com about their finances will find
        nothing there, and needs to know that up front rather than debug it.
        """
        result = runner.invoke(
            app, ["install", "--client", "chatgpt-desktop", "--print"]
        )
        assert "web" in result.stderr.lower()
        assert "restart" in result.stderr.lower()


class TestMCPInstallSnippetHardening:
    """The generated snippet must survive the launch context clients actually use."""

    def test_snippet_uses_the_absolute_uv_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """GUI-launched macOS clients don't inherit the shell PATH, so `uv` must be absolute."""

        def fake_which(_cmd: str) -> str | None:
            return "/opt/homebrew/bin/uv"

        monkeypatch.setattr("shutil.which", fake_which)
        result = runner.invoke(
            app, ["install", "--client", "claude-desktop", "--print"]
        )
        assert result.exit_code == 0
        assert "/opt/homebrew/bin/uv" in result.output

    def test_snippet_falls_back_to_bare_uv_when_not_resolvable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If `uv` isn't on our own PATH there's nothing to resolve — emit it bare."""

        def fake_which(_cmd: str) -> str | None:
            return None

        monkeypatch.setattr("shutil.which", fake_which)
        result = runner.invoke(
            app, ["install", "--client", "claude-desktop", "--print"]
        )
        assert result.exit_code == 0
        assert '"command": "uv"' in result.output

    def test_codex_snippet_sets_a_startup_timeout(self) -> None:
        """Uvx cold start runs 3-15s; Codex's default startup timeout is 10s."""
        result = runner.invoke(app, ["install", "--client", "codex", "--print"])
        assert result.exit_code == 0
        assert "startup_timeout_sec" in result.output

    def test_gemini_cli_snippet_never_enables_trust(self) -> None:
        """`trust: true` bypasses ALL tool-call confirmations — never for a finance server."""
        result = runner.invoke(app, ["install", "--client", "gemini-cli", "--print"])
        assert result.exit_code == 0
        assert '"trust"' not in result.output

    def test_gemini_cli_install_explains_the_trust_setting(self) -> None:
        """Explain the setting we deliberately left off, so it isn't cargo-culted on."""
        result = runner.invoke(app, ["install", "--client", "gemini-cli", "--print"])
        assert "trust" in result.stderr.lower()
        assert "confirmation" in result.stderr.lower()

    def test_windsurf_install_warns_that_moneybin_exceeds_the_tool_cap(self) -> None:
        """Cascade holds 100 tools; MoneyBin ships 102 and hides none.

        Windsurf gives no signal when it drops the overflow — it just behaves as
        though MoneyBin can't do things it can. The guide says so, but the person
        running the install never reads the guide, so the install has to say it too.
        """
        result = runner.invoke(app, ["install", "--client", "windsurf", "--print"])
        assert result.exit_code == 0
        assert "100" in result.stderr
        assert "102" in result.stderr
        assert "disable" in result.stderr.lower()

    def test_non_windsurf_installs_do_not_carry_the_tool_cap_warning(self) -> None:
        """The cap is Cascade's alone — don't alarm every other client's user."""
        result = runner.invoke(
            app, ["install", "--client", "claude-desktop", "--print"]
        )
        assert "102" not in result.stderr

    @pytest.mark.parametrize(
        "client", ["claude-desktop", "cursor", "windsurf", "gemini-cli", "claude-code"]
    )
    def test_print_emits_only_the_config_bytes_on_stdout(self, client: str) -> None:
        """`--print` promises "the exact bytes the command would write".

        Advisory text (the Gemini trust note, the Claude Code launch hint, the
        auto-load warning) belongs on stderr. Mixed into stdout it breaks the
        documented contract and anything the user pipes it through — this test parses
        stdout as JSON, which is exactly what `mcp install --print | jq` does.
        """
        import json as _json

        result = runner.invoke(app, ["install", "--client", client, "--print"])
        assert result.exit_code == 0
        parsed = _json.loads(result.stdout)  # raises if a note leaked onto stdout
        assert "mcpServers" in parsed

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

    def test_path_chatgpt_desktop_is_the_codex_config(self) -> None:
        """chatgpt-desktop shares the Codex host's config file, so it resolves a path."""
        result = runner.invoke(app, ["config", "path", "--client", "chatgpt-desktop"])
        assert result.exit_code == 0
        assert result.stdout.strip().endswith(".codex/config.toml")

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
        # `uv` is emitted as the absolute path we resolved (bare `uv` only when it
        # isn't on PATH) — GUI-launched clients don't inherit the shell PATH.
        assert 'command = "' in result.output
        assert "uv" in result.output
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
        assert Path(entry["command"]).name == "uv"
        assert "moneybin" in entry["args"]
        # Cold `uv run` outruns Codex's 10s default on first launch.
        assert entry["startup_timeout_sec"] == 30
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
        assert Path(entry["command"]).name == "uv"

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


class TestGateNetworkTransport:
    """The `_gate_network_transport` helper enforces the unauthenticated-HTTP gate.

    stdio is local-only and always allowed. Every network transport (sse,
    streamable-http) binds an UNAUTHENTICATED port and must be opted into with
    --insecure; without it the helper refuses with a usage error (exit 2), with
    it the helper emits a loud warning and returns.
    """

    def test_stdio_is_a_noop_regardless_of_insecure(self) -> None:
        """Stdio never triggers the gate — no raise, no warning."""
        _gate_network_transport("stdio", insecure=False)
        _gate_network_transport("stdio", insecure=True)

    def test_streamable_http_without_insecure_raises_usage_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A network transport without --insecure refuses with exit 2 + risk message."""
        with pytest.raises(typer.Exit) as exc_info:
            _gate_network_transport("streamable-http", insecure=False)
        assert exc_info.value.exit_code == 2
        assert "authentication" in caplog.text.lower()
        assert "--insecure" in caplog.text

    def test_sse_without_insecure_also_refuses(self) -> None:
        """The gate covers every non-stdio transport, not just streamable-http."""
        with pytest.raises(typer.Exit) as exc_info:
            _gate_network_transport("sse", insecure=False)
        assert exc_info.value.exit_code == 2

    def test_streamable_http_with_insecure_warns_and_returns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """With --insecure the gate warns loudly on stderr and allows startup."""
        with caplog.at_level(logging.WARNING):
            _gate_network_transport("streamable-http", insecure=True)
        assert "authentication" in caplog.text.lower()
        assert "⚠️" in caplog.text


@contextmanager
def _mock_server_start() -> Generator[MagicMock, None, None]:
    """Patch out the real MCP server boot so `serve` can reach `mcp.run` inertly.

    Forces the unconfigured branch (fewest external calls) and replaces the
    FastMCP server, DB lifecycle, observability setup, and profile-resolver
    wiring with mocks. Yields the mock server so callers can assert on
    `mock.run(...)`.
    """
    mock_mcp = MagicMock()
    with (
        patch("moneybin.cli.commands.mcp.importlib"),
        patch("moneybin.cli.commands.mcp._is_unconfigured", return_value=True),
        patch("moneybin.mcp.server.init_db"),
        patch("moneybin.mcp.server.close_db"),
        patch("moneybin.mcp.server.mcp", mock_mcp),
        patch("moneybin.observability.setup_observability"),
        patch("moneybin.config.register_profile_resolver"),
        patch("moneybin.mcp.first_run.FirstRunSetupMiddleware"),
    ):
        yield mock_mcp


class TestMCPServe:
    """The `serve` command wires the unauthenticated-HTTP gate to a CLI flag."""

    def test_network_transport_without_insecure_refuses(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """`serve --transport streamable-http` (no --insecure) exits 2 with the risk stated.

        No server mock is needed: the gate raises before `serve` imports the
        server stack, so the command never reaches startup.
        """
        result = runner.invoke(app, ["serve", "--transport", "streamable-http"])
        assert result.exit_code == 2
        assert "authentication" in caplog.text.lower()
        assert "--insecure" in caplog.text

    def test_invalid_transport_is_a_usage_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """An unknown transport value exits 2 (usage error), matching the flag conventions."""
        result = runner.invoke(app, ["serve", "--transport", "bogus"])
        assert result.exit_code == 2
        assert "Invalid transport" in caplog.text

    def test_insecure_flag_starts_network_transport_with_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """`--insecure` allows the network transport to start after a loud warning."""
        with _mock_server_start() as mock_mcp:
            with caplog.at_level(logging.WARNING):
                result = runner.invoke(
                    app, ["serve", "--transport", "streamable-http", "--insecure"]
                )
        assert result.exit_code == 0
        mock_mcp.run.assert_called_once_with(transport="streamable-http")
        assert "authentication" in caplog.text.lower()

    def test_stdio_default_starts_without_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The default stdio path is unaffected — it starts and emits no auth warning."""
        with _mock_server_start() as mock_mcp:
            result = runner.invoke(app, ["serve"])
        assert result.exit_code == 0
        mock_mcp.run.assert_called_once_with(transport="stdio")
        assert "authentication" not in caplog.text.lower()
