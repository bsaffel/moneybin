"""MCP server commands for MoneyBin CLI.

`moneybin mcp serve` starts the Model Context Protocol server that exposes
DuckDB financial data to MCP-compatible clients. `moneybin mcp config
generate --client <c>` produces install snippets for the supported clients
(see `_SUPPORTED_CLIENTS`); `docs/guides/mcp-clients.md` documents per-client
behavior, the concurrency model, and per-session opt-in for Claude Code.
"""

import asyncio
import importlib
import json
import logging
import os
import signal
from pathlib import Path
from typing import Annotated, Any, Literal, get_args

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json
from moneybin.config import find_repo_root, get_base_dir

app = typer.Typer(help="MCP server for AI assistant integration", no_args_is_help=True)
logger = logging.getLogger(__name__)

# Transport types supported by FastMCP.run()
TransportType = Literal["stdio", "sse", "streamable-http"]
_VALID_TRANSPORTS: tuple[str, ...] = get_args(TransportType)

# MCP client config file locations for clients that auto-load from a fixed path.
# claude-code uses a per-profile path resolved at runtime (see _client_install_path).
# vscode uses a workspace-local path (.vscode/mcp.json) resolved at runtime.
# chatgpt-desktop has no config file — servers are added via the Connectors UI.
_CLIENT_CONFIG_PATHS: dict[str, Path] = {
    "claude-desktop": Path.home()
    / "Library"
    / "Application Support"
    / "Claude"
    / "claude_desktop_config.json",
    "cursor": Path.home() / ".cursor" / "mcp.json",
    "windsurf": Path.home() / ".codeium" / "windsurf" / "mcp_config.json",
    "gemini-cli": Path.home() / ".gemini" / "settings.json",
    "codex": Path.home() / ".codex" / "config.toml",
}

# Clients that don't write to a fixed path. Listed for help text and validation.
_PROFILE_SCOPED_CLIENTS: tuple[str, ...] = ("claude-code",)
_WORKSPACE_SCOPED_CLIENTS: tuple[str, ...] = ("vscode",)
_NO_INSTALL_CLIENTS: tuple[str, ...] = ("chatgpt-desktop",)

_SUPPORTED_CLIENTS: tuple[str, ...] = (
    *_CLIENT_CONFIG_PATHS,
    *_PROFILE_SCOPED_CLIENTS,
    *_WORKSPACE_SCOPED_CLIENTS,
    *_NO_INSTALL_CLIENTS,
)

_DEFAULT_CLIENT = "claude-desktop"

# ── config subgroup ──────────────────────────────────────────────────────────

config_app = typer.Typer(help="MCP server configuration")
app.add_typer(config_app, name="config")


@config_app.callback(invoke_without_command=True)
def mcp_config_show(ctx: typer.Context) -> None:
    """Display current MCP server configuration.

    Shows the active profile, database path, and MCP-specific limits
    (max_rows, max_chars). Runs automatically when `mcp config` is
    invoked without a subcommand.
    """
    if ctx.invoked_subcommand is not None:
        return

    from moneybin.config import get_current_profile, get_database_path, get_settings

    settings = get_settings()
    profile = get_current_profile()
    db_path = get_database_path()

    typer.echo(f"Profile:    {profile}")
    typer.echo(f"Database:   {db_path}")
    typer.echo(f"max_rows:   {settings.mcp.max_rows}")
    typer.echo(f"max_chars:  {settings.mcp.max_chars}")


@config_app.command("path")
def mcp_config_path(
    client: Annotated[
        str,
        typer.Option(
            "--client",
            "-c",
            help=f"MCP client. Supported: {', '.join(_SUPPORTED_CLIENTS)}",
        ),
    ] = _DEFAULT_CLIENT,
    profile: Annotated[
        str | None,
        typer.Option("--profile", "-p", help="MoneyBin profile (default: active)."),
    ] = None,
) -> None:
    """Print the install path of an MCP client's config file.

    Used by `make claude-mcp` to locate the per-profile Claude Code config.
    Exits non-zero with no output for clients that don't have a JSON config
    file (e.g. chatgpt-desktop).

    Profile resolution is non-interactive: if no profile is set and `--profile`
    is not given, this exits with a clear error instead of starting the
    first-run wizard. The wizard's stdin prompts would hang under any
    `$(...)` command substitution that captures stdout.
    """
    from moneybin.config import get_current_profile

    if client not in _SUPPORTED_CLIENTS:
        supported = ", ".join(_SUPPORTED_CLIENTS)
        logger.error(f"❌ Unknown client '{client}'. Supported: {supported}")
        raise typer.Exit(2)

    # Profile resolution only matters for profile-scoped clients (claude-code).
    # Fixed-path and workspace-scoped clients have profile-independent paths,
    # so skip the lookup and let an unset profile produce a placeholder.
    needs_profile = client in _PROFILE_SCOPED_CLIENTS
    if profile:
        resolved_profile = profile
    elif needs_profile:
        try:
            resolved_profile = get_current_profile(auto_resolve=False)
        except RuntimeError as e:
            logger.error(
                "❌ No active profile and --profile not supplied. "
                "Run `moneybin profile create <name>` or pass `--profile <name>`."
            )
            raise typer.Exit(1) from e
    else:
        resolved_profile = ""  # unused for non-profile-scoped clients

    path = _client_install_path(client, resolved_profile)
    if path is None:
        if client in _WORKSPACE_SCOPED_CLIENTS:
            logger.error(
                f"❌ {client} config path requires running inside a repo "
                "(no git root found from current directory)."
            )
        raise typer.Exit(1)
    typer.echo(str(path))


@config_app.command("generate")
def mcp_config_generate(
    client: Annotated[
        str,
        typer.Option(
            "--client",
            "-c",
            help=f"MCP client to generate config for. Supported: {', '.join(_SUPPORTED_CLIENTS)}",
        ),
    ] = _DEFAULT_CLIENT,
    profile: Annotated[
        str | None,
        typer.Option(
            "--profile",
            "-p",
            help="MoneyBin profile to use in the generated config.",
        ),
    ] = None,
    install: Annotated[
        bool,
        typer.Option(
            "--install",
            help="Write the generated config directly into the client's config file.",
        ),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Skip confirmation prompt when --install is set.",
        ),
    ] = False,
) -> None:
    """Generate an MCP server config snippet for an AI client.

    Prints a JSON snippet that registers MoneyBin as an MCP server.
    With --install, merges the snippet into the client's existing
    config file (creating it if absent).

    Args:
        client: Target MCP client identifier.
        profile: MoneyBin profile to embed in the config.
        install: Write directly to the client's config file.
        yes: Bypass install confirmation prompt.

    Examples:
        # Print config snippet for Claude Desktop
        moneybin mcp config generate --client claude-desktop

        # Install directly without prompting
        moneybin mcp config generate --client claude-desktop --install --yes

        # Profile-scoped Claude Code config (loaded only with `claude --mcp-config`)
        moneybin mcp config generate --client claude-code --install --yes

        # Print snippet + step-by-step Connector setup for ChatGPT Desktop
        moneybin mcp config generate --client chatgpt-desktop

        # Codex (CLI / Desktop app / IDE extension all share ~/.codex/config.toml)
        moneybin mcp config generate --client codex --install --yes

        # Workspace-local .vscode/mcp.json
        moneybin mcp config generate --client vscode --install --yes

        # User-level ~/.gemini/settings.json
        moneybin mcp config generate --client gemini-cli --install --yes
    """
    from moneybin.config import get_current_profile

    if client not in _SUPPORTED_CLIENTS:
        supported = ", ".join(_SUPPORTED_CLIENTS)
        logger.error(f"❌ Unknown client '{client}'. Supported: {supported}")
        raise typer.Exit(2)  # usage error — matches `mcp config path` convention

    resolved_profile = profile or get_current_profile()

    args: list[str] = ["run"]
    env: dict[str, str] = {}

    # os.getenv used intentionally: get_settings().base_dir cannot distinguish
    # an explicit MONEYBIN_HOME from the default-derived path; we need to know
    # whether the user set it so we can pin it in the generated client config
    # env block.
    moneybin_home = os.getenv("MONEYBIN_HOME")
    repo_root = find_repo_root()

    if moneybin_home:
        # Explicit override — pin the home so it survives the client's launch context.
        env["MONEYBIN_HOME"] = str(Path(moneybin_home).expanduser().resolve())
        if repo_root is not None:
            args += ["--directory", str(repo_root)]
    elif repo_root is not None:
        # Repo checkout: anchor uv at the repo root so repo detection resolves
        # the local .moneybin/ at server-launch time.
        args += ["--directory", str(repo_root)]
    # else: default ~/.moneybin/ — omit --directory; rely on a global install on PATH.

    args += ["moneybin", "--profile", resolved_profile, "mcp", "serve"]

    server_entry: dict[str, Any] = {"command": "uv", "args": args}
    if env:
        server_entry["env"] = env

    entry_name = (
        f"MoneyBin ({resolved_profile})"
        if resolved_profile != "default"
        else "MoneyBin"
    )
    snippet, snippet_text = _build_snippet(client, entry_name, server_entry)
    typer.echo(snippet_text)

    if client == "chatgpt-desktop":
        if install:
            logger.error(
                "❌ --install is not supported for chatgpt-desktop. "
                "ChatGPT Desktop adds MCP servers through its Connectors UI, "
                "not a JSON config file. Follow the instructions below to add "
                "MoneyBin as a custom connector."
            )
            _print_chatgpt_desktop_instructions(server_entry, entry_name)
            raise typer.Exit(1)
        _print_chatgpt_desktop_instructions(server_entry, entry_name)
        return

    if client == "claude-code":
        config_path = _client_install_path(client, resolved_profile)
        if config_path is None:  # unreachable — claude-code always resolves a path
            raise typer.Exit(1)
        if not install:
            _print_claude_code_launch_hint(config_path)
            return
        _confirm_and_merge(config_path, snippet, yes=yes)
        _print_claude_code_launch_hint(config_path)
        return

    if client == "vscode":
        vscode_path = _client_install_path(client, resolved_profile)
        if vscode_path is None:
            logger.error(
                "❌ vscode --install requires running inside a repo "
                "(creates .vscode/mcp.json in the repo root)."
            )
            raise typer.Exit(1)
        if not install:
            return
        _confirm_and_merge(vscode_path, snippet, yes=yes)
        return

    if not install:
        return

    config_path = _get_client_config_path(client)
    if _confirm_and_merge(config_path, snippet, yes=yes):
        _maybe_warn_auto_load(client, resolved_profile)


# ── helpers ──────────────────────────────────────────────────────────────────


def _client_install_path(client: str, profile: str) -> Path | None:
    """Resolve the install path for a client, or None if it has no config file.

    Claude Code is the only client with per-launch MCP override support, so we
    keep its config in a per-profile file (`<base>/profiles/<profile>/...`) and
    leave it un-auto-loaded; `make claude-mcp` opts in via
    `claude --strict-mcp-config --mcp-config <path>`. The other clients all
    auto-load their canonical config — see docs/guides/mcp-clients.md for the
    concurrency model. VS Code uses a workspace-local `.vscode/mcp.json`
    resolved from the current repo root; returns None when not in a repo.
    """
    if client in _CLIENT_CONFIG_PATHS:
        return _CLIENT_CONFIG_PATHS[client]
    if client == "claude-code":
        return get_base_dir() / "profiles" / profile / "claude-code-mcp.json"
    if client == "vscode":
        repo = find_repo_root()
        if repo is None:
            return None
        return repo / ".vscode" / "mcp.json"
    return None


def _build_snippet(
    client: str, entry_name: str, server_entry: dict[str, Any]
) -> tuple[dict[str, Any], str]:
    """Build the (parsed-snippet-dict, rendered-text) pair for a client.

    Most clients use the canonical `{"mcpServers": {<name>: {...}}}` JSON shape.
    VS Code's workspace `.vscode/mcp.json` uses `{"servers": {...}}` with an
    explicit `"type": "stdio"` field. Codex uses TOML under `[mcp_servers.<name>]`.
    The dict half is what `_merge_client_config` writes; the text half is what
    we echo to the user.
    """
    if client == "vscode":
        vscode_entry = {"type": "stdio", **server_entry}
        snippet: dict[str, Any] = {"servers": {entry_name: vscode_entry}}
        return snippet, json.dumps(snippet, indent=2)
    if client == "codex":
        snippet = {"mcp_servers": {entry_name: server_entry}}
        return snippet, _render_codex_toml(snippet)
    snippet = {"mcpServers": {entry_name: server_entry}}
    return snippet, json.dumps(snippet, indent=2)


def _render_codex_toml(snippet: dict[str, Any]) -> str:
    """Render the codex snippet via tomlkit so display matches what we install.

    Using the same TOML writer for both the printed snippet and the
    `_merge_toml_config` write guarantees byte-identical output, eliminating
    any divergence in quoting, escaping, or whitespace between what the user
    sees and what lands in `config.toml`.
    """
    import tomlkit

    doc = tomlkit.document()
    for top_key, top_val in snippet.items():
        if isinstance(top_val, dict):
            section = tomlkit.table()
            for entry_name, entry_val in top_val.items():
                section[entry_name] = entry_val  # type: ignore[index]  # tomlkit table behaves as MutableMapping at runtime; stub omits __setitem__
            doc[top_key] = section
        else:
            doc[top_key] = top_val
    return tomlkit.dumps(doc).rstrip()  # pyright: ignore[reportUnknownMemberType]  # tomlkit.dumps stub returns Unknown


def _confirm_and_merge(
    config_path: Path, snippet: dict[str, Any], *, yes: bool
) -> bool:
    """Confirm with the user (unless --yes) and merge the snippet into the file.

    Dispatches by file suffix: `.toml` files are round-tripped through tomlkit
    so existing comments and key ordering survive the merge. JSON files use
    the simpler shallow-merge path.

    Returns True if the file was written, False if the user declined the
    confirmation prompt. Callers gate post-install side effects (e.g. the
    auto-load warning) on the return value so users who decline don't get
    warnings about a server they didn't install.
    """
    if not yes:
        confirmed = typer.confirm(f"\nInstall into {config_path}?", default=False)
        if not confirmed:
            logger.info("Installation cancelled.")
            return False
    if config_path.suffix == ".toml":
        _merge_toml_config(config_path, snippet)
    else:
        _merge_client_config(config_path, snippet)
    logger.info(f"✅ Config written to {config_path}")
    return True


def _merge_toml_config(config_path: Path, patch: dict[str, Any]) -> None:
    """Merge `patch` into a TOML file, preserving comments and key ordering.

    `patch` follows the same nested-dict shape as JSON snippets (e.g.
    `{"mcp_servers": {"<name>": {...}}}`). Only the leaf entries we own are
    overwritten — sibling keys, comments, and formatting are left intact.
    """
    import tomlkit
    from tomlkit.exceptions import TOMLKitError

    config_path.parent.mkdir(parents=True, exist_ok=True)

    if config_path.exists():
        try:
            doc = tomlkit.parse(config_path.read_text())
        except TOMLKitError:
            logger.error(
                f"❌ Cannot parse existing TOML at {config_path}. "
                "Fix the file manually before running --install again."
            )
            raise typer.Exit(1) from None
    else:
        doc = tomlkit.document()

    for top_key, top_val in patch.items():
        if isinstance(top_val, dict):
            section = doc.get(top_key)  # pyright: ignore[reportUnknownMemberType]
            if not isinstance(section, dict):
                section = tomlkit.table()
                doc[top_key] = section
            for entry_name, entry_val in top_val.items():
                section[entry_name] = entry_val  # type: ignore[index]  # tomlkit table behaves as MutableMapping at runtime; stub omits __setitem__
        else:
            doc[top_key] = top_val

    config_path.write_text(tomlkit.dumps(doc))  # pyright: ignore[reportUnknownMemberType]  # tomlkit.dumps stub returns Unknown


def _get_client_config_path(client: str) -> Path:
    """Return the fixed-path config file for clients in `_CLIENT_CONFIG_PATHS`.

    Profile-scoped (`claude-code`) and no-install (`chatgpt-desktop`) clients are
    handled inline in `config_generate` and never reach this helper.
    """
    if client not in _CLIENT_CONFIG_PATHS:
        supported = ", ".join(_CLIENT_CONFIG_PATHS)
        logger.error(f"❌ Unknown client '{client}'. Supported: {supported}")
        raise typer.Exit(1)
    return _CLIENT_CONFIG_PATHS[client]


# Per-invocation CLI clients: every shell command spawns a fresh server.
# Surface this so users understand the "always-on" install semantics.
_PER_INVOCATION_CLIENTS: frozenset[str] = frozenset({"codex", "gemini-cli"})


def _maybe_warn_auto_load(client: str, profile: str) -> None:
    """Warn after install when the client auto-loads on every invocation.

    For codex (CLI/Desktop/IDE) and gemini-cli, install means MoneyBin starts on
    every shell launch of that tool — same profile from two terminals will fight
    over the lock. Surface this so users can choose paste-only instead.
    """
    if client not in _PER_INVOCATION_CLIENTS:
        return
    surface = (
        "the Codex CLI, Desktop app, and IDE extension"
        if client == "codex"
        else "every `gemini` invocation"
    )
    typer.echo("")
    typer.echo(
        f"⚠️  {client} auto-loads MoneyBin on {surface}. Two concurrent "
        f"sessions on profile '{profile}' will fight over the DB write lock — "
        "the second exits. See docs/guides/mcp-clients.md."
    )


def _print_claude_code_launch_hint(config_path: Path) -> None:
    """Tell the user how to launch Claude Code with the generated config."""
    import shlex

    typer.echo("")
    typer.echo("Launch Claude Code with this MCP server only:")
    typer.echo(
        f"  claude --strict-mcp-config --mcp-config {shlex.quote(str(config_path))}"
    )
    typer.echo("")
    typer.echo(
        "Or run `make claude-mcp` from the repo to launch with the active profile."
    )


def _print_chatgpt_desktop_instructions(
    server_entry: dict[str, Any], entry_name: str
) -> None:
    """Print step-by-step Connector setup for ChatGPT Desktop.

    ChatGPT Desktop installs MCP servers through Settings → Connectors (Developer
    Mode), not a JSON config file we can write. The snippet above is informational
    — copy individual fields into the connector form.
    """
    command = server_entry["command"]
    args_str = " ".join(server_entry.get("args", []))
    env_pairs = server_entry.get("env", {})

    typer.echo("")
    typer.echo("ChatGPT Desktop install steps:")
    typer.echo("  1. Open ChatGPT → Settings → Connectors.")
    typer.echo(
        "     If you don't see 'Add custom connector', enable Developer Mode "
        "under Settings → Advanced first."
    )
    typer.echo("  2. Click 'Add custom connector' → choose the local/stdio option.")
    typer.echo(f"  3. Name: {entry_name}")
    typer.echo(f"     Command: {command}")
    typer.echo(f"     Arguments: {args_str}")
    if env_pairs:
        typer.echo("     Environment variables:")
        for key, value in env_pairs.items():
            typer.echo(f"       {key}={value}")
    typer.echo("  4. Save. ChatGPT will spawn the server on demand.")
    typer.echo("")
    typer.echo(
        "Note: ChatGPT Desktop's MCP support is gated by version and plan. "
        "If your build only accepts HTTP connectors, run "
        "`moneybin mcp serve --transport streamable-http` and add the resulting "
        "URL as a custom connector instead."
    )


def _merge_client_config(config_path: Path, patch: dict[str, Any]) -> None:
    """Merge a patch dict into a JSON config file, creating it if absent.

    The merge is shallow at the top level — nested keys under
    ``mcpServers`` are merged by name so existing servers are preserved.

    Args:
        config_path: Path to the JSON config file.
        patch: Dict to merge in (e.g. ``{"mcpServers": {"MoneyBin": {...}}}``.
    """
    config_path.parent.mkdir(parents=True, exist_ok=True)

    existing: dict[str, Any] = {}
    if config_path.exists():
        try:
            existing = json.loads(config_path.read_text())
        except json.JSONDecodeError:
            logger.error(
                f"❌ Cannot parse existing config at {config_path}. "
                "Fix the JSON manually before running --install again."
            )
            raise typer.Exit(1) from None

    for key, value in patch.items():
        existing_val = existing.get(key)
        if isinstance(value, dict) and isinstance(existing_val, dict):
            existing[key] = {**existing_val, **value}
        else:
            existing[key] = value

    config_path.write_text(json.dumps(existing, indent=2))


# ── list-tools ───────────────────────────────────────────────────────────────


@app.command("list-tools")
def mcp_list_tools(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001 — list-tools has no info chatter; only data lines
) -> None:
    """List all registered MCP tools.

    Enumerates the v1 namespace registry. Useful for verifying that
    all expected tools are available before connecting an AI client.

    Examples:
        moneybin mcp list-tools
    """
    from moneybin.mcp.server import init_db, mcp

    init_db()
    # Bypass visibility filters so list-tools shows every registered tool,
    # including extended-namespace tools that are hidden by default.
    tools = asyncio.run(mcp._list_tools())  # noqa: SLF001 — public API filters by visibility  # pyright: ignore[reportPrivateUsage]

    sorted_tools = sorted(tools, key=lambda t: t.name)

    if output == OutputFormat.JSON:
        tools_payload = [
            {
                "name": tool.name,
                "description": getattr(tool, "description", "") or "",
            }
            for tool in sorted_tools
        ]
        emit_json("tools", tools_payload)
        return

    for tool in sorted_tools:
        description = getattr(tool, "description", "") or ""
        typer.echo(f"  {tool.name}: {description}")


# ── list-prompts ─────────────────────────────────────────────────────────────


@app.command("list-prompts")
def mcp_list_prompts(
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List all registered MCP prompts.

    Imports prompt modules to trigger decorator registration, then
    enumerates the FastMCP prompt registry.

    Examples:
        moneybin mcp list-prompts
    """
    from moneybin.mcp.server import (
        mcp,  # noqa: PLC0415 — defer fastmcp import to subcommand body
    )

    for module in (
        "moneybin.mcp.prompts",
        "moneybin.mcp.resources",
    ):
        importlib.import_module(module)

    prompts = asyncio.run(mcp.list_prompts(run_middleware=False))

    sorted_prompts = sorted(prompts, key=lambda p: p.name)

    if output == OutputFormat.JSON:
        prompts_payload = [
            {
                "name": prompt.name,
                "description": getattr(prompt, "description", None) or "",
            }
            for prompt in sorted_prompts
        ]
        emit_json("prompts", prompts_payload)
        return

    if not sorted_prompts:
        if not quiet:
            typer.echo("No prompts registered.")
        return

    for prompt in sorted_prompts:
        description = getattr(prompt, "description", None) or ""
        typer.echo(f"  {prompt.name}  {description}")


# ── serve ────────────────────────────────────────────────────────────────────


@app.command("serve")
def serve(
    transport: Annotated[
        str,
        typer.Option(
            "--transport",
            "-t",
            help="MCP transport type: stdio, sse, or streamable-http",
        ),
    ] = "stdio",
) -> None:
    """Start the MoneyBin MCP server.

    This launches an MCP server that gives AI assistants full access to
    your financial data in DuckDB — querying, importing, categorizing,
    and budgeting. The server communicates via stdio (standard input/output)
    by default, which is the standard transport for local MCP integrations.

    The server uses the currently active profile to determine which
    database to connect to.

    Examples:
        # Start MCP server with default profile
        moneybin mcp serve

        # Start with specific profile
        moneybin --profile=alice mcp serve

        # Typically invoked by AI clients, not run manually
    """
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.config import get_database_path
    from moneybin.mcp.server import close_db, init_db, mcp

    # Import resources/prompts to register their decorators with the server.
    # Tools are registered via register_core_tools() in init_db().
    for module in (
        "moneybin.mcp.resources",
        "moneybin.mcp.prompts",
    ):
        importlib.import_module(module)

    db_path = get_database_path()

    from moneybin.cli.utils import get_verbose_flag
    from moneybin.config import get_current_profile
    from moneybin.observability import setup_observability

    setup_observability(
        stream="mcp", verbose=get_verbose_flag(), profile=get_current_profile()
    )

    logger.info(f"Starting MCP server with database: {db_path}")

    if transport not in _VALID_TRANSPORTS:
        logger.error(
            f"Invalid transport '{transport}'. Must be one of: {', '.join(_VALID_TRANSPORTS)}"
        )
        raise typer.Exit(1)

    # Cast validated string to the literal type
    validated_transport: TransportType = transport  # type: ignore[assignment] — validated above

    # Convert SIGTERM to SystemExit so the finally block runs and we close
    # the DuckDB connection cleanly. Without this, parent-driven shutdowns
    # (Claude Desktop/Code disconnecting via `kill <pid>`) leak the DB FD
    # until OS process teardown.
    def _on_sigterm(_signum: int, _frame: Any) -> None:
        logger.info("MCP server received SIGTERM; shutting down")
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _on_sigterm)

    try:
        with handle_cli_errors():
            init_db()
        logger.info(f"MCP server starting (transport={transport}, db={db_path})")
        mcp.run(transport=validated_transport)
    except FileNotFoundError as e:
        logger.error(f"Database not found: {e}")
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        logger.info("MCP server stopped by user")
    finally:
        # Flush metrics before closing — close_db() clears the singleton,
        # so the atexit handler would find no DB to flush to.
        from moneybin.observability import flush_metrics

        flush_metrics()
        close_db()
