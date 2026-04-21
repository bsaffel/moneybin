"""MCP server commands for MoneyBin CLI.

This module provides the `moneybin mcp serve` command that starts the
Model Context Protocol server, exposing DuckDB financial data to AI
assistants like Cursor, Claude Desktop, and ChatGPT Desktop.
"""

import importlib
import json
import logging
from pathlib import Path
from typing import Annotated, Any, Literal, get_args

import typer

from moneybin.config import get_base_dir
from moneybin.mcp.server import mcp as mcp_server

app = typer.Typer(help="MCP server for AI assistant integration", no_args_is_help=True)
logger = logging.getLogger(__name__)

# Transport types supported by FastMCP.run()
TransportType = Literal["stdio", "sse", "streamable-http"]
_VALID_TRANSPORTS: tuple[str, ...] = get_args(TransportType)

# Supported MCP client config file locations
_CLIENT_CONFIG_PATHS: dict[str, Path] = {
    "claude-desktop": Path.home()
    / "Library"
    / "Application Support"
    / "Claude"
    / "claude_desktop_config.json",
    "cursor": Path.home() / ".cursor" / "mcp.json",
    "windsurf": Path.home() / ".codeium" / "windsurf" / "mcp_config.json",
}

_DEFAULT_CLIENT = "claude-desktop"

# ── config subgroup ──────────────────────────────────────────────────────────

config_app = typer.Typer(help="MCP server configuration")
app.add_typer(config_app, name="config")


@config_app.callback(invoke_without_command=True)
def config_show(ctx: typer.Context) -> None:
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


@config_app.command("generate")
def config_generate(
    client: Annotated[
        str,
        typer.Option(
            "--client",
            "-c",
            help=f"MCP client to generate config for. Supported: {', '.join(_CLIENT_CONFIG_PATHS)}",
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
    """
    from moneybin.config import get_current_profile

    resolved_profile = profile or get_current_profile()

    server_entry: dict[str, Any] = {
        "command": "uv",
        "args": [
            "run",
            "--directory",
            str(get_base_dir()),
            "moneybin",
            "--profile",
            resolved_profile,
            "mcp",
            "serve",
        ],
    }

    entry_name = (
        f"MoneyBin ({resolved_profile})"
        if resolved_profile != "default"
        else "MoneyBin"
    )
    snippet = {"mcpServers": {entry_name: server_entry}}
    snippet_json = json.dumps(snippet, indent=2)

    typer.echo(snippet_json)

    if not install:
        return

    config_path = _get_client_config_path(client)

    if not yes:
        confirmed = typer.confirm(
            f"\nInstall into {config_path}?",
            default=False,
        )
        if not confirmed:
            logger.info("Installation cancelled.")
            return

    _merge_client_config(config_path, snippet)
    logger.info(f"✅ Config written to {config_path}")


# ── helpers ──────────────────────────────────────────────────────────────────


def _get_client_config_path(client: str) -> Path:
    """Return the config file path for the given MCP client.

    Args:
        client: Client identifier (e.g. "claude-desktop").

    Returns:
        Absolute path to the client's config file.

    Raises:
        typer.Exit: If the client is not recognized.
    """
    if client not in _CLIENT_CONFIG_PATHS:
        supported = ", ".join(_CLIENT_CONFIG_PATHS)
        logger.error(f"❌ Unknown client '{client}'. Supported: {supported}")
        raise typer.Exit(1)
    return _CLIENT_CONFIG_PATHS[client]


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
def list_tools() -> None:
    """List all registered MCP tools.

    Imports tool modules to trigger decorator registration, then
    enumerates the FastMCP tool registry. Useful for verifying that
    all expected tools are available before connecting an AI client.

    Examples:
        moneybin mcp list-tools
    """
    for module in (
        "moneybin.mcp.tools",
        "moneybin.mcp.write_tools",
    ):
        importlib.import_module(module)

    tools: dict[str, object] = mcp_server._tool_manager._tools  # type: ignore[reportAttributeAccessIssue] — accessing FastMCP internals

    if not tools:
        typer.echo("No tools registered.")
        return

    for name, tool in sorted(tools.items()):
        description = getattr(tool, "description", None) or ""
        typer.echo(f"  {name}  {description}")


# ── list-prompts ─────────────────────────────────────────────────────────────


@app.command("list-prompts")
def list_prompts() -> None:
    """List all registered MCP prompts.

    Imports prompt modules to trigger decorator registration, then
    enumerates the FastMCP prompt registry.

    Examples:
        moneybin mcp list-prompts
    """
    for module in (
        "moneybin.mcp.prompts",
        "moneybin.mcp.resources",
    ):
        importlib.import_module(module)

    prompts: dict[str, object] = mcp_server._prompt_manager._prompts  # type: ignore[reportAttributeAccessIssue] — accessing FastMCP internals

    if not prompts:
        typer.echo("No prompts registered.")
        return

    for name, prompt in sorted(prompts.items()):
        description = getattr(prompt, "description", None) or ""
        typer.echo(f"  {name}  {description}")


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
    from moneybin.config import get_database_path
    from moneybin.database import DatabaseKeyError
    from moneybin.mcp.server import close_db, init_db, mcp

    # Import tools/resources/prompts to register their decorators with the server
    for module in (
        "moneybin.mcp.tools",
        "moneybin.mcp.write_tools",
        "moneybin.mcp.resources",
        "moneybin.mcp.prompts",
    ):
        importlib.import_module(module)

    db_path = get_database_path()

    from moneybin.observability import setup_observability

    setup_observability(stream="mcp")

    logger.info(f"Starting MCP server with database: {db_path}")

    if transport not in _VALID_TRANSPORTS:
        logger.error(
            f"Invalid transport '{transport}'. Must be one of: {', '.join(_VALID_TRANSPORTS)}"
        )
        raise typer.Exit(1)

    # Cast validated string to the literal type
    validated_transport: TransportType = transport  # type: ignore[assignment] — validated above

    try:
        init_db()
        logger.info(f"MCP server starting (transport={transport}, db={db_path})")
        mcp.run(transport=validated_transport)
    except DatabaseKeyError as e:
        logger.error(f"❌ Database is locked: {e}")
        typer.echo(
            "💡 Run 'moneybin db unlock' to unlock the database first.",
            err=True,
        )
        raise typer.Exit(1) from e
    except FileNotFoundError as e:
        logger.error(f"Database not found: {e}")
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        logger.info("MCP server stopped by user")
    finally:
        close_db()
