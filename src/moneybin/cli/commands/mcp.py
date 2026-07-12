"""MCP server commands for MoneyBin CLI.

`moneybin mcp serve` starts the Model Context Protocol server that exposes
DuckDB financial data to MCP-compatible clients. `moneybin mcp install
--client <c>` writes the install snippet into the client's config file (use
`--print` to emit the snippet without writing); see `_SUPPORTED_CLIENTS` for
the supported clients. `docs/guides/mcp-clients.md` documents per-client
behavior, the concurrency model, and per-session opt-in for Claude Code.
"""

import asyncio
import importlib
import json
import logging
import os
import shutil
import signal
from pathlib import Path
from typing import Annotated, Any, Literal, get_args

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import _flags  # pyright: ignore[reportPrivateUsage]
from moneybin.config import find_repo_root, get_base_dir
from moneybin.protocol.envelope import build_envelope
from moneybin.utils.user_config import get_default_profile

app = typer.Typer(help="MCP server for AI assistant integration", no_args_is_help=True)
logger = logging.getLogger(__name__)

# Transport types supported by FastMCP.run()
TransportType = Literal["stdio", "sse", "streamable-http"]
_VALID_TRANSPORTS: tuple[str, ...] = get_args(TransportType)

# MCP client config file locations for clients that auto-load from a fixed path.
# claude-code uses a per-profile path resolved at runtime (see _client_install_path).
# vscode uses a workspace-local path (.vscode/mcp.json) resolved at runtime.
_CODEX_CONFIG_PATH = Path.home() / ".codex" / "config.toml"

# The ChatGPT desktop app hosts Codex, and shares its MCP configuration: per
# OpenAI's docs, "The ChatGPT desktop app, Codex CLI, and IDE extension support MCP
# servers and share MCP configuration for the same Codex host" — one
# `~/.codex/config.toml`, stdio servers included (Settings → MCP servers → Add
# server → STDIO). So chatgpt-desktop is a real local install that happens to land
# in Codex's file, NOT a separate config format. ChatGPT *web* is the surface that
# genuinely cannot reach a local server; it doesn't read this file at all.
_CLIENT_CONFIG_PATHS: dict[str, Path] = {
    "claude-desktop": Path.home()
    / "Library"
    / "Application Support"
    / "Claude"
    / "claude_desktop_config.json",
    "cursor": Path.home() / ".cursor" / "mcp.json",
    "windsurf": Path.home() / ".codeium" / "windsurf" / "mcp_config.json",
    "gemini-cli": Path.home() / ".gemini" / "settings.json",
    "codex": _CODEX_CONFIG_PATH,
    "chatgpt-desktop": _CODEX_CONFIG_PATH,
}

# Clients whose config is the Codex TOML (`[mcp_servers.<name>]`), not JSON.
_CODEX_HOSTED_CLIENTS: tuple[str, ...] = ("codex", "chatgpt-desktop")

# Clients that don't write to a fixed path. Listed for help text and validation.
_PROFILE_SCOPED_CLIENTS: tuple[str, ...] = ("claude-code",)
_WORKSPACE_SCOPED_CLIENTS: tuple[str, ...] = ("vscode",)

_SUPPORTED_CLIENTS: tuple[str, ...] = (
    *_CLIENT_CONFIG_PATHS,
    *_PROFILE_SCOPED_CLIENTS,
    *_WORKSPACE_SCOPED_CLIENTS,
)

_DEFAULT_CLIENT = "claude-desktop"

# Codex defaults to a 10s startup timeout, but a cold `uv run` (resolving and
# building the environment on first launch) routinely takes 3-15s — so the very
# first connection is the one most likely to time out, and it reads to the user as
# "MoneyBin is broken" rather than "the environment was still warming up".
_CODEX_STARTUP_TIMEOUT_SEC = 30

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
    Exits non-zero with no output for clients that resolve no path (e.g. `vscode`
    outside a repo checkout).

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


@app.command("install")
def mcp_install(
    client: Annotated[
        str,
        typer.Option(
            "--client",
            "-c",
            help=f"MCP client to install for. Supported: {', '.join(_SUPPORTED_CLIENTS)}",
        ),
    ] = _DEFAULT_CLIENT,
    profile: Annotated[
        str | None,
        typer.Option(
            "--profile",
            "-p",
            help="MoneyBin profile to embed in the generated config.",
        ),
    ] = None,
    print_only: Annotated[
        bool,
        typer.Option(
            "--print",
            help="Print the snippet to stdout instead of writing to the client's config file.",
        ),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help="Skip the install confirmation prompt.",
        ),
    ] = False,
) -> None:
    """Install MoneyBin into a client's MCP config.

    Default behavior writes the config snippet directly into the client's
    config file (with a confirmation prompt unless --yes is set). Use
    --print to emit the snippet to stdout without writing — useful for
    inspection or for merging into a shared config by hand.

    chatgpt-desktop writes the same ~/.codex/config.toml as codex: the ChatGPT
    desktop app hosts Codex and shares its MCP configuration, so one entry serves
    the desktop app, the Codex CLI, and the IDE extension. ChatGPT on the web can
    NOT see it — reaching that needs a remote MCP server (M3D).

    Args:
        client: Target MCP client identifier.
        profile: MoneyBin profile to embed in the config.
        print_only: Emit the snippet to stdout instead of writing it.
        yes: Bypass install confirmation prompt.

    Examples:
        # Install for Claude Desktop without prompting
        moneybin mcp install --client claude-desktop --yes

        # Profile-scoped Claude Code config (loaded only with `claude --mcp-config`)
        moneybin mcp install --client claude-code --profile alice --yes

        # Print the snippet without writing
        moneybin mcp install --client claude-desktop --print

        # Codex (CLI / Desktop app / IDE extension all share ~/.codex/config.toml)
        moneybin mcp install --client codex --yes

        # ChatGPT desktop app (same Codex-hosted config as above)
        moneybin mcp install --client chatgpt-desktop --yes

        # Workspace-local .vscode/mcp.json
        moneybin mcp install --client vscode --yes

        # User-level ~/.gemini/settings.json
        moneybin mcp install --client gemini-cli --yes
    """
    from moneybin.config import get_current_profile

    if client not in _SUPPORTED_CLIENTS:
        supported = ", ".join(_SUPPORTED_CLIENTS)
        logger.error(f"❌ Unknown client '{client}'. Supported: {supported}")
        raise typer.Exit(2)  # usage error — matches `mcp config path` convention

    from moneybin.cli.main import get_version

    resolved_profile = profile or get_current_profile()

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
        # Repo checkout (the dev path): anchor uv at the repo root so repo
        # detection resolves the local .moneybin/ at server-launch time.
        args: list[str] = ["run", "--directory", str(repo_root)]
    else:
        # Installed path: run the PUBLISHED package, pinned. Unpinned would let
        # a new release auto-install on the client's next restart and migrate
        # the user's encrypted database with no user action.
        #
        # Accepted pre-launch gap: until the first tag publishes moneybin to
        # PyPI, this pinned `--from moneybin==X.Y.Z` config is unresolvable, so a
        # user who runs `mcp install` from outside the repo checkout during the
        # pre-launch window gets a config `uv tool run` can't satisfy. Narrow and
        # self-resolving — the documented pre-launch install is the git-clone dev
        # path (which takes the repo_root branch above), and README/
        # ai-client-compatibility.md present the published path as arriving with
        # the first release. No fallback is added: once published the pin is
        # correct, and a permanent warning would be post-launch noise.
        args = ["tool", "run", "--from", f"moneybin=={get_version()}"]

    args += ["moneybin", "--profile", resolved_profile, "mcp", "serve"]

    server_entry: dict[str, Any] = {"command": _resolve_uv_command(), "args": args}
    if env:
        server_entry["env"] = env

    entry_name = (
        f"MoneyBin ({resolved_profile})"
        if resolved_profile != "default"
        else "MoneyBin"
    )
    snippet, snippet_text = _build_snippet(client, entry_name, server_entry)
    typer.echo(snippet_text)
    _print_client_notes(client)

    if client == "claude-code":
        config_path = _client_install_path(client, resolved_profile)
        if config_path is None:  # unreachable — claude-code always resolves a path
            raise typer.Exit(1)
        if print_only:
            _print_claude_code_launch_hint(config_path)
            return
        _confirm_and_merge(config_path, snippet, yes=yes)
        _print_claude_code_launch_hint(config_path)
        return

    if client == "vscode":
        if print_only:
            return
        vscode_path = _client_install_path(client, resolved_profile)
        if vscode_path is None:
            logger.error(
                "❌ vscode install requires running inside a repo "
                "(creates .vscode/mcp.json in the repo root)."
            )
            raise typer.Exit(1)
        _confirm_and_merge(vscode_path, snippet, yes=yes)
        return

    if print_only:
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
    if client in _CODEX_HOSTED_CLIENTS:
        codex_entry = {
            **server_entry,
            "startup_timeout_sec": _CODEX_STARTUP_TIMEOUT_SEC,
        }
        snippet = {"mcp_servers": {entry_name: codex_entry}}
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
                "Fix the file manually before running `mcp install` again."
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

    Profile-scoped (`claude-code`) and workspace-scoped (`vscode`) clients are
    handled inline in `mcp_install` and never reach this helper.
    """
    if client not in _CLIENT_CONFIG_PATHS:
        supported = ", ".join(_CLIENT_CONFIG_PATHS)
        logger.error(f"❌ Unknown client '{client}'. Supported: {supported}")
        raise typer.Exit(1)
    return _CLIENT_CONFIG_PATHS[client]


# Per-invocation CLI clients: every shell command spawns a fresh server.
# Surface this so users understand the "always-on" install semantics.
# chatgpt-desktop is included because it writes the *shared* Codex config — so
# installing "for ChatGPT" also makes every `codex` shell invocation auto-load
# MoneyBin, which is exactly the surprise this warning exists to prevent.
_PER_INVOCATION_CLIENTS: frozenset[str] = frozenset({
    "codex",
    "chatgpt-desktop",
    "gemini-cli",
})


def _maybe_warn_auto_load(client: str, profile: str) -> None:
    """Warn after install when the client auto-loads on every invocation.

    For codex (CLI/Desktop/IDE) and gemini-cli, install means MoneyBin starts on
    every shell launch of that tool. Two sessions on the same profile share one
    DuckDB file: reads coexist with other reads and writes serialize via the
    per-operation lock. A write-mode call fails only when another session holds a
    conflicting lock past the retry window (a long write, or a long read holding
    the read lock); a read-mode call fails only when it lands during a long write.
    Surface this so users can choose paste-only instead.
    """
    if client not in _PER_INVOCATION_CLIENTS:
        return
    surface = (
        "the Codex CLI, Desktop app, IDE extension, and ChatGPT desktop app"
        if client in _CODEX_HOSTED_CLIENTS
        else "every `gemini` invocation"
    )
    typer.echo("", err=True)
    typer.echo(
        f"⚠️  {client} auto-loads MoneyBin on {surface}. Two concurrent "
        f"sessions on profile '{profile}' share one DuckDB file. Writes "
        "serialize and reads usually coexist; a tool call can fail only when "
        "another session holds a conflicting lock past the retry window "
        "(a long write, or a long read for write-mode calls). To opt out of "
        "auto-load, re-run with `mcp install --print` and paste the snippet "
        "manually. See docs/guides/mcp-clients.md.",
        err=True,
    )


def _print_claude_code_launch_hint(config_path: Path) -> None:
    """Tell the user how to launch Claude Code with the generated config.

    Stderr: this hint prints on the `--print` path too, and stdout there is the
    snippet the user pipes to a file or to `jq`.
    """
    import shlex

    typer.echo("", err=True)
    typer.echo("Launch Claude Code with this MCP server only:", err=True)
    typer.echo(
        f"  claude --strict-mcp-config --mcp-config {shlex.quote(str(config_path))}",
        err=True,
    )
    typer.echo("", err=True)
    typer.echo(
        "Or run `make claude-mcp` from the repo to launch with the active profile.",
        err=True,
    )


def _resolve_uv_command() -> str:
    """Absolute path to `uv`, falling back to the bare name.

    macOS clients launched from the GUI (Claude Desktop, Cursor) do not inherit the
    shell's PATH, so a bare `uv` in the config resolves to nothing and the server
    fails to start with an error the user cannot act on. Pin the interpreter we can
    see at install time. If `uv` isn't on our own PATH either, there is nothing to
    resolve — emit it bare and let the client report the failure.
    """
    return shutil.which("uv") or "uv"


def _print_client_notes(client: str) -> None:
    """Print per-client caveats that the snippet itself cannot express.

    To stderr, like every other advisory here: the snippet on stdout is the data,
    and `--print` promises "the exact bytes the command would write" — a note mixed
    into it would break `mcp install --print | jq` and any config the user pipes
    straight to a file.
    """
    if client == "gemini-cli":
        typer.echo("", err=True)
        typer.echo(
            "Note: Gemini CLI's `trust: true` server setting bypasses ALL tool-call "
            "confirmations. MoneyBin deliberately does not set it — the surface "
            "includes write tools (import, categorize, delete), and those should ask "
            "before they act. Add it yourself only if you accept that.",
            err=True,
        )
    if client == "chatgpt-desktop":
        typer.echo("", err=True)
        typer.echo(
            "Note: this writes ~/.codex/config.toml — the ChatGPT desktop app hosts "
            "Codex and shares its MCP configuration, so this same entry also serves "
            "the Codex CLI and IDE extension (installing for `codex` is equivalent). "
            "In ChatGPT, the server appears under Settings → MCP servers; select "
            "Restart there to pick it up. ChatGPT on the WEB cannot see it — it "
            "does not read local Codex config, and reaching it needs a remote MCP "
            "server (M3D).",
            err=True,
        )
    if client == "windsurf":
        # Windsurf silently drops whatever doesn't fit, so a user who never opens
        # the guide would just find MoneyBin unable to do things it can do. The
        # counts come from moneybin.mcp.surface (plain constants, no FastMCP import
        # — resolving them live would make `mcp install` boot the server), and a
        # test asserts them against the live registry so they can't go stale.
        from moneybin.mcp.surface import (  # noqa: PLC0415 — keep it off the CLI cold-start path
            VISIBLE_TOOL_COUNT,
            WINDSURF_ACTIVE_TOOL_CAP,
        )

        overflow = VISIBLE_TOOL_COUNT - WINDSURF_ACTIVE_TOOL_CAP
        typer.echo("", err=True)
        typer.echo(
            f"⚠️  Windsurf (Cascade) holds at most {WINDSURF_ACTIVE_TOOL_CAP} tools "
            f"at a time, across ALL your MCP servers. MoneyBin registers "
            f"{VISIBLE_TOOL_COUNT} and hides none, so this install alone is "
            f"{overflow} over the ceiling — and Windsurf gives no warning when it "
            "drops the overflow; it simply acts as though MoneyBin cannot do things "
            "it can. Open Settings → MCP Servers and disable the tools you don't "
            "need (turning off a namespace you don't use, e.g. investments_* or "
            "tax_*, is the quickest way down). See docs/guides/mcp-clients.md.",
            err=True,
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
                "Fix the JSON manually before running `mcp install` again."
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
        render_or_json(
            build_envelope(data=tools_payload, sensitivity="low"),
            output,
            cli_actor="mcp_list_tools",
        )
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
        render_or_json(
            build_envelope(data=prompts_payload, sensitivity="low"),
            output,
            cli_actor="mcp_list_prompts",
        )
        return

    if not sorted_prompts:
        if not quiet:
            typer.echo("No prompts registered.")
        return

    for prompt in sorted_prompts:
        description = getattr(prompt, "description", None) or ""
        typer.echo(f"  {prompt.name}  {description}")


# ── serve ────────────────────────────────────────────────────────────────────


def _gate_network_transport(transport: str, *, insecure: bool) -> None:
    """Refuse to start a network transport without an explicit insecure opt-in.

    stdio is local-only and always allowed. Every other transport (sse,
    streamable-http) binds a network-reachable port, and MoneyBin has no HTTP
    authentication yet — so an unauthenticated listener exposes all financial
    data to anyone who can reach the port. Without --insecure, exit with a usage
    error that names the risk; with it, emit a loud stderr warning and return so
    the caller can start the server. Keep this gate until authenticated HTTP
    transport ships.
    """
    if transport == "stdio":
        return
    if not insecure:
        logger.error(
            f"❌ Refusing to start the MCP server on transport '{transport}' "
            "without authentication. This opens a network-reachable MCP server "
            "with NO authentication — anyone who can reach the port can read and "
            "write your financial data. MoneyBin has no HTTP authentication yet; "
            "use the default stdio transport (`moneybin mcp serve`) for local AI "
            "clients. To override on a trusted, localhost-only network, re-run "
            "with --insecure."
        )
        raise typer.Exit(2)
    logger.warning(
        f"⚠️  Starting the MCP server on transport '{transport}' with NO "
        "authentication (--insecure). Anyone who can reach this port can read "
        "and write your financial data. Bind to localhost only and never expose "
        "this port to an untrusted network."
    )


def _is_unconfigured() -> bool:
    """True when no profile is resolvable without the interactive wizard.

    The MCP serve path must never trigger the first-run wizard — its stdout
    prompts corrupt the stdio JSON-RPC stream. When this returns True, the
    server boots unconfigured and FirstRunSetupMiddleware handles setup on the
    first tool call. See docs/specs/mcp-first-run-setup.md.
    """
    if _flags.profile is not None:
        return False
    if os.environ.get("MONEYBIN_PROFILE"):
        return False
    return get_default_profile() is None


@app.command("serve")
def serve(
    transport: Annotated[
        str,
        typer.Option(
            "--transport",
            "-t",
            help=(
                "MCP transport. Default: stdio (local stdin/stdout — the supported "
                "path for AI clients). The network transports (sse, streamable-http) "
                "are UNAUTHENTICATED and require --insecure."
            ),
        ),
    ] = "stdio",
    insecure: Annotated[
        bool,
        typer.Option(
            "--insecure",
            help=(
                "Allow an UNAUTHENTICATED network transport (sse/streamable-http). "
                "Opens a port with no authentication — anyone who can reach it can "
                "read and write your financial data. Localhost-only, trusted "
                "networks only. Ignored for stdio."
            ),
        ),
    ] = False,
) -> None:
    """Start the MoneyBin MCP server.

    This launches an MCP server that gives AI assistants full access to
    your financial data in DuckDB — querying, importing, categorizing,
    and budgeting. The server communicates via stdio (standard input/output)
    by default, which is the supported transport for local MCP integrations.

    The network transports (sse, streamable-http) are UNAUTHENTICATED —
    MoneyBin has no HTTP auth yet — so they refuse to start unless you pass
    --insecure, and even then only on a trusted, localhost-only network. Use
    stdio for real AI-client installs (`moneybin mcp install`).

    The server uses the currently active profile to determine which
    database to connect to. With no profile configured, it still boots —
    in an unconfigured mode that defers profile setup to the first tool
    call (see docs/specs/mcp-first-run-setup.md), rather than running the
    interactive wizard (which would corrupt the stdio JSON-RPC stream).

    Examples:
        # Start MCP server with default profile
        moneybin mcp serve

        # Start with specific profile
        moneybin --profile=alice mcp serve

        # Typically invoked by AI clients, not run manually
    """
    # Validate the transport and enforce the auth gate before doing any work —
    # a refused insecure listener must not even import the server stack.
    if transport not in _VALID_TRANSPORTS:
        logger.error(
            f"Invalid transport '{transport}'. Must be one of: {', '.join(_VALID_TRANSPORTS)}"
        )
        raise typer.Exit(2)

    _gate_network_transport(transport, insecure=insecure)

    # Cast validated string to the literal type
    validated_transport: TransportType = transport  # type: ignore[assignment] — validated above

    from moneybin.cli.utils import get_verbose_flag, handle_cli_errors
    from moneybin.mcp.server import check_schema_at_boot, close_db, init_db, mcp
    from moneybin.observability import setup_observability

    # Import resources/prompts to register their decorators with the server.
    # Tools are registered via register_core_tools() in init_db().
    for module in (
        "moneybin.mcp.resources",
        "moneybin.mcp.prompts",
    ):
        importlib.import_module(module)

    # Convert SIGTERM to SystemExit so the finally block runs and we close
    # the DuckDB connection cleanly. Without this, parent-driven shutdowns
    # (Claude Desktop/Code disconnecting via `kill <pid>`) leak the DB FD
    # until OS process teardown.
    def _on_sigterm(_signum: int, _frame: Any) -> None:
        logger.info("MCP server received SIGTERM; shutting down")
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _on_sigterm)

    if _is_unconfigured():
        # No profile yet: boot without resolving one (resolving would run the
        # interactive wizard and corrupt the stdio JSON-RPC stream). Register
        # tools + first-run middleware; setup happens on the first tool call.
        from moneybin.config import register_profile_resolver
        from moneybin.mcp.first_run import FirstRunSetupMiddleware

        # The lazy profile resolver (registered process-wide in main_callback)
        # runs the interactive wizard, which writes to stdout. In unconfigured
        # mode the wizard must be unreachable from EVERY MCP entry point — not
        # just tool calls (guarded by FirstRunSetupMiddleware) but also
        # resource/prompt reads (e.g. moneybin://schema), which reach
        # get_database() → get_settings() directly, bypassing the middleware.
        # Clearing the resolver makes get_settings() raise a clean error
        # instead of prompting; the middleware does the real elicitation-based
        # setup on the first tool call, calling set_current_profile() directly
        # so the happy path never needs the resolver.
        register_profile_resolver(None)
        verbose = get_verbose_flag()
        setup_observability(stream="mcp", verbose=verbose)
        logger.info(
            f"MCP server starting unconfigured "
            f"(transport={transport}); awaiting first-run setup"
        )
        try:
            init_db()
            mcp.add_middleware(FirstRunSetupMiddleware(verbose=verbose))
            mcp.run(transport=validated_transport)
        except KeyboardInterrupt:
            logger.info("MCP server stopped by user")
        finally:
            close_db()
        return

    from moneybin.config import get_current_profile, get_database_path

    db_path = get_database_path()
    setup_observability(
        stream="mcp", verbose=get_verbose_flag(), profile=get_current_profile()
    )
    logger.info(f"Starting MCP server with database: {db_path}")

    try:
        with handle_cli_errors():
            init_db()
            check_schema_at_boot()
        logger.info(f"MCP server starting (transport={transport}, db={db_path})")
        mcp.run(transport=validated_transport)
    except FileNotFoundError as e:
        logger.error(f"Database not found: {e}")
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        logger.info("MCP server stopped by user")
    finally:
        # close_db() flushes metrics internally; no separate call needed.
        close_db()
