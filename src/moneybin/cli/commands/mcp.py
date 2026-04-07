"""MCP server commands for MoneyBin CLI.

This module provides the `moneybin mcp serve` command that starts the
Model Context Protocol server, exposing DuckDB financial data to AI
assistants like Cursor, Claude Desktop, and ChatGPT Desktop.
"""

import importlib
import logging
import os
import subprocess  # noqa: S404
from pathlib import Path
from typing import Annotated, Literal, get_args

import typer

app = typer.Typer(help="MCP server for AI assistant integration")
logger = logging.getLogger(__name__)


def _find_db_processes(db_path: Path) -> list[dict[str, str | int]]:
    """Find processes that have the DuckDB file open, excluding the current process.

    Args:
        db_path: Path to the DuckDB database file.

    Returns:
        List of dicts with keys: pid (int), command (str), cmdline (str).
    """
    own_pid = os.getpid()
    try:
        result = subprocess.run(  # noqa: S603 — lsof with static args, db_path is a validated Path
            ["lsof", "-F", "pcn", str(db_path)],  # noqa: S607
            capture_output=True,
            text=True,
            timeout=5,
        )
    except FileNotFoundError:
        logger.error("❌ lsof not found — cannot inspect file locks")
        return []
    except subprocess.TimeoutExpired:
        logger.error("❌ lsof timed out")
        return []

    if not result.stdout:
        return []

    # lsof -F output: each process block starts with p<pid>, then c<cmd>, then n<file>
    processes: list[dict[str, str | int]] = []
    seen_pids: set[int] = set()
    current_pid: int | None = None
    current_cmd: str = ""

    for line in result.stdout.splitlines():
        if line.startswith("p"):
            current_pid = int(line[1:])
            current_cmd = ""
        elif line.startswith("c") and current_pid is not None:
            current_cmd = line[1:]
        elif (
            line.startswith("n")
            and current_pid is not None
            and current_pid not in seen_pids
        ):
            seen_pids.add(current_pid)
            if current_pid == own_pid:
                continue
            ps_result = subprocess.run(  # noqa: S603 — ps with static args and validated int PID
                ["ps", "-p", str(current_pid), "-o", "args="],  # noqa: S607
                capture_output=True,
                text=True,
            )
            cmdline = ps_result.stdout.strip()
            processes.append({
                "pid": current_pid,
                "command": current_cmd,
                "cmdline": cmdline,
            })

    return processes


# Transport types supported by FastMCP.run()
TransportType = Literal["stdio", "sse", "streamable-http"]
_VALID_TRANSPORTS: tuple[str, ...] = get_args(TransportType)


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
    logger.info("Starting MCP server with database: %s", db_path)

    if transport not in _VALID_TRANSPORTS:
        logger.error(
            "Invalid transport '%s'. Must be one of: %s",
            transport,
            ", ".join(_VALID_TRANSPORTS),
        )
        raise typer.Exit(1)

    # Cast validated string to the literal type
    validated_transport: TransportType = transport  # type: ignore[assignment] — validated above

    try:
        init_db(db_path)
        logger.info("MCP server starting (transport=%s, db=%s)", transport, db_path)
        mcp.run(transport=validated_transport)
    except FileNotFoundError as e:
        logger.error("Database not found: %s", e)
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        logger.info("MCP server stopped by user")
    finally:
        close_db()


@app.command("show")
def show() -> None:
    """Show other processes holding the MoneyBin database file open.

    Useful for diagnosing database lock conflicts. Excludes the current process.

    Examples:
        moneybin mcp show
        moneybin --profile=alice mcp show
    """
    from moneybin.config import get_database_path

    db_path = get_database_path()

    if not db_path.exists():
        logger.info("Database file does not exist yet: %s", db_path)
        return

    processes = _find_db_processes(db_path)

    if not processes:
        logger.info("No other processes have %s open", db_path.name)
        return

    typer.echo(f"Processes holding {db_path} open:\n")
    typer.echo(f"  {'PID':<8} {'COMMAND':<16} ARGS")
    typer.echo(f"  {'-' * 7:<8} {'-' * 15:<16} {'-' * 40}")
    for proc in processes:
        typer.echo(f"  {proc['pid']:<8} {proc['command']:<16} {proc['cmdline']}")


@app.command("kill")
def kill() -> None:
    """Kill other processes holding the MoneyBin database file open.

    Lists any processes with a lock on the database, then asks for confirmation
    before sending SIGTERM to each. Use when `mcp serve` won't start because
    another session is holding the database.

    Examples:
        moneybin mcp kill
        moneybin --profile=alice mcp kill
    """
    import signal

    from moneybin.config import get_database_path

    db_path = get_database_path()

    if not db_path.exists():
        logger.info("Database file does not exist yet: %s", db_path)
        return

    processes = _find_db_processes(db_path)

    if not processes:
        logger.info("No other processes have %s open", db_path.name)
        return

    typer.echo(f"Processes holding {db_path} open:\n")
    typer.echo(f"  {'PID':<8} {'COMMAND':<16} ARGS")
    typer.echo(f"  {'-' * 7:<8} {'-' * 15:<16} {'-' * 40}")
    for proc in processes:
        typer.echo(f"  {proc['pid']:<8} {proc['command']:<16} {proc['cmdline']}")
    typer.echo()

    count = len(processes)
    noun = "process" if count == 1 else "processes"
    if not typer.confirm(f"Send SIGTERM to {count} {noun}?"):
        raise typer.Exit(0)

    killed = 0
    for proc in processes:
        pid = int(proc["pid"])
        try:
            os.kill(pid, signal.SIGTERM)
            logger.info("Sent SIGTERM to PID %d (%s)", pid, proc["command"])
            killed += 1
        except ProcessLookupError:
            logger.warning("⚠️  PID %d already exited", pid)
        except PermissionError:
            logger.error("❌ No permission to kill PID %d (%s)", pid, proc["command"])

    if killed:
        logger.info("✅ Sent SIGTERM to %d %s", killed, noun)
