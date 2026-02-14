"""MCP server commands for MoneyBin CLI.

This module provides the `moneybin mcp serve` command that starts the
Model Context Protocol server, exposing DuckDB financial data to AI
assistants like Cursor, Claude Desktop, and ChatGPT Desktop.
"""

import importlib
import logging
from typing import Annotated, Literal, get_args

import typer

app = typer.Typer(help="MCP server for AI assistant integration")
logger = logging.getLogger(__name__)

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

    This launches an MCP server that gives AI assistants read-only access
    to your financial data in DuckDB. The server communicates via stdio
    (standard input/output) by default, which is the standard transport
    for local MCP integrations.

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
        "moneybin.mcp.resources",
        "moneybin.mcp.prompts",
    ):
        importlib.import_module(module)

    db_path = get_database_path()
    logger.info("Starting MCP server with database: %s", db_path)

    if transport not in _VALID_TRANSPORTS:
        print(
            f"Error: Invalid transport '{transport}'. Must be one of: {', '.join(_VALID_TRANSPORTS)}"
        )
        raise typer.Exit(1)

    # Cast validated string to the literal type
    validated_transport: TransportType = transport  # type: ignore[assignment] — validated above

    try:
        init_db(db_path)
        print(f"MCP server starting (transport={transport}, db={db_path})")
        mcp.run(transport=validated_transport)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        logger.info("MCP server stopped by user")
    finally:
        close_db()
