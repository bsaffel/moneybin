"""Database exploration commands for MoneyBin CLI.

This module provides commands for interacting with the DuckDB database,
including opening the web UI and running SQL queries.
"""

import logging
import shutil
import subprocess  # noqa: S404
import sys
from pathlib import Path

import typer

from moneybin.config import get_database_path

app = typer.Typer(help="Database exploration and query commands")
logger = logging.getLogger(__name__)


def _check_duckdb_cli() -> str | None:
    """Check if DuckDB CLI is available and return its path.

    Returns:
        str | None: Path to DuckDB CLI executable, or None if not found
    """
    return shutil.which("duckdb")


@app.command("ui")
def open_ui(
    database: Path | None = typer.Option(
        None,
        "--database",
        "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
) -> None:
    """Open DuckDB web UI to explore and query your financial data.

    This command launches the DuckDB web interface in your browser,
    automatically using the database file from your current profile.

    The web UI provides:
    - Interactive SQL query editor
    - Table and schema browser
    - Query results visualization
    - Database statistics

    Examples:
        # Open UI for current profile's database
        moneybin db ui

        # Open UI for specific database file
        moneybin --profile=alice db ui
        moneybin db ui --database data/custom.duckdb
    """
    # Determine database path
    if database is None:
        database = get_database_path()
        logger.info(f"Using database from profile: {database}")
    else:
        logger.info(f"Using specified database: {database}")

    # Check if database file exists
    if not database.exists():
        logger.error(f"❌ Database file not found: {database}")
        logger.info("💡 Run 'moneybin load' to create and populate the database first")
        raise typer.Exit(1)

    # Check if DuckDB CLI is available
    duckdb_path = _check_duckdb_cli()
    if duckdb_path is None:
        logger.error("❌ DuckDB CLI not found in PATH")
        logger.info("💡 The DuckDB CLI is separate from the Python package")
        logger.info("   Install it from: https://duckdb.org/docs/installation/")
        logger.info("   Or via Homebrew: brew install duckdb")
        raise typer.Exit(1)

    try:
        logger.info("🚀 Opening DuckDB web UI...")
        logger.info("   Press Ctrl+C to stop the server")

        # Run duckdb with -ui flag (httpfs extension is optional)
        cmd = ["duckdb", str(database), "-ui"]

        # Run with output to terminal so user sees the URL
        subprocess.run(cmd, check=True)  # noqa: S603

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ DuckDB UI failed to start: {e}")
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        logger.info("\n✅ DuckDB UI stopped")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Failed to start DuckDB UI: {e}")
        raise typer.Exit(1) from e


@app.command("query")
def run_query(
    sql: str = typer.Argument(..., help="SQL query to execute"),
    database: Path | None = typer.Option(
        None,
        "--database",
        "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
    output_format: str = typer.Option(
        "table",
        "--format",
        "-f",
        help="Output format: table, csv, json, markdown, box",
    ),
) -> None:
    """Execute a SQL query against the DuckDB database.

    This is a convenience wrapper around the DuckDB CLI that automatically
    uses your profile's database file.

    Examples:
        # Query account balances
        moneybin db query "SELECT * FROM raw_ofx_accounts LIMIT 10"

        # Export to CSV
        moneybin db query "SELECT * FROM fct_transactions" --format csv > output.csv

        # Query specific profile's database
        moneybin --profile=alice db query "SELECT COUNT(*) FROM raw_ofx_transactions"
    """
    # Determine database path
    if database is None:
        database = get_database_path()

    # Check if database file exists
    if not database.exists():
        logger.error(f"❌ Database file not found: {database}")
        logger.info("💡 Run 'moneybin load' to create and populate the database first")
        raise typer.Exit(1)

    # Check if DuckDB CLI is available
    duckdb_path = _check_duckdb_cli()
    if duckdb_path is None:
        logger.error("❌ DuckDB CLI not found in PATH")
        logger.info("💡 Install from: https://duckdb.org/docs/installation/")
        raise typer.Exit(1)

    try:
        # Build command with output format
        cmd = ["duckdb", str(database), "-c", sql]

        # Add output format flag
        format_map = {
            "table": "-table",
            "csv": "-csv",
            "json": "-json",
            "markdown": "-markdown",
            "box": "-box",
        }

        if output_format in format_map:
            cmd.append(format_map[output_format])
        else:
            logger.warning(
                f"⚠️  Unknown format '{output_format}', using default table format"
            )

        # Run query and stream output
        subprocess.run(cmd, check=True)  # noqa: S603

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Query failed: {e}")
        raise typer.Exit(1) from e
    except Exception as e:
        logger.error(f"❌ Failed to execute query: {e}")
        raise typer.Exit(1) from e


@app.command("shell")
def open_shell(
    database: Path | None = typer.Option(
        None,
        "--database",
        "-d",
        help="Path to DuckDB database file (default: profile config)",
    ),
) -> None:
    """Open an interactive DuckDB SQL shell.

    This launches the DuckDB CLI in interactive mode, allowing you to
    run multiple queries and explore your data.

    Examples:
        # Open shell for current profile
        moneybin db shell

        # Open shell for specific database
        moneybin --profile=alice db shell
    """
    # Determine database path
    if database is None:
        database = get_database_path()
        logger.info(f"Using database from profile: {database}")
    else:
        logger.info(f"Using specified database: {database}")

    # Check if database file exists
    if not database.exists():
        logger.error(f"❌ Database file not found: {database}")
        logger.info("💡 Run 'moneybin load' to create and populate the database first")
        raise typer.Exit(1)

    # Check if DuckDB CLI is available
    duckdb_path = _check_duckdb_cli()
    if duckdb_path is None:
        logger.error("❌ DuckDB CLI not found in PATH")
        logger.info("💡 Install from: https://duckdb.org/docs/installation/")
        raise typer.Exit(1)

    try:
        logger.info("🦆 Opening DuckDB interactive shell...")
        logger.info("   Type .help for commands, .quit to exit")

        # Run duckdb in interactive mode
        cmd = ["duckdb", str(database)]
        subprocess.run(cmd, check=True)  # noqa: S603

    except subprocess.CalledProcessError as e:
        logger.error(f"❌ DuckDB shell failed: {e}")
        raise typer.Exit(1) from e
    except KeyboardInterrupt:
        logger.info("\n✅ DuckDB shell closed")
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ Failed to open DuckDB shell: {e}")
        raise typer.Exit(1) from e
