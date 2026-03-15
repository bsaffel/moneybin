"""MCP server definition with DuckDB lifecycle management.

This module creates the FastMCP server instance and manages DuckDB connections
used by all tools and resources. The server uses a **read-only connection by
default** for queries, and acquires a short-lived read-write connection only
when write operations (imports, categorization, budgets) are needed.

This allows multiple MCP server instances and other tools (CLI, notebooks)
to read the database concurrently. DuckDB supports unlimited concurrent
read-only connections but only one read-write connection at a time.

Documentation: https://modelcontextprotocol.github.io/python-sdk/
"""

import logging
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import duckdb
from mcp.server.fastmcp import FastMCP

from moneybin.tables import TableRef

logger = logging.getLogger(__name__)


# Global server instance — tools/resources/prompts register against this
mcp = FastMCP(
    "MoneyBin",
    instructions=(
        "MoneyBin is an AI-powered personal finance app. You can import bank "
        "statements (OFX/QFX files), query transactions and accounts, categorize "
        "spending, set budgets, and get financial insights. All data stays local "
        "in a DuckDB database — nothing is sent to any external service.\n\n"
        "IMPORTANT: When categorizing transactions or creating rules/merchant "
        "mappings, always prefer the bulk tools (bulk_categorize, "
        "bulk_create_categorization_rules, bulk_create_merchant_mappings) over "
        "their single-item equivalents. Fetch a batch with a read tool, reason "
        "about all items, then submit the full list in one bulk call."
    ),
)

# Module-level state — set by init_db() before the server starts
_db: duckdb.DuckDBPyConnection | None = None
_db_path: Path | None = None


def get_db_path() -> Path:
    """Get the path to the DuckDB database file.

    Returns:
        The database file path.

    Raises:
        RuntimeError: If the database has not been initialized.
    """
    if _db_path is None:
        raise RuntimeError("DuckDB connection not initialized. Call init_db() first.")
    return _db_path


def get_db() -> duckdb.DuckDBPyConnection:
    """Get the read-only DuckDB connection for queries.

    Returns:
        The active read-only DuckDB connection.

    Raises:
        RuntimeError: If the database has not been initialized.
    """
    if _db is None:
        raise RuntimeError("DuckDB connection not initialized. Call init_db() first.")
    return _db


def refresh_read_connection() -> None:
    """Reopen the read-only connection to pick up changes.

    DuckDB read-only connections get a snapshot at open time. Call this
    after external writes so subsequent reads see the new data.

    Raises:
        RuntimeError: If the database has not been initialized.
    """
    global _db  # noqa: PLW0603 — module-level singleton is intentional

    if _db_path is None:
        raise RuntimeError("DuckDB connection not initialized. Call init_db() first.")

    if _db is not None:
        _db.close()

    _db = duckdb.connect(str(_db_path), read_only=True)
    logger.info("Read-only connection refreshed")


@contextmanager
def get_write_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Open a short-lived read-write connection for write operations.

    Closes the read-only connection, opens a read-write connection, yields
    it, then closes it and reopens the read-only connection. DuckDB does
    not allow mixed read-only and read-write connections to the same file
    in the same process.

    Yields:
        A read-write DuckDB connection.

    Raises:
        RuntimeError: If the database has not been initialized.
    """
    global _db  # noqa: PLW0603 — module-level singleton is intentional

    if _db_path is None:
        raise RuntimeError("DuckDB connection not initialized. Call init_db() first.")

    # Close read-only connection before opening read-write
    if _db is not None:
        _db.close()
        _db = None

    write_conn = duckdb.connect(str(_db_path), read_only=False)
    try:
        yield write_conn
    finally:
        write_conn.close()
        _db = duckdb.connect(str(_db_path), read_only=True)
        logger.info("Read-only connection restored after write")


def table_exists(table: TableRef) -> bool:
    """Check if a table exists in the database.

    Args:
        table: Table reference to check.

    Returns:
        True if the table exists.
    """
    db = get_db()
    try:
        result = db.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [table.schema, table.name],
        ).fetchone()
        return bool(result and result[0] > 0)
    except Exception:
        return False


def _init_schemas(conn: duckdb.DuckDBPyConnection) -> None:
    """Initialize all database schemas and app tables.

    Args:
        conn: Active DuckDB connection.
    """
    from moneybin.schema import init_schemas

    init_schemas(conn)


def init_db(db_path: Path) -> None:
    """Initialize the database and open a read-only connection.

    If the database file does not exist, it will be created and initialized
    with all required schemas (raw, core, app) via a temporary read-write
    connection. The long-lived connection is always read-only.

    Args:
        db_path: Path to the DuckDB database file.

    Raises:
        duckdb.IOException: If the database cannot be opened.
    """
    global _db, _db_path  # noqa: PLW0603 — module-level singleton is intentional

    is_new = not db_path.exists()

    if is_new:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Creating new database: %s", db_path)

    # Initialize schemas via a temporary read-write connection.
    # Runs on every startup (all DDL uses IF NOT EXISTS) so that
    # newly added tables are created in existing databases.
    init_conn = duckdb.connect(str(db_path), read_only=False)
    try:
        _init_schemas(init_conn)
    finally:
        init_conn.close()
    logger.info("Database schemas initialized: %s", db_path)

    # Open long-lived read-only connection
    _db_path = db_path
    _db = duckdb.connect(str(db_path), read_only=True)
    logger.info("DuckDB read-only connection established: %s", db_path)


def close_db() -> None:
    """Close the DuckDB connection if open."""
    global _db, _db_path  # noqa: PLW0603 — module-level singleton is intentional

    if _db is not None:
        _db.close()
        _db = None
        _db_path = None
        try:
            logger.info("DuckDB connection closed")
        except ValueError:
            pass  # stderr already closed during MCP stdio shutdown
