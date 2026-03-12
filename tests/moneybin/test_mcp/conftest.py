"""Shared fixtures for MCP tests.

Provides a single database setup method used across all MCP test modules.
Base reference data (institutions, accounts, balances) is pre-populated;
individual test classes insert their own specific fixture data.
"""

from collections.abc import Generator
from pathlib import Path

import duckdb
import pytest

from moneybin.mcp import server
from moneybin.schema import init_schemas
from tests.moneybin.db_helpers import create_core_tables


@pytest.fixture(autouse=True)
def mcp_db(tmp_path: Path) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Create a DuckDB with schemas and base reference data for MCP tests.

    Creates all table schemas from canonical SQL files and populates
    base reference data that most tests need:

    - 2 institutions (Test Bank, Other Bank)
    - 2 accounts (ACC001 CHECKING, ACC002 SAVINGS)
    - 2 account balances

    Uses a file-backed database so both read-only and read-write connections
    work (matching production behavior). Sets up server._db as a read-only
    connection and server._db_path for get_write_db().
    """
    db_path = tmp_path / "test.duckdb"

    # Use a read-write connection for setup
    setup_conn = duckdb.connect(str(db_path))

    # Create app/raw schemas and tables via production init_schemas
    init_schemas(setup_conn)

    # Core tables are managed by SQLMesh in production; create test-only
    # concrete tables so we can INSERT fixture data directly.
    create_core_tables(setup_conn)

    # -- Base reference data: institutions --
    setup_conn.execute("""
        INSERT INTO raw.ofx_institutions VALUES
        ('Test Bank', '1234', 'test.qfx', '2025-01-01', CURRENT_TIMESTAMP),
        ('Other Bank', '5678', 'other.qfx', '2025-01-01', CURRENT_TIMESTAMP)
    """)

    # -- Base reference data: accounts --
    setup_conn.execute("""
        INSERT INTO core.dim_accounts VALUES
        ('ACC001', '111000025', 'CHECKING', 'Test Bank', '1234', 'ofx',
         'test.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('ACC002', '222000050', 'SAVINGS', 'Other Bank', '5678', 'ofx',
         'other.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)

    # -- Base reference data: account balances --
    setup_conn.execute("""
        INSERT INTO raw.ofx_balances VALUES
        ('ACC001', '2025-06-01', '2025-06-30', 5000.00,
         '2025-06-30', 4800.00, 'test.qfx',
         '2025-01-24', CURRENT_TIMESTAMP),
        ('ACC002', '2025-06-01', '2025-06-30', 15000.00,
         '2025-06-30', 15000.00, 'other.qfx',
         '2025-01-24', CURRENT_TIMESTAMP)
    """)

    setup_conn.close()

    # Set up server state: read-only connection + db_path for write access
    read_conn = duckdb.connect(str(db_path), read_only=True)
    server._db = read_conn  # type: ignore[reportPrivateUsage] — test fixture
    server._db_path = db_path  # type: ignore[reportPrivateUsage] — test fixture
    yield read_conn
    server.close_db()
