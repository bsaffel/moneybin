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

_SCHEMA_DIR = (
    Path(__file__).resolve().parents[3] / "src" / "moneybin" / "sql" / "schema"
)

_SCHEMA_FILES = [
    "raw_schema.sql",
    "core_schema.sql",
    "raw_ofx_institutions.sql",
    "raw_ofx_accounts.sql",
    "raw_ofx_transactions.sql",
    "raw_ofx_balances.sql",
    "raw_w2_forms.sql",
    "core_dim_accounts.sql",
    "core_fct_transactions.sql",
]


@pytest.fixture(autouse=True)
def mcp_db(tmp_path: Path) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Create a DuckDB with schemas and base reference data for MCP tests.

    Creates all table schemas from canonical SQL files and populates
    base reference data that most tests need:

    - 2 institutions (Test Bank, Other Bank)
    - 2 accounts (ACC001 CHECKING, ACC002 SAVINGS)
    - 2 account balances

    Yields a writable connection so test classes can insert additional
    fixture data via class-level autouse fixtures.
    """
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))

    # Create schemas and tables from canonical SQL files
    for sql_file in _SCHEMA_FILES:
        conn.execute((_SCHEMA_DIR / sql_file).read_text())

    # -- Base reference data: institutions --
    conn.execute("""
        INSERT INTO raw.ofx_institutions VALUES
        ('Test Bank', '1234', 'test.qfx', '2025-01-01', CURRENT_TIMESTAMP),
        ('Other Bank', '5678', 'other.qfx', '2025-01-01', CURRENT_TIMESTAMP)
    """)

    # -- Base reference data: accounts --
    conn.execute("""
        INSERT INTO core.dim_accounts VALUES
        ('ACC001', '111000025', 'CHECKING', 'Test Bank', '1234', 'ofx',
         'test.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('ACC002', '222000050', 'SAVINGS', 'Other Bank', '5678', 'ofx',
         'other.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)

    # -- Base reference data: account balances --
    conn.execute("""
        INSERT INTO raw.ofx_balances VALUES
        ('ACC001', '2025-06-01', '2025-06-30', 5000.00,
         '2025-06-30', 4800.00, 'test.qfx',
         '2025-01-24', CURRENT_TIMESTAMP),
        ('ACC002', '2025-06-01', '2025-06-30', 15000.00,
         '2025-06-30', 15000.00, 'other.qfx',
         '2025-01-24', CURRENT_TIMESTAMP)
    """)

    server._db = conn  # type: ignore[reportPrivateUsage] — test fixture
    yield conn
    server.close_db()
