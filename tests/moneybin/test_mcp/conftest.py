"""Shared fixtures for MCP tests.

Provides a single database setup method used across all MCP test modules.
Base reference data (institutions, accounts, balances) is pre-populated;
individual test classes insert their own specific fixture data.
"""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from tests.moneybin.db_helpers import create_core_tables_raw


@pytest.fixture()
def mcp_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Create a Database instance with schemas and base reference data for MCP tests.

    Creates all table schemas from canonical SQL files and populates
    base reference data that most tests need:

    - 2 institutions (Test Bank, Other Bank)
    - 2 accounts (ACC001 CHECKING, ACC002 SAVINGS)
    - 2 account balances

    Injects the Database instance into the moneybin.database singleton so
    all MCP server functions (get_db, get_db_path, table_exists) resolve
    against this test database automatically.
    """
    db_path = tmp_path / "test.duckdb"

    # Build a mock SecretStore that returns a fixed key so we can open
    # the encrypted database in tests.
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"

    database = Database(db_path, secret_store=mock_store, no_auto_upgrade=True)
    conn = database.conn

    # Core tables are managed by SQLMesh in production; create test-only
    # concrete tables so we can INSERT fixture data directly.
    create_core_tables_raw(conn)

    # -- Base reference data: institutions --
    conn.execute("""
        INSERT INTO raw.ofx_institutions
            (organization, fid, source_file, extracted_at, loaded_at, import_id, source_type)
        VALUES
        ('Test Bank', '1234', 'test.qfx', '2025-01-01', CURRENT_TIMESTAMP, NULL, 'ofx'),
        ('Other Bank', '5678', 'other.qfx', '2025-01-01', CURRENT_TIMESTAMP, NULL, 'ofx')
    """)

    # -- Base reference data: accounts --
    conn.execute("""
        INSERT INTO core.dim_accounts
            (account_id, routing_number, account_type, institution_name, institution_fid,
             source_type, source_file, extracted_at, loaded_at, updated_at)
        VALUES
        ('ACC001', '111000025', 'CHECKING', 'Test Bank', '1234', 'ofx',
         'test.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('ACC002', '222000050', 'SAVINGS', 'Other Bank', '5678', 'ofx',
         'other.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)

    # -- Base reference data: account balances --
    conn.execute("""
        INSERT INTO raw.ofx_balances
            (account_id, statement_start_date, statement_end_date, ledger_balance,
             ledger_balance_date, available_balance, source_file,
             extracted_at, loaded_at, import_id, source_type)
        VALUES
        ('ACC001', '2025-06-01', '2025-06-30', 5000.00,
         '2025-06-30', 4800.00, 'test.qfx',
         '2025-01-24', CURRENT_TIMESTAMP, NULL, 'ofx'),
        ('ACC002', '2025-06-01', '2025-06-30', 15000.00,
         '2025-06-30', 15000.00, 'other.qfx',
         '2025-01-24', CURRENT_TIMESTAMP, NULL, 'ofx')
    """)

    # Inject the Database singleton so all MCP server functions use this DB
    db_module._database_instance = database  # type: ignore[reportPrivateUsage] — test fixture

    yield database

    # Teardown: close and clear the singleton so subsequent tests get a fresh one
    db_module._database_instance = None  # type: ignore[reportPrivateUsage] — test fixture
    database.close()
