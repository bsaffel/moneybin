"""Shared fixtures for MCP tests.

`_mcp_db_template` builds the baseline encrypted DuckDB once per session
(core tables + base reference data). `mcp_db` then copies the file into
each test's tmp_path so every test gets an isolated database without
re-running the schema DDL or 6 baseline INSERTs.

Base reference data:
- 2 institutions (Test Bank, Other Bank)
- 2 accounts (ACC001 CHECKING, ACC002 SAVINGS)
- 2 account balances
"""

from __future__ import annotations

import shutil
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from tests.moneybin.db_helpers import create_core_tables_raw

_MOCK_KEY = "test-encryption-key-256bit-placeholder"


def _make_mock_store() -> MagicMock:
    store = MagicMock()
    store.get_key.return_value = _MOCK_KEY
    return store


@pytest.fixture(scope="session")
def _mcp_db_template(  # pyright: ignore[reportUnusedFunction]  # pytest fixture referenced by parameter name
    tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    """Build the baseline encrypted DB once per session and return its path."""
    template_dir = tmp_path_factory.mktemp("mcp_db_template")
    template_path = template_dir / "template.duckdb"

    database = Database(
        template_path, secret_store=_make_mock_store(), no_auto_upgrade=True
    )
    conn = database.conn
    create_core_tables_raw(conn)

    conn.execute("""
        INSERT INTO raw.ofx_institutions
            (organization, fid, source_file, extracted_at, loaded_at, import_id, source_type)
        VALUES
        ('Test Bank', '1234', 'test.qfx', '2025-01-01', CURRENT_TIMESTAMP, NULL, 'ofx'),
        ('Other Bank', '5678', 'other.qfx', '2025-01-01', CURRENT_TIMESTAMP, NULL, 'ofx')
    """)

    conn.execute("""
        INSERT INTO core.dim_accounts VALUES
        ('ACC001', '111000025', 'CHECKING', 'Test Bank', '1234', 'ofx',
         'test.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
        ('ACC002', '222000050', 'SAVINGS', 'Other Bank', '5678', 'ofx',
         'other.qfx', '2025-01-01', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
    """)

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

    database.close()
    return template_path


@pytest.fixture()
def mcp_db(tmp_path: Path, _mcp_db_template: Path) -> Generator[Database, None, None]:
    """Per-test Database initialized from a snapshot of the session template.

    Copies the baseline encrypted DuckDB file into the test's tmp_path,
    opens it, and injects the singleton so MCP server functions resolve
    against this DB. Restores the singleton on teardown.
    """
    db_path = tmp_path / "test.duckdb"
    shutil.copy(_mcp_db_template, db_path)

    database = Database(db_path, secret_store=_make_mock_store(), no_auto_upgrade=True)

    db_module._database_instance = database  # type: ignore[reportPrivateUsage] — test fixture
    try:
        yield database
    finally:
        db_module._database_instance = None  # type: ignore[reportPrivateUsage] — test fixture
        database.close()
