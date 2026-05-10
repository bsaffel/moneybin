"""Shared helpers for transaction-curation CLI tests.

Provides ``make_curation_db`` to build a Database with core + app tables and
one seeded transaction (T1), and ``patch_db`` to redirect
``moneybin.cli.utils.get_database`` at that database.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from tests.moneybin.db_helpers import create_core_tables_raw


def make_curation_db(tmp_path: Path) -> Database:
    """Build a curation Database with core tables, A1 account, and T1 txn."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "curation.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    create_core_tables_raw(database.conn)
    database.conn.execute(
        "INSERT INTO core.dim_accounts (account_id) VALUES (?)", ["A1"]
    )
    database.conn.execute(
        """
        INSERT INTO core.fct_transactions (
            transaction_id, account_id, transaction_date, amount,
            amount_absolute, transaction_direction, description,
            transaction_type, is_pending, currency_code, source_type,
            source_extracted_at, loaded_at,
            transaction_year, transaction_month, transaction_day,
            transaction_day_of_week, transaction_year_month,
            transaction_year_quarter
        ) VALUES (
            'T1', 'A1', '2026-04-10', -100.00, 100.00, 'expense',
            'Test Txn', 'DEBIT', false, 'USD', 'ofx',
            '2026-04-10', CURRENT_TIMESTAMP,
            2026, 4, 10, 3, '2026-04', '2026-Q2'
        )
        """  # noqa: S608  # test input, not executing SQL
    )
    return database


def patch_db(monkeypatch: pytest.MonkeyPatch, database: Database) -> None:
    """Patch ``moneybin.cli.utils.get_database`` to return ``database``."""
    monkeypatch.setattr("moneybin.cli.utils.get_database", lambda: database)
