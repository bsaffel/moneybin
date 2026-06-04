"""Fixtures for report-framework tests: a DB with classified core.* + a view.

``reports_db`` builds the core tables the privacy ``CLASSIFICATION`` registry
covers, seeds a few transactions, and creates ``reports.test_summary`` — a view
whose body references ``core.fct_transactions`` so lineage can derive real
per-column classes (account_id → CRITICAL, SUM(amount) → HIGH, COUNT → LOW).
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from tests.moneybin.db_helpers import create_core_tables_raw


@pytest.fixture
def reports_db(tmp_path: Path) -> Generator[Database, None, None]:
    """A Database with classified core tables and a reports.* test view."""
    store = MagicMock()
    store.get_key.return_value = "test-encryption-key-for-unit-tests"
    db = Database(
        tmp_path / "reports.duckdb",
        secret_store=store,
        no_auto_upgrade=True,
        read_only=False,
    )
    create_core_tables_raw(db.conn)
    db.execute(
        """
        INSERT INTO core.fct_transactions (transaction_id, account_id, amount)
        VALUES ('t1', 'acct_11112222', -30.00),
               ('t2', 'acct_11112222', -20.00),
               ('t3', 'acct_99998888', 100.00)
        """
    )
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute(
        """
        CREATE OR REPLACE VIEW reports.test_summary AS
        SELECT account_id, SUM(amount) AS amount, COUNT(*) AS txn_count
        FROM core.fct_transactions
        GROUP BY account_id
        """
    )
    try:
        yield db
    finally:
        db.close()
