"""Fixtures for report-framework tests: a DB with classified core.* + a view.

``reports_db`` builds the core tables the privacy ``CLASSIFICATION`` registry
covers, seeds a few transactions, and creates ``reports.test_summary`` — a view
whose body references ``core.fct_transactions`` so lineage can derive real
per-column classes (account_id → CRITICAL, SUM(amount) → HIGH, COUNT → LOW).

``saved_db`` is the same shape over the shared ``db`` fixture, which carries the
full ``app`` schema — so a report can actually be saved. Both the user-tier
tests and R7's execution-parity tests need it.
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


@pytest.fixture
def saved_db(db: Database) -> Database:
    """The shared schema (including ``app.user_reports``) plus classified core rows."""
    create_core_tables_raw(db.conn)
    db.execute(
        """
        INSERT INTO core.dim_accounts
            (account_id, routing_number, institution_name, display_name)
        VALUES ('acct_11112222', '021000021', 'Test Bank', 'Checking'),
               ('acct_99998888', '026009593', 'Other Bank', 'Savings')
        """
    )
    db.execute(
        """
        INSERT INTO core.fct_transactions (transaction_id, account_id, amount)
        VALUES ('t1', 'acct_11112222', -30.00),
               ('t2', 'acct_99998888', 100.00)
        """
    )
    # A `reports.*` view no `@report` declares. `reports_class_map()` has never
    # heard of it, so its columns are the honest unresolvable case — the same
    # fail-closed answer a package-contributed view would get today.
    db.execute(
        """
        CREATE OR REPLACE VIEW reports.test_summary AS
        SELECT account_id, SUM(amount) AS amount, COUNT(*) AS txn_count
        FROM core.fct_transactions
        GROUP BY account_id
        """
    )
    return db
