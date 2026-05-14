"""SQL tests for prep.stg_plaid__* staging views.

The sign-flip test is load-bearing — it locks in the sole place Plaid's
positive-is-expense convention is reversed to MoneyBin's negative-is-expense.
Any other code path doing this flip is a bug.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.database import Database, sqlmesh_context
from moneybin.loaders.plaid_loader import PlaidLoader

FIXTURE = (
    Path(__file__).parent / "test_loaders" / "fixtures" / "plaid_sync_response.yaml"
)


@pytest.fixture
def db_with_data(db: Database) -> Database:
    with FIXTURE.open() as f:
        sync_data = SyncDataResponse.model_validate(yaml.safe_load(f))
    loader = PlaidLoader(db)
    loader.load(sync_data, job_id=sync_data.metadata.job_id)
    return db


@pytest.mark.slow
def test_dim_accounts_includes_plaid(db_with_data: Database) -> None:
    """Plaid accounts appear in core.dim_accounts with source_type='plaid'."""
    with sqlmesh_context(db_with_data) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    rows = db_with_data.execute(
        """
        SELECT account_id, source_type
        FROM core.dim_accounts
        WHERE source_type = 'plaid'
        ORDER BY account_id
        """
    ).fetchall()
    assert len(rows) == 2
    assert {r[0] for r in rows} == {"acc_chase_check", "acc_chase_save"}


@pytest.mark.slow
def test_stg_plaid_transactions_flips_sign(db_with_data: Database) -> None:
    """Raw amount 42.50 (Plaid expense) → staging -42.50 (MoneyBin expense)."""
    with sqlmesh_context(db_with_data) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    # Expense (Plaid positive) → MoneyBin negative
    row = db_with_data.execute(
        "SELECT amount FROM prep.stg_plaid__transactions WHERE transaction_id = 'txn_001'"
    ).fetchone()
    assert row is not None
    assert row[0] == Decimal("-42.50")

    # Income (Plaid negative) → MoneyBin positive
    row = db_with_data.execute(
        "SELECT amount FROM prep.stg_plaid__transactions WHERE transaction_id = 'txn_002'"
    ).fetchone()
    assert row is not None
    assert row[0] == Decimal("1500.00")


@pytest.mark.slow
def test_fct_transactions_includes_plaid_with_correct_sign(
    db_with_data: Database,
) -> None:
    """Plaid transactions in core.fct_transactions use MoneyBin sign convention."""
    with sqlmesh_context(db_with_data) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    row = db_with_data.execute(
        """
        SELECT amount FROM core.fct_transactions
        WHERE source_type = 'plaid' AND description LIKE '%STARBUCKS%'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == Decimal("-42.50")

    row = db_with_data.execute(
        """
        SELECT amount FROM core.fct_transactions
        WHERE source_type = 'plaid' AND description LIKE '%PAYROLL%'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == Decimal("1500.00")
