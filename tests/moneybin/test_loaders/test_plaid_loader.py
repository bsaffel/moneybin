"""Unit tests for PlaidLoader."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.database import Database
from moneybin.loaders.plaid_loader import PlaidLoader

FIXTURE = Path(__file__).parent / "fixtures" / "plaid_sync_response.yaml"


@pytest.fixture
def sync_data() -> SyncDataResponse:
    with FIXTURE.open() as f:
        payload = yaml.safe_load(f)
    return SyncDataResponse.model_validate(payload)


def test_loader_writes_accounts(db: Database, sync_data: SyncDataResponse) -> None:
    loader = PlaidLoader(db)
    result = loader.load(sync_data, job_id=sync_data.metadata.job_id)
    assert result.accounts_loaded == 2

    rows = db.execute(
        """
        SELECT account_id, institution_name, mask, source_file, source_type, source_origin
        FROM raw.plaid_accounts ORDER BY account_id
        """
    ).fetchall()
    assert len(rows) == 2
    assert rows[0] == (
        "acc_chase_check",
        "Chase",
        "1234",
        "sync_550e8400-e29b-41d4-a716-446655440000",
        "plaid",
        "item_chase_abc",
    )


def test_loader_writes_transactions_preserving_plaid_sign(
    db: Database, sync_data: SyncDataResponse
) -> None:
    loader = PlaidLoader(db)
    result = loader.load(sync_data, job_id=sync_data.metadata.job_id)
    assert result.transactions_loaded == 3

    # Fixture has txn_001 with amount 42.50 (expense in Plaid convention).
    # Raw MUST preserve positive sign — the flip happens in staging.
    amount = db.execute(
        "SELECT amount FROM raw.plaid_transactions WHERE transaction_id = 'txn_001'"
    ).fetchone()
    assert amount is not None
    assert amount[0] == Decimal("42.50")

    # txn_002 is payroll: -1500.00 in Plaid (income).
    income = db.execute(
        "SELECT amount FROM raw.plaid_transactions WHERE transaction_id = 'txn_002'"
    ).fetchone()
    assert income is not None
    assert income[0] == Decimal("-1500.00")


def test_loader_upserts_on_same_transaction_id_and_source_origin(
    db: Database, sync_data: SyncDataResponse
) -> None:
    loader = PlaidLoader(db)
    loader.load(sync_data, job_id=sync_data.metadata.job_id)

    # Re-run the same load with a different job_id (different source_file)
    # but the same source_origin (same provider_item_id) — should UPSERT,
    # not duplicate.
    loader.load(sync_data, job_id="different-job-456")

    count = db.execute(
        "SELECT COUNT(*) FROM raw.plaid_transactions WHERE transaction_id = 'txn_001'"
    ).fetchone()
    assert count is not None
    assert count[0] == 1

    # source_file should reflect the latest sync
    sf = db.execute(
        "SELECT source_file FROM raw.plaid_transactions WHERE transaction_id = 'txn_001'"
    ).fetchone()
    assert sf is not None
    assert sf[0] == "sync_different-job-456"


def test_loader_pending_to_posted_transition(
    db: Database, sync_data: SyncDataResponse
) -> None:
    """A pending transaction in sync 1, posted (pending=false) in sync 2 → single row, pending=false."""
    loader = PlaidLoader(db)
    loader.load(sync_data, job_id=sync_data.metadata.job_id)

    # Sync 2: same transaction_id, pending now false
    payload2 = sync_data.model_copy(deep=True)
    payload2.transactions = [
        t.model_copy(update={"pending": False}) if t.transaction_id == "txn_003" else t
        for t in payload2.transactions
    ]
    payload2.metadata = sync_data.metadata.model_copy(update={"job_id": "job-2"})
    loader.load(payload2, job_id="job-2")

    rows = db.execute(
        "SELECT pending FROM raw.plaid_transactions WHERE transaction_id = 'txn_003'"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] is False


def test_loader_writes_balances(db: Database, sync_data: SyncDataResponse) -> None:
    loader = PlaidLoader(db)
    result = loader.load(sync_data, job_id=sync_data.metadata.job_id)
    assert result.balances_loaded == 2

    rows = db.execute(
        """
        SELECT account_id, current_balance, available_balance
        FROM raw.plaid_balances ORDER BY account_id
        """
    ).fetchall()
    assert len(rows) == 2
    assert rows[0] == ("acc_chase_check", Decimal("1234.56"), Decimal("1200.00"))


def test_handle_removed_transactions(db: Database, sync_data: SyncDataResponse) -> None:
    loader = PlaidLoader(db)
    loader.load(sync_data, job_id=sync_data.metadata.job_id)

    before = db.execute(
        "SELECT COUNT(*) FROM raw.plaid_transactions WHERE transaction_id = 'txn_001'"
    ).fetchone()
    assert before is not None
    assert before[0] == 1

    deleted = loader.handle_removed_transactions(["txn_001", "nonexistent_id"])
    assert deleted == 2  # method reports the count of IDs it tried to remove

    after = db.execute(
        "SELECT COUNT(*) FROM raw.plaid_transactions WHERE transaction_id = 'txn_001'"
    ).fetchone()
    assert after is not None
    assert after[0] == 0


def test_handle_removed_transactions_empty_list_is_noop(db: Database) -> None:
    loader = PlaidLoader(db)
    assert loader.handle_removed_transactions([]) == 0
