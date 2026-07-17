"""Unit tests for PlaidExtractor."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.database import Database, sqlmesh_context
from moneybin.extractors.plaid import PlaidExtractor

FIXTURE = Path(__file__).parent / "fixtures" / "plaid_sync_response.yaml"


@pytest.fixture
def sync_data() -> SyncDataResponse:
    with FIXTURE.open() as f:
        payload = yaml.safe_load(f)
    return SyncDataResponse.model_validate(payload)


def test_loader_writes_accounts(db: Database, sync_data: SyncDataResponse) -> None:
    loader = PlaidExtractor(db)
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
    loader = PlaidExtractor(db)
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


def test_loader_writes_extended_plaid_fields(
    db: Database, sync_data: SyncDataResponse
) -> None:
    loader = PlaidExtractor(db)
    loader.load(sync_data, job_id=sync_data.metadata.job_id)

    row = db.execute(
        """
        SELECT original_description, iso_currency_code, authorized_date,
               payment_channel, location_city, location_latitude,
               merchant_entity_id, category_detailed, category_confidence
        FROM raw.plaid_transactions WHERE transaction_id = 'txn_001'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == "SQ *STARBUCKS 1234 SEATTLE WA"
    assert row[1] == "USD"
    assert str(row[2]) == "2026-04-06"
    assert row[3] == "in store"
    assert row[4] == "Seattle"
    assert row[5] == 47.6062
    assert row[6] == "entity_starbucks_001"
    assert row[7] == "FOOD_AND_DRINK_COFFEE"
    assert row[8] == "VERY_HIGH"

    # txn_002 omits the extended fields -> they load as NULL.
    sparse = db.execute(
        "SELECT original_description, location_city FROM raw.plaid_transactions "
        "WHERE transaction_id = 'txn_002'"
    ).fetchone()
    assert sparse == (None, None)


def test_loader_upserts_on_same_transaction_id_and_source_origin(
    db: Database, sync_data: SyncDataResponse
) -> None:
    loader = PlaidExtractor(db)
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
    loader = PlaidExtractor(db)
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
    loader = PlaidExtractor(db)
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
    loader = PlaidExtractor(db)
    loader.load(sync_data, job_id=sync_data.metadata.job_id)

    before = db.execute(
        "SELECT COUNT(*) FROM raw.plaid_transactions WHERE transaction_id = 'txn_001'"
    ).fetchone()
    assert before is not None
    assert before[0] == 1

    deleted = loader.handle_removed_transactions(["txn_001", "nonexistent_id"])
    # Method reports the actual rowcount deleted: txn_001 was present, the
    # nonexistent ID was a no-op, so deleted == 1, not len(removed_ids) == 2.
    assert deleted == 1

    after = db.execute(
        "SELECT COUNT(*) FROM raw.plaid_transactions WHERE transaction_id = 'txn_001'"
    ).fetchone()
    assert after is not None
    assert after[0] == 0


def test_handle_removed_transactions_empty_list_is_noop(db: Database) -> None:
    loader = PlaidExtractor(db)
    assert loader.handle_removed_transactions([]) == 0


def test_loader_writes_balance_currency(
    db: Database, sync_data: SyncDataResponse
) -> None:
    payload = sync_data.model_copy(deep=True)
    payload.balances = [
        b.model_copy(update={"iso_currency_code": "EUR"})
        if b.account_id == "acc_chase_check"
        else b
        for b in payload.balances
    ]

    loader = PlaidExtractor(db)
    loader.load(payload, job_id=payload.metadata.job_id)

    row = db.execute(
        "SELECT iso_currency_code FROM raw.plaid_balances WHERE account_id = 'acc_chase_check'"
    ).fetchone()
    assert row is not None
    assert row[0] == "EUR"

    # The untouched fixture row has no currency captured -> NULL, not a guess.
    other = db.execute(
        "SELECT iso_currency_code FROM raw.plaid_balances WHERE account_id = 'acc_chase_save'"
    ).fetchone()
    assert other is not None
    assert other[0] is None


@pytest.mark.integration
@pytest.mark.slow
def test_loader_writes_balance_unofficial_currency(
    db: Database, sync_data: SyncDataResponse
) -> None:
    """A balance with only unofficial_currency_code must still land non-NULL.

    Plaid reports iso_currency_code and unofficial_currency_code as mutually
    exclusive: an account whose currency Plaid can't map to ISO 4217
    (crypto-adjacent or specialty accounts) reports it only via
    unofficial_currency_code. That must still reach
    core.fct_balances.currency_code via the COALESCE in fct_balances.sql --
    not silently fall back to dim_accounts' USD default (multi-currency.md
    M1K.1 Requirement 1).
    """
    payload = sync_data.model_copy(deep=True)
    payload.balances = [
        b.model_copy(update={"unofficial_currency_code": "USDC"})
        if b.account_id == "acc_chase_check"
        else b
        for b in payload.balances
    ]

    loader = PlaidExtractor(db)
    loader.load(payload, job_id=payload.metadata.job_id)

    raw_row = db.execute(
        "SELECT iso_currency_code, unofficial_currency_code FROM raw.plaid_balances "
        "WHERE account_id = 'acc_chase_check'"
    ).fetchone()
    assert raw_row is not None
    assert raw_row == (None, "USDC")

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)

    core_row = db.execute(
        "SELECT currency_code FROM core.fct_balances WHERE account_id = 'acc_chase_check'"
    ).fetchone()
    assert core_row is not None
    assert core_row[0] == "USDC"
