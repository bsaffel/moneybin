"""PlaidExtractor investment loading: counts, scoping, snapshots, drift."""

from decimal import Decimal
from pathlib import Path

import pytest
import yaml
from prometheus_client import REGISTRY

from moneybin.connectors.sync_models import SyncDataResponse
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor

FIXTURE = Path(__file__).parent / "fixtures" / "plaid_investments_sync_response.yaml"


@pytest.fixture
def sync_data() -> SyncDataResponse:
    with FIXTURE.open() as f:
        return SyncDataResponse.model_validate(yaml.safe_load(f))


def _load(db: Database, sync_data: SyncDataResponse, job_id: str = "job-inv-1"):
    return PlaidExtractor(db).load(sync_data, job_id=job_id)


def test_loads_all_three_arrays(db: Database, sync_data: SyncDataResponse) -> None:
    result = _load(db, sync_data)
    assert result.securities_loaded == 3
    assert result.investment_transactions_loaded == 3
    assert result.holdings_loaded == 3
    assert result.holding_lots_loaded == 2


def test_investment_transaction_payload_values_preserved_verbatim(
    db: Database, sync_data: SyncDataResponse
) -> None:
    """amount/quantity/price land unaltered — no sign flip, no all-NULL column.

    A `-1 *` on amount, or a typo'd schema key silently producing an
    all-NULL column, would still pass a counts-only assertion. These are
    exact fixture values, not derived from the code under test.
    """
    _load(db, sync_data)
    buy = db.execute(
        """
        SELECT amount, quantity, price
        FROM raw.plaid_investment_transactions
        WHERE investment_transaction_id = 'itx_buy_1'
        """
    ).fetchone()
    # Plaid-positive amount (cash out) stored positive — verbatim, not negated.
    assert buy == (Decimal("2145.50"), Decimal("10.0"), Decimal("214.55"))

    cash = db.execute(
        """
        SELECT amount, quantity, price
        FROM raw.plaid_investment_transactions
        WHERE investment_transaction_id = 'itx_cash_1'
        """
    ).fetchone()
    # Plaid-negative amount (cash in) stored negative — verbatim, not negated.
    assert cash == (Decimal("-500.00"), None, None)


def test_holdings_and_lots_payload_values_preserved(
    db: Database, sync_data: SyncDataResponse
) -> None:
    """cost_basis and lot fields land unaltered — an all-NULL column fails this."""
    _load(db, sync_data)
    holding = db.execute(
        """
        SELECT cost_basis, quantity
        FROM raw.plaid_investment_holdings
        WHERE account_id = 'acc_1' AND security_id = 'sec_aapl'
        """
    ).fetchone()
    assert holding == (Decimal("1980.00"), Decimal("10.0"))

    lot_with_institution_id = db.execute(
        """
        SELECT institution_lot_id, quantity, purchase_price, cost_basis
        FROM raw.plaid_investment_holding_lots
        WHERE account_id = 'acc_1' AND security_id = 'sec_aapl' AND lot_index = 0
        """
    ).fetchone()
    assert lot_with_institution_id == (
        "lot_7f",
        Decimal("6.0"),
        Decimal("121.00"),
        Decimal("726.00"),
    )

    lot_without_institution_id = db.execute(
        """
        SELECT institution_lot_id, quantity, cost_basis, position_type
        FROM raw.plaid_investment_holding_lots
        WHERE account_id = 'acc_1' AND security_id = 'sec_aapl' AND lot_index = 1
        """
    ).fetchone()
    assert lot_without_institution_id == (
        None,
        Decimal("4.0"),
        Decimal("1254.00"),
        "long",
    )


def test_same_payload_reload_is_idempotent(
    db: Database, sync_data: SyncDataResponse
) -> None:
    _load(db, sync_data)
    _load(db, sync_data)
    for table, expected in [
        ("raw.plaid_securities", 3),
        ("raw.plaid_investment_transactions", 3),
        ("raw.plaid_investment_holdings", 3),
        ("raw.plaid_investment_holding_lots", 2),
    ]:
        row = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608  # fixed table list
        assert row is not None and row[0] == expected, table


def test_new_job_replaces_transactional_and_retains_snapshots(
    db: Database, sync_data: SyncDataResponse
) -> None:
    _load(db, sync_data, job_id="job-inv-1")
    _load(db, sync_data, job_id="job-inv-2")
    txn = db.execute(
        "SELECT COUNT(*), MAX(source_file) FROM raw.plaid_investment_transactions"
    ).fetchone()
    assert txn == (3, "sync_job-inv-2")  # re-delivery replaced, lineage updated
    snap = db.execute(
        "SELECT COUNT(DISTINCT source_file), COUNT(*) FROM raw.plaid_investment_holdings"
    ).fetchone()
    assert snap == (2, 6)  # both snapshots retained
    lots_snap = db.execute(
        "SELECT COUNT(DISTINCT source_file), COUNT(*) "
        "FROM raw.plaid_investment_holding_lots"
    ).fetchone()
    assert lots_snap == (2, 4)  # both snapshots retained (2 lots/snapshot)


def test_window_start_stamped_per_item(
    db: Database, sync_data: SyncDataResponse
) -> None:
    _load(db, sync_data)
    rows = db.execute(
        """
        SELECT DISTINCT source_origin, transactions_window_start
        FROM raw.plaid_investment_holdings ORDER BY source_origin
        """
    ).fetchall()
    assert [(r[0], str(r[1])) for r in rows] == [
        ("item_1", "2024-07-08"),
        ("item_2", "2025-01-15"),
    ]


def test_colliding_provider_ids_stay_distinct(
    db: Database, sync_data: SyncDataResponse
) -> None:
    _load(db, sync_data)
    row = db.execute(
        """
        SELECT COUNT(*) FROM raw.plaid_investment_holdings
        WHERE account_id = 'acc_dup' AND security_id = 'sec_dup'
        """
    ).fetchone()
    assert row is not None and row[0] == 2  # one per item, never conflated


def test_missing_window_start_raises(db: Database, sync_data: SyncDataResponse) -> None:
    broken = sync_data.model_copy(deep=True)
    broken.metadata.institutions[0].transactions_window_start = None
    with pytest.raises(ValueError, match="transactions_window_start"):
        _load(db, broken)


def test_drift_guard_counts_unreconcilable_rows(
    db: Database, sync_data: SyncDataResponse
) -> None:
    name = "moneybin_investment_amount_drift_rows_total"
    before = REGISTRY.get_sample_value(name) or 0.0
    _load(db, sync_data)
    assert (REGISTRY.get_sample_value(name) or 0.0) - before == 1.0  # itx_drift_1 only


def test_empty_arrays_load_cleanly(db: Database) -> None:
    payload = {
        "accounts": [],
        "transactions": [],
        "balances": [],
        "removed_transactions": [],
        "metadata": {
            "job_id": "j0",
            "synced_at": "2026-07-08T12:00:00Z",
            "institutions": [],
        },
    }
    result = _load(db, SyncDataResponse.model_validate(payload), job_id="j0")
    assert result.securities_loaded == 0
    assert result.investment_transactions_loaded == 0
    assert result.holdings_loaded == 0
    assert result.holding_lots_loaded == 0
