"""PlaidExtractor investment loading: counts, scoping, snapshots, drift."""

from datetime import UTC, date, datetime, timedelta, timezone
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
    assert result.investment_transactions_loaded == 4
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
        ("raw.plaid_investment_transactions", 4),
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
    assert txn == (4, "sync_job-inv-2")  # re-delivery replaced, lineage updated
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
    # itx_drift_1 only; itx_large_qty_1 reconciles within the scaled tolerance
    # (see test_large_quantity_price_rounding_does_not_count_as_drift below).
    assert (REGISTRY.get_sample_value(name) or 0.0) - before == 1.0


def test_large_quantity_price_rounding_does_not_count_as_drift(
    db: Database, sync_data: SyncDataResponse
) -> None:
    """Coarse-precision price rounding at scale must not false-positive as drift.

    itx_large_qty_1: quantity=10000, price=214.55 (2dp) -> gross =
    |10000 * 214.55| = 2,145,500.00. A price rounded to 2dp can hide up to
    $0.005/share of true price, i.e. up to $50.00 of true gross at this
    quantity. amount=2,145,530.00 sits $30.00 off gross -- outside the old
    flat $0.01 tolerance, inside the scaled tolerance
    (0.01 + 10000 * 0.01 / 2 = $50.01). A flat-cent tolerance would
    false-positive this row; the scaled tolerance must not.
    """
    only_large_qty = sync_data.model_copy(deep=True)
    only_large_qty.investment_transactions = [
        txn
        for txn in only_large_qty.investment_transactions
        if txn.investment_transaction_id == "itx_large_qty_1"
    ]
    name = "moneybin_investment_amount_drift_rows_total"
    before = REGISTRY.get_sample_value(name) or 0.0
    _load(db, only_large_qty, job_id="job-large-qty")
    assert (REGISTRY.get_sample_value(name) or 0.0) - before == 0.0


def test_zero_quantity_buy_reaches_reconciliation_and_counts_drift(
    db: Database,
) -> None:
    """quantity=Decimal('0') is falsy but not None -- the guard must not skip it.

    The pre-fix guard (`if not txn.quantity`) silently exempted zero-quantity
    buy/sell rows from reconciliation, since `Decimal("0")` is falsy. The
    fixed guard checks `is None` explicitly, so a zero-quantity buy now
    reaches reconciliation -- and a nonzero amount against zero quantity
    reconciles against nothing, so it must count as drift.
    """
    payload = {
        "accounts": [{"account_id": "acc_1", "institution_name": "Alpha Brokerage"}],
        "transactions": [],
        "balances": [],
        "removed_transactions": [],
        "investment_transactions": [
            {
                "investment_transaction_id": "itx_zero_qty_1",
                "account_id": "acc_1",
                "provider_item_id": "item_1",
                "security_id": "sec_aapl",
                "date": "2026-07-03",
                "quantity": "0",
                "amount": "50.00",
                "price": "10.00",
                "fees": "0",
                "type": "buy",
                "subtype": "buy",
            }
        ],
        "metadata": {
            "job_id": "j-zero-qty",
            "synced_at": "2026-07-08T12:00:00Z",
            "institutions": [
                {
                    "provider_item_id": "item_1",
                    "institution_name": "Alpha Brokerage",
                    "status": "completed",
                }
            ],
        },
    }
    name = "moneybin_investment_amount_drift_rows_total"
    before = REGISTRY.get_sample_value(name) or 0.0
    _load(db, SyncDataResponse.model_validate(payload), job_id="j-zero-qty")
    assert (REGISTRY.get_sample_value(name) or 0.0) - before == 1.0


def test_event_timestamps_land_as_utc_wall_clocks_in_a_non_utc_session(
    db: Database, sync_data: SyncDataResponse
) -> None:
    """A UTC instant must store the UTC wall clock whatever the machine's zone.

    The raw datetime columns are naive TIMESTAMP and staging derives dates
    from them (`transaction_datetime::DATE` -> trade_date;
    `original_purchase_datetime::DATE` -> acquisition_date). Hand DuckDB a
    tz-AWARE value and it rebases the instant into the session zone on
    insert: in America/Los_Angeles a 2026-03-10T01:30Z trade lands as
    2026-03-09 18:30, so trade_date comes out a day early -- misdating
    cost-basis lots, inverting FIFO order between adjacent days, flipping an
    anniversary sale from long to short, and desyncing trade_date from
    holdings_date (a UTC date computed in Python).

    Must go through the loader: inserting a naive string straight into raw
    bypasses the exact coercion under test.
    """
    db.execute("SET TimeZone = 'America/Los_Angeles'")
    payload = sync_data.model_copy(deep=True)
    instant = datetime(2026, 3, 10, 1, 30, tzinfo=UTC)  # 2026-03-09 18:30 in LA
    payload.investment_transactions[0].transaction_datetime = instant
    payload.investment_holdings[0].tax_lots[0].original_purchase_datetime = instant

    _load(db, payload, job_id="job-tz")

    txn = db.execute(
        """
        SELECT transaction_datetime, transaction_datetime::DATE
        FROM raw.plaid_investment_transactions
        WHERE investment_transaction_id = 'itx_buy_1'
        """
    ).fetchone()
    assert txn == (datetime(2026, 3, 10, 1, 30), date(2026, 3, 10))

    lot = db.execute(
        """
        SELECT original_purchase_datetime, original_purchase_datetime::DATE
        FROM raw.plaid_investment_holding_lots
        WHERE account_id = 'acc_1' AND security_id = 'sec_aapl' AND lot_index = 0
        """
    ).fetchone()
    assert lot == (datetime(2026, 3, 10, 1, 30), date(2026, 3, 10))


def test_holdings_date_is_the_utc_date_of_the_sync_instant(
    db: Database, sync_data: SyncDataResponse
) -> None:
    """holdings_date is the snapshot's UTC calendar date, not an offset-local one.

    `int_plaid__opening_positions` compares trade_date (derived in SQL from
    the UTC-wall-clock columns above) against holdings_date. A synced_at
    carrying a non-UTC offset would put the two on different calendars, and
    the bootstrap would synthesize a phantom opening lot for shares an
    in-window buy already accounts for.
    """
    payload = sync_data.model_copy(deep=True)
    # 2026-07-09T01:00+09:00 is 2026-07-08T16:00Z — the UTC date is the 8th.
    payload.metadata.synced_at = datetime(
        2026, 7, 9, 1, 0, tzinfo=timezone(timedelta(hours=9))
    )

    _load(db, payload, job_id="job-tz-holdings")

    rows = db.execute(
        "SELECT DISTINCT holdings_date FROM raw.plaid_investment_holdings"
    ).fetchall()
    assert rows == [(date(2026, 7, 8),)]


def test_investment_transaction_for_an_unknown_account_raises(
    db: Database, sync_data: SyncDataResponse
) -> None:
    """Plaid eventual-consistency drift must fail loudly, as it does for banking.

    An investment transaction whose account_id never arrived in
    sync_data.accounts gets no app.account_links row, so staging's
    COALESCE(al.account_id, r.account_id) falls back to the raw Plaid id and
    core.fct_investment_transactions carries an account_id with no
    core.dim_accounts row.
    """
    payload = sync_data.model_copy(deep=True)
    payload.investment_transactions[0].account_id = "acc_ghost"

    with pytest.raises(ValueError, match="acc_ghost"):
        _load(db, payload, job_id="job-orphan-txn")

    # The guard runs before the first ingest — nothing partial lands.
    row = db.execute(
        "SELECT COUNT(*) FROM raw.plaid_investment_transactions"
    ).fetchone()
    assert row == (0,)


def test_investment_holding_for_an_unknown_account_raises(
    db: Database, sync_data: SyncDataResponse
) -> None:
    """Same orphan drift on the holdings array — orphan core.dim_holdings rows."""
    payload = sync_data.model_copy(deep=True)
    payload.investment_holdings[0].account_id = "acc_ghost"

    with pytest.raises(ValueError, match="acc_ghost"):
        _load(db, payload, job_id="job-orphan-holding")


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
