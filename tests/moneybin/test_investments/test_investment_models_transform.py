"""Integration test: the investment SQLMesh models built by TransformService.

Drives the real engine<->SQLMesh integration end-to-end: insert a buy + a
partial sell into raw.manual_investment_transactions, run the transform, and
assert core.fct_investment_lots, core.fct_realized_gains, and core.dim_holdings
carry the exact Decimal basis/proceeds/gain the cost-basis engine produces.

Correctness is to the cent (1099-B). The buy quantity carries EIGHTEEN significant
figures (12345678.9012345678) — more than float64's ~15-16 — specifically to catch
the float64 trap: str(float(12345678.9012345678)) == "12345678.901234567" LOSES a
digit, so if the loader dropped its ::VARCHAR casts and routed the DECIMAL(28,10)
through float, original_quantity/remaining_quantity would no longer equal the exact
Decimals asserted here and this test would go RED. (An 11-sig-fig quantity would
round-trip through float exactly and hide the regression.)

Expected values are hand-derived from the two input rows (FIFO, the default when
no method is elected):

- Buy 12345678.9012345678 units, |amount| = 1000.00 basis -> cost_basis_total 1000.00
- Sell 6172839.4506172839 units (exactly 1/2 of the lot) for 800.00 proceeds
    - basis consumed = 1000.00 * (6172839.4506172839 / 12345678.9012345678) = 500.00
    - gain = 800.00 - 500.00 = 300.00, held 2024-01-01..2024-07-01 (182d) -> short
    - lot remaining = 12345678.9012345678 - 6172839.4506172839 = 6172839.4506172839
    - lot cost_basis_remaining = 1000.00 - 500.00 = 500.00
- Holding = the one open lot: quantity 6172839.4506172839, cost_basis 500.00,
  average_cost = 500.00 / 6172839.4506172839 = 0.0000810000 exactly at
  DECIMAL(28,10) (the true quotient 0.00008100000072900... rounds to scale 10).
  The model casts the WHOLE division to DECIMAL(28,10); without that cast
  DuckDB's decimal `/` promotes to DOUBLE and this comes back as a float
  8.100000072900001e-05 (a database.md "no FLOAT for financial quantities"
  violation) — the exact-Decimal + isinstance assertions below lock that shut.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.services.transform_service import TransformService

pytestmark = pytest.mark.integration

_ACCOUNT_ID = "acct_inv_1"
_SECURITY_ID = "sec_inv_1"
# MAX over the two ledger rows' created_at -> the updated_at every derived row
# inherits (core-updated-at convention: MAX of inputs, never CURRENT_TIMESTAMP).
_SELL_CREATED_AT = datetime(2024, 7, 2, 10, 0, 0)


def _insert_security(db: Database) -> None:
    db.execute(
        """
        INSERT INTO app.securities (security_id, name, security_type, currency_code)
        VALUES (?, 'Test Security', 'equity', 'USD')
        """,  # noqa: S608  # test fixture, not executing user SQL
        [_SECURITY_ID],
    )


def _insert_investment_txn(
    db: Database,
    *,
    source_txn_id: str,
    investment_txn_id: str,
    txn_type: str,
    trade_date: str,
    quantity: str,
    amount: str,
    created_at: str,
) -> None:
    """Seed one manual investment row (decimals/dates cast to their target types)."""
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions
            (source_transaction_id, import_id, account_id, security_id, type,
             trade_date, quantity, amount, fees, currency_code,
             created_at, created_by, investment_transaction_id)
        VALUES (?, 'import_test', ?, ?, ?, ?::DATE,
                ?::DECIMAL(28,10), ?::DECIMAL(18,2), 0::DECIMAL(18,2), 'USD',
                ?::TIMESTAMP, 'cli', ?)
        """,  # noqa: S608  # test fixture, not executing user SQL
        [
            source_txn_id,
            _ACCOUNT_ID,
            _SECURITY_ID,
            txn_type,
            trade_date,
            quantity,
            amount,
            created_at,
            investment_txn_id,
        ],
    )


@pytest.mark.slow
def test_transform_builds_investment_lots_gains_and_holdings(db: Database) -> None:
    """A buy + partial sell must materialize exact lots, gains, and holdings."""
    _insert_security(db)
    _insert_investment_txn(
        db,
        source_txn_id="manual_buy_1",
        investment_txn_id="inv_buy_1",
        txn_type="buy",
        trade_date="2024-01-01",
        quantity="12345678.9012345678",
        amount="-1000.00",
        created_at="2024-01-01 09:00:00",
    )
    _insert_investment_txn(
        db,
        source_txn_id="manual_sell_1",
        investment_txn_id="inv_sell_1",
        txn_type="sell",
        trade_date="2024-07-01",
        quantity="6172839.4506172839",
        amount="800.00",
        created_at="2024-07-02 10:00:00",
    )

    result = TransformService(db).apply()
    assert result.applied, f"transform apply failed: {result.error}"

    # --- core.fct_investment_lots -------------------------------------------
    lots = db.execute(
        """
        SELECT original_quantity, remaining_quantity, cost_basis_total,
               cost_basis_remaining, cost_basis_method, acquisition_type,
               is_open, acquisition_date, updated_at
        FROM core.fct_investment_lots
        """
    ).fetchall()
    assert len(lots) == 1, f"expected one lot, got {lots}"
    (
        original_quantity,
        remaining_quantity,
        cost_basis_total,
        cost_basis_remaining,
        cost_basis_method,
        acquisition_type,
        is_open,
        acquisition_date,
        lot_updated_at,
    ) = lots[0]
    # Exact-Decimal equality on an 18-sig-fig quantity proves no float64 round-trip
    # corrupted it: str(float(12345678.9012345678)) drops a digit, so removing the
    # loader's ::VARCHAR casts would fail these assertions.
    assert original_quantity == Decimal("12345678.9012345678")
    assert remaining_quantity == Decimal("6172839.4506172839")
    assert cost_basis_total == Decimal("1000.00")
    assert cost_basis_remaining == Decimal("500.00")
    assert cost_basis_method == "fifo"
    assert acquisition_type == "buy"
    assert is_open is True
    assert str(acquisition_date) == "2024-01-01"
    assert lot_updated_at == _SELL_CREATED_AT

    # --- core.fct_realized_gains --------------------------------------------
    gains = db.execute(
        """
        SELECT quantity, proceeds, cost_basis, gain_loss, term,
               basis_incomplete, cost_basis_method, updated_at
        FROM core.fct_realized_gains
        """
    ).fetchall()
    assert len(gains) == 1, f"expected one realized gain, got {gains}"
    (
        gain_quantity,
        proceeds,
        cost_basis,
        gain_loss,
        term,
        basis_incomplete,
        gain_method,
        gain_updated_at,
    ) = gains[0]
    assert gain_quantity == Decimal("6172839.4506172839")
    assert proceeds == Decimal("800.00")
    assert cost_basis == Decimal("500.00")
    assert gain_loss == Decimal("300.00")
    assert term == "short"
    assert basis_incomplete is False
    assert gain_method == "fifo"
    assert gain_updated_at == _SELL_CREATED_AT

    # --- core.dim_holdings ---------------------------------------------------
    holdings = db.execute(
        """
        SELECT quantity, cost_basis, average_cost, currency_code, updated_at
        FROM core.dim_holdings
        """
    ).fetchall()
    assert len(holdings) == 1, f"expected one holding, got {holdings}"
    quantity, holding_basis, average_cost, currency_code, holding_updated_at = holdings[
        0
    ]
    assert quantity == Decimal("6172839.4506172839")
    assert holding_basis == Decimal("500.00")
    # average_cost is DECIMAL(28,10), never DOUBLE: a DOUBLE column comes back as
    # a Python float, so isinstance(Decimal) guards the type promotion the model's
    # whole-division cast prevents. The value is hand-derived: 500.00 /
    # 6172839.4506172839 = 0.00008100000072900... → 0.0000810000 rounded to scale
    # 10 (NOT observe-and-paste — independently computed, per testing.md).
    assert isinstance(average_cost, Decimal), (
        f"average_cost must be DECIMAL (Python Decimal), got {type(average_cost).__name__}"
    )
    assert average_cost == Decimal("0.0000810000")
    assert currency_code == "USD"
    assert holding_updated_at == _SELL_CREATED_AT
