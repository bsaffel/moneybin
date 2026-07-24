"""Scenario: a mixed-method investment portfolio ties out to hand-computed truth.

One account (``acct_persona``) holding three positions, each electing a
different cost-basis method, proving the whole pipeline (raw manual rows →
transform → lots/gains/holdings) matches ground truth to the cent AND that each
method actually fires (a HIFO position whose realized basis equals FIFO's would
be a silent regression).

Ground truth is derived independently BEFORE running the pipeline (testing.md:
no observe-and-paste), anchored to the verified cost-basis engine unit tests so
the math is already reviewed. All monetary literals are ``Decimal``; equality is
exact (to the cent).

=============================================================================
HAND-DERIVATION (independent of pipeline output)
=============================================================================
Account ``acct_persona``. The engine groups per (account, security); the three
securities are independent ledgers.

--- Security STOCK (equity, method NULL → FIFO default) -----------------------
Mirrors engine unit test ``test_sell_spanning_two_lots_splits_terms_and_
prorates_proceeds``.
  b1: BUY 10 @ 2023-01-01, amount -1000.00  → lot $100/u (LONG at sale)
  b2: BUY 10 @ 2024-06-01, amount -2000.00  → lot $200/u (SHORT at sale)
  s1: SELL -15 @ 2024-07-01, amount 3000.00
FIFO consumes b1 (10u, LONG) fully, then 5u of b2 (SHORT):
  • b1 leg: qty 10, basis 1000.00, proceeds 3000*10/15 = 2000.00, gain 1000.00,
    term LONG (2023-01-01→2024-07-01 = 547 days > 365).
  • b2 leg: qty  5, basis 1000.00 (5/10*2000), proceeds 3000-2000 = 1000.00,
    gain 0.00, term SHORT (2024-06-01→2024-07-01 = 30 days).
Lots after: b1 closed (0u, 0.00); b2 open 5u, basis_remaining 1000.00.
Holding STOCK: qty 5, cost_basis 1000.00, average_cost 1000.00/5 = 200.

--- Security FUND (mutual_fund, method 'average') -----------------------------
Mirrors ``test_average_partial_sell_uses_pooled_basis_and_rescales_pool``.
  b1: BUY 10 @ 2024-01-01, amount -100.00
  b2: BUY 10 @ 2024-02-01, amount -200.00   → pool 20u/$300, avg $15/u
  s1: SELL -5 @ 2024-03-01, amount 90.00
Pooled basis for 5u = 5*$15 = 75.00 (NOT FIFO's 5*$10 = 50.00 — the method proof):
  • one leg: qty 5, basis 75.00, proceeds 90.00, gain 15.00, term SHORT
    (2024-01-01→2024-03-01 = 60 days; oldest lot supplies the holding period).
Pool rescales to 15u/$225: b1 open 5u → 75.00, b2 open 10u → 150.00.
Holding FUND: qty 15, cost_basis 225.00, average_cost 225.00/15 = 15.

--- Security CRYPTO (crypto, method 'hifo') -----------------------------------
Mirrors ``test_hifo_consumes_highest_per_unit_basis_lot_first``.
  b1: BUY 10 @ 2024-01-01, amount -100.00   → $10/u
  b2: BUY 10 @ 2024-02-01, amount -200.00   → $20/u
  s1: SELL -5 @ 2024-03-01, amount 150.00
HIFO consumes the $20/u lot (b2) first (NOT FIFO's $10/u b1 — the method proof):
  • one leg: qty 5, basis 100.00 (NOT FIFO's 50.00), proceeds 150.00, gain 50.00,
    term SHORT (b2 2024-02-01→2024-03-01).
Lots after: b2 open 5u → 100.00; b1 untouched open 10u → 100.00.
Holding CRYPTO: qty 15, cost_basis 200.00, average_cost 200.00/15 = 13.3333333333
  (200/15 = 13.33333... rounded to DECIMAL(28,10)).

--- Per-term realized-gain totals (across all three securities) ---------------
SHORT: rows = STOCK-short (0.00) + FUND (15.00) + CRYPTO (50.00)
  proceeds 1000.00 + 90.00 + 150.00 = 1240.00
  cost_basis 1000.00 + 75.00 + 100.00 = 1175.00
  gain_loss    0.00 + 15.00 + 50.00 =    65.00
LONG:  rows = STOCK-long
  proceeds 2000.00, cost_basis 1000.00, gain_loss 1000.00

--- Valuation (Pillar C.1): ingest -> resolve -> value ------------------------
Three price observations are seeded into raw.security_prices under the
PROVIDER's key, and resolve to canonical securities through accepted
app.security_links bindings — the same chain a Plaid `sync pull` drives.

STOCK (priced):
  binding  plaid_sec_stock -> STOCK
  close    250.00 on 2024-08-01, price_basis 'raw'
  holding  qty 5, cost_basis 1000.00 (derived above)
  market_value    = 5 x 250.00      = 1250.00
  unrealized_gain = 1250.00 - 1000.00 = 250.00
  valuation_status = 'carried_forward' — 2024-08-01 is strictly before today,
  so the close is the most recent one AT OR BEFORE today but not today's.

FUND (negative expectation — an observation that must NOT value the position):
  binding  plaid_sec_fund -> FUND
  close    30.00 on 2024-08-01, price_basis 'split_adjusted'
  core.fct_security_prices admits only price_basis = 'raw', so this
  observation is excluded and FUND reports NO value. Had it leaked through,
  FUND would read 15 x 30.00 = 450.00 — the assertion below fails loudly
  rather than silently valuing a position from an adjusted series.
  market_value = NULL, valuation_status = 'unpriced'

CRYPTO (negative expectation — unresolved provider key):
  close    99.00 on 2024-08-01 under key plaid_sec_unbound, with NO accepted
  binding. prep.stg_security_prices INNER JOINs app.security_links, so the
  observation waits in raw and reaches no position.
  market_value = NULL, valuation_status = 'unpriced'

No position is 'withheld': the withhold clauses key off Plaid holdings /
transaction staging, and this persona is manual-only, so no account is
broker-covered. `days_since_observed` is deliberately NOT asserted exactly —
it is CURRENT_DATE - price_date and grows by one every day the suite runs.
=============================================================================
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import Database
from tests.scenarios._investments_seed import (
    insert_event,
    insert_security,
    insert_security_link,
    insert_security_price,
)
from tests.scenarios._runner import load_shipped_scenario, scenario_env
from tests.scenarios._runner.steps import run_step

_ACCOUNT = "acct_persona"

# Expected open/closed lots keyed by (security_id, acquisition_date):
# (remaining_quantity, cost_basis_remaining, is_open, cost_basis_method, acq_type)
_EXPECTED_LOTS: dict[tuple[str, str], tuple[Decimal, Decimal, bool, str, str]] = {
    ("STOCK", "2023-01-01"): (Decimal("0"), Decimal("0.00"), False, "fifo", "buy"),
    ("STOCK", "2024-06-01"): (Decimal("5"), Decimal("1000.00"), True, "fifo", "buy"),
    ("FUND", "2024-01-01"): (Decimal("5"), Decimal("75.00"), True, "average", "buy"),
    ("FUND", "2024-02-01"): (Decimal("10"), Decimal("150.00"), True, "average", "buy"),
    ("CRYPTO", "2024-01-01"): (Decimal("10"), Decimal("100.00"), True, "hifo", "buy"),
    ("CRYPTO", "2024-02-01"): (Decimal("5"), Decimal("100.00"), True, "hifo", "buy"),
}

# Expected per-term realized-gain totals: term → (proceeds, cost_basis, gain_loss).
_EXPECTED_TERM_TOTALS: dict[str, tuple[Decimal, Decimal, Decimal]] = {
    "short": (Decimal("1240.00"), Decimal("1175.00"), Decimal("65.00")),
    "long": (Decimal("2000.00"), Decimal("1000.00"), Decimal("1000.00")),
}

# Expected holdings keyed by security_id: (quantity, cost_basis, average_cost).
_EXPECTED_HOLDINGS: dict[str, tuple[Decimal, Decimal, Decimal]] = {
    "STOCK": (Decimal("5"), Decimal("1000.00"), Decimal("200")),
    "FUND": (Decimal("15"), Decimal("225.00"), Decimal("15")),
    "CRYPTO": (Decimal("15"), Decimal("200.00"), Decimal("13.3333333333")),
}

# Expected valuation keyed by security_id:
# (market_value, unrealized_gain, valuation_status). Derived in the docstring's
# valuation block — NOT read off the pipeline. A None pair is the load-bearing
# negative: an adjusted series (FUND) and an unresolved provider key (CRYPTO)
# must each leave the position unvalued rather than publishing a number.
_EXPECTED_VALUATION: dict[str, tuple[Decimal | None, Decimal | None, str]] = {
    "STOCK": (Decimal("1250.00"), Decimal("250.00"), "carried_forward"),
    "FUND": (None, None, "unpriced"),
    "CRYPTO": (None, None, "unpriced"),
}

# Method-proof: realized cost basis per security proves HIFO/average actually
# fired (each differs from what FIFO would have produced).
_EXPECTED_REALIZED_BASIS_BY_SECURITY: dict[str, Decimal] = {
    "STOCK": Decimal("2000.00"),  # 1000 (long) + 1000 (short)
    "FUND": Decimal("75.00"),  # pooled $15/u, NOT FIFO's 50.00
    "CRYPTO": Decimal("100.00"),  # HIFO $20/u lot, NOT FIFO's 50.00
}


def _seed_ledger(db: Database) -> None:
    """Seed app.securities (method election) + the raw investment ledger."""
    insert_security(db, security_id="STOCK", name="Acme Corp", security_type="equity")
    insert_security(
        db,
        security_id="FUND",
        name="Index Fund",
        security_type="mutual_fund",
        cost_basis_method="average",
    )
    insert_security(
        db,
        security_id="CRYPTO",
        name="Bitcoin",
        security_type="crypto",
        cost_basis_method="hifo",
    )

    # STOCK (FIFO) — sell spans short + long across two lots.
    insert_event(
        db,
        investment_transaction_id="stock_b1",
        account_id=_ACCOUNT,
        security_id="STOCK",
        type_="buy",
        trade_date="2023-01-01",
        quantity="10",
        amount="-1000.00",
    )
    insert_event(
        db,
        investment_transaction_id="stock_b2",
        account_id=_ACCOUNT,
        security_id="STOCK",
        type_="buy",
        trade_date="2024-06-01",
        quantity="10",
        amount="-2000.00",
    )
    insert_event(
        db,
        investment_transaction_id="stock_s1",
        account_id=_ACCOUNT,
        security_id="STOCK",
        type_="sell",
        trade_date="2024-07-01",
        quantity="-15",
        amount="3000.00",
    )

    # FUND (average cost) — pooled basis.
    insert_event(
        db,
        investment_transaction_id="fund_b1",
        account_id=_ACCOUNT,
        security_id="FUND",
        type_="buy",
        trade_date="2024-01-01",
        quantity="10",
        amount="-100.00",
    )
    insert_event(
        db,
        investment_transaction_id="fund_b2",
        account_id=_ACCOUNT,
        security_id="FUND",
        type_="buy",
        trade_date="2024-02-01",
        quantity="10",
        amount="-200.00",
    )
    insert_event(
        db,
        investment_transaction_id="fund_s1",
        account_id=_ACCOUNT,
        security_id="FUND",
        type_="sell",
        trade_date="2024-03-01",
        quantity="-5",
        amount="90.00",
    )

    # CRYPTO (HIFO) — highest per-unit-basis lot consumed first.
    insert_event(
        db,
        investment_transaction_id="crypto_b1",
        account_id=_ACCOUNT,
        security_id="CRYPTO",
        type_="buy",
        trade_date="2024-01-01",
        quantity="10",
        amount="-100.00",
    )
    insert_event(
        db,
        investment_transaction_id="crypto_b2",
        account_id=_ACCOUNT,
        security_id="CRYPTO",
        type_="buy",
        trade_date="2024-02-01",
        quantity="10",
        amount="-200.00",
    )
    insert_event(
        db,
        investment_transaction_id="crypto_s1",
        account_id=_ACCOUNT,
        security_id="CRYPTO",
        type_="sell",
        trade_date="2024-03-01",
        quantity="-5",
        amount="150.00",
    )


def _seed_prices(db: Database) -> None:
    """Seed the price feed: one usable close plus two that must not value a position.

    Provider keys resolve through app.security_links, so the observation lands
    keyed the way a provider sends it and the pipeline does the binding.
    """
    insert_security_link(
        db, link_id="link_stock", security_id="STOCK", ref_value="plaid_sec_stock"
    )
    insert_security_link(
        db, link_id="link_fund", security_id="FUND", ref_value="plaid_sec_fund"
    )

    # STOCK: a usable raw close — the one observation that values a position.
    insert_security_price(
        db,
        provider_security_key="plaid_sec_stock",
        price_date="2024-08-01",
        close="250.00",
    )
    # FUND: bound, but split_adjusted — core.fct_security_prices admits only 'raw'.
    insert_security_price(
        db,
        provider_security_key="plaid_sec_fund",
        price_date="2024-08-01",
        close="30.00",
        price_basis="split_adjusted",
    )
    # CRYPTO: raw, but the provider key has no accepted binding — staging's INNER
    # JOIN holds it in raw rather than valuing anything.
    insert_security_price(
        db,
        provider_security_key="plaid_sec_unbound",
        price_date="2024-08-01",
        close="99.00",
    )


@pytest.mark.scenarios
@pytest.mark.slow
def test_investments_persona_correctness() -> None:
    """Mixed FIFO/average/HIFO portfolio ties out to hand-computed ground truth."""
    scenario = load_shipped_scenario("investments-persona")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        _seed_ledger(db)
        _seed_prices(db)
        run_step("transform", scenario.setup, db, env=env)

        # --- core.fct_investment_lots ---------------------------------------
        lot_rows = db.execute(
            """
            SELECT security_id, acquisition_date::VARCHAR, remaining_quantity,
                   cost_basis_remaining, is_open, cost_basis_method, acquisition_type
            FROM core.fct_investment_lots
            """
        ).fetchall()
        actual_lots = {
            (str(r[0]), str(r[1])): (r[2], r[3], bool(r[4]), str(r[5]), str(r[6]))
            for r in lot_rows
        }
        assert len(actual_lots) == len(_EXPECTED_LOTS), (
            f"expected {len(_EXPECTED_LOTS)} lots, got {len(actual_lots)}"
        )
        for key, expected in _EXPECTED_LOTS.items():
            actual = actual_lots.get(key)
            assert actual is not None, f"missing lot {key}"
            rem_qty, basis_rem, is_open, method, acq_type = actual
            exp_qty, exp_basis, exp_open, exp_method, exp_type = expected
            assert rem_qty == exp_qty, f"{key} remaining_quantity"
            assert basis_rem == exp_basis, f"{key} cost_basis_remaining"
            assert is_open == exp_open, f"{key} is_open"
            assert method == exp_method, f"{key} cost_basis_method"
            assert acq_type == exp_type, f"{key} acquisition_type"

        # --- core.fct_realized_gains: per-term totals -----------------------
        term_rows = db.execute(
            """
            SELECT term, SUM(proceeds), SUM(cost_basis), SUM(gain_loss)
            FROM core.fct_realized_gains
            GROUP BY term
            """
        ).fetchall()
        actual_terms = {str(r[0]): (r[1], r[2], r[3]) for r in term_rows}
        assert actual_terms.keys() == _EXPECTED_TERM_TOTALS.keys()
        for term, (exp_proc, exp_basis, exp_gain) in _EXPECTED_TERM_TOTALS.items():
            proc, basis, gain = actual_terms[term]
            assert proc == exp_proc, f"{term} proceeds"
            assert basis == exp_basis, f"{term} cost_basis"
            assert gain == exp_gain, f"{term} gain_loss"

        # Method proof: realized basis per security differs from FIFO's number.
        basis_rows = db.execute(
            """
            SELECT security_id, SUM(cost_basis)
            FROM core.fct_realized_gains
            GROUP BY security_id
            """
        ).fetchall()
        actual_basis = {str(r[0]): r[1] for r in basis_rows}
        for sec, expected in _EXPECTED_REALIZED_BASIS_BY_SECURITY.items():
            assert actual_basis[sec] == expected, f"{sec} realized basis (method proof)"

        # --- core.dim_holdings ----------------------------------------------
        holding_rows = db.execute(
            """
            SELECT security_id, quantity, cost_basis, average_cost
            FROM core.dim_holdings
            """
        ).fetchall()
        actual_holdings = {str(r[0]): (r[1], r[2], r[3]) for r in holding_rows}
        assert actual_holdings.keys() == _EXPECTED_HOLDINGS.keys()
        for sec, (exp_qty, exp_basis, exp_avg) in _EXPECTED_HOLDINGS.items():
            qty, basis, avg = actual_holdings[sec]
            assert qty == exp_qty, f"{sec} holding quantity"
            assert basis == exp_basis, f"{sec} holding cost_basis"
            assert isinstance(avg, Decimal), f"{sec} average_cost must be Decimal"
            assert avg == exp_avg, f"{sec} holding average_cost"

        # --- Valuation: ingest -> resolve -> value ---------------------------
        valuation_rows = db.execute(
            """
            SELECT security_id, market_value, unrealized_gain, valuation_status,
                   price_date::VARCHAR, days_since_observed
            FROM core.dim_holdings
            """
        ).fetchall()
        actual_valuation = {
            str(r[0]): (r[1], r[2], str(r[3]), r[4], r[5]) for r in valuation_rows
        }
        assert actual_valuation.keys() == _EXPECTED_VALUATION.keys()
        for sec, (exp_value, exp_gain, exp_status) in _EXPECTED_VALUATION.items():
            value, gain, status, price_date, age = actual_valuation[sec]
            assert status == exp_status, f"{sec} valuation_status"
            assert value == exp_value, f"{sec} market_value"
            assert gain == exp_gain, f"{sec} unrealized_gain"
            if exp_value is None:
                # NULL, never 0.00 — a zero is indistinguishable from a
                # worthless position and silently understates every aggregate.
                assert value is None, f"{sec} unvalued position must be NULL"
                assert price_date is None, f"{sec} unvalued position has no price"
            else:
                assert isinstance(value, Decimal), f"{sec} market_value is Decimal"
                assert price_date == "2024-08-01", f"{sec} price_date"
                # Exact age is CURRENT_DATE-dependent; the invariant is that a
                # carried-forward close is strictly older than today.
                assert age is not None and age > 0, f"{sec} days_since_observed"
