"""Scenario: the 1099-B reconciliation gate (headline M1J.1 correctness test).

A hand-labeled full-tax-year ledger for one security-rich account, exercising
the whole investment taxonomy, must reconcile to the cent: every per-lot
realized gain, the per-term (ST/LT) totals, the ``basis_incomplete`` flag on the
one oversold slice (and only there), the reinvest acquisition + income pairing,
and the income-aggregation double-count trap.

Ground truth is hand-derived BELOW, independently of the pipeline, BEFORE
running it (testing.md: no observe-and-paste). Real-broker 1099-B data replaces
this fixture at the milestone tie-out later. All monetary literals are
``Decimal``; equality is exact (to the cent).

The engine groups per (account, security); the two securities are independent
ledgers within the one account ``acct_1099b``.

=============================================================================
HAND-DERIVATION — Security ACME (equity, FIFO): buys, ST+LT multi-lot sell,
reinvested dividend (lot stays open), 2:1 split, return of capital, dividend.
=============================================================================
Events in trade-date order (the engine sorts within a group):

A1 2022-01-01 BUY 100 @ -2000.00        → L1: 100u, $20/u, basis 2000  (LONG)
A2 2024-01-01 BUY 100 @ -4000.00        → L2: 100u, $40/u, basis 4000  (SHORT)
A3 2024-02-01 REINVEST 20 @ -200.00     → L3: 20u,  $10/u, basis 200   (SHORT,
       acquisition_type='reinvest'); paired income row (dividend +200.00,
       security ACME) shares event_group_id 'grp_acme_ri'. The dividend leg is
       income-only — the engine skips it (no lot, no gain).
A4 2024-03-01 SELL -150 @ 6000.00       → the ST+LT multi-lot disposal.
       FIFO: L1 (100u, LONG) fully, then 50u of L2 (SHORT).
         • L1 leg: qty 100, basis 2000.00, proceeds 6000*100/150 = 4000.00,
           gain 2000.00, term LONG (2022-01-01→2024-03-01 = 790 days).
         • L2 leg: qty  50, basis 2000.00 (50/100*4000), proceeds 6000-4000 =
           2000.00, gain 0.00, term SHORT (2024-01-01→2024-03-01 = 60 days).
       After A4: L1 closed (0u, 0.00); L2 open 50u/2000.00; L3 open 20u/200.00.
A5 2024-06-01 SPLIT (multiplier 2)      → scales OPEN lots ×2, basis preserved;
       closed L1 untouched. L2: 50u→100u (basis 2000). L3: 20u→40u (basis 200).
A6 2024-07-01 RETURN_OF_CAPITAL +140.00 → pro-rata across open lots by qty.
       Open: L2 100u, L3 40u; total 140u ⇒ $1.00/u.
         • L2: 2000.00 - 100*1 = 1900.00
         • L3:  200.00 -  40*1 =  160.00
A7 2024-10-01 DIVIDEND +50.00           → standalone cash dividend (income-only,
       engine skips it; makes the income aggregation non-trivial).

ACME end lots:
  L1 buy      2022-01-01  remaining   0, basis_remaining    0.00  CLOSED
  L2 buy      2024-01-01  remaining 100, basis_remaining 1900.00  OPEN
  L3 reinvest 2024-02-01  remaining  40, basis_remaining  160.00  OPEN
ACME realized gains: L1 leg (LONG, gain 2000.00), L2 leg (SHORT, gain 0.00).
No basis_incomplete on ACME.

=============================================================================
HAND-DERIVATION — Security ZETA (equity, FIFO): the one oversold disposal.
=============================================================================
Mirrors ``test_oversold_emits_zero_basis_incomplete_slice_without_raising``.

Z1 2024-04-01 BUY 10 @ -500.00          → Lz: 10u, $50/u, basis 500  (SHORT)
Z2 2024-08-01 SELL -15 @ 1500.00        → 5u beyond tracked (oversold).
       FIFO: Lz (10u) fully, then 5u unmatched.
         • Lz leg:  qty 10, basis 500.00, proceeds 1500*10/15 = 1000.00,
           gain 500.00, term SHORT, basis_incomplete FALSE.
         • unmatched: qty 5, basis 0.00, proceeds 1500-1000 = 500.00,
           gain 500.00, term SHORT (acq=disposal date → 0 days),
           basis_incomplete TRUE.
       After Z2: Lz closed. No open ZETA lot.

=============================================================================
Realized-gain rows (4 total) and per-term totals:
  #  security  lot        qty  cost_basis  proceeds  gain     term   incomplete
  1  ACME      L1         100     2000.00   4000.00  2000.00  long   False
  2  ACME      L2          50     2000.00   2000.00     0.00  short  False
  3  ZETA      Lz          10      500.00   1000.00   500.00  short  False
  4  ZETA      unmatched    5        0.00    500.00   500.00  short  True

  LONG  totals: proceeds 4000.00, cost_basis 2000.00, gain 2000.00  (1 row)
  SHORT totals: proceeds 3500.00, cost_basis 2500.00, gain 1000.00  (3 rows)
  basis_incomplete TRUE: exactly 1 row (ZETA unmatched); FALSE: 3 rows.

Income aggregation (type IN dividend/interest/capital_gain_distribution):
  reinvested dividend +200.00 (grp_acme_ri) + standalone dividend +50.00
  ⇒ 2 income rows, SUM 250.00. The reinvest ACQUISITION leg (-200.00, type
  'reinvest') is NOT income-typed → the reinvested dividend is counted EXACTLY
  once, never doubled by the acquisition leg (the double-count trap).
=============================================================================
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from moneybin.database import Database
from tests.scenarios._investments_seed import insert_event, insert_security
from tests.scenarios._runner import load_shipped_scenario, scenario_env
from tests.scenarios._runner.steps import run_step

_ACCOUNT = "acct_1099b"
_REINVEST_GROUP = "grp_acme_ri"

# Expected realized-gain rows as a flat list for exact, order-independent
# per-row assertions. ACME A4 emits two matched rows (LONG L1 + SHORT L2); ZETA
# Z2 emits one matched (Lz) + one unmatched (oversold) row.
_EXPECTED_ROWS: list[dict[str, object]] = [
    {
        "disposal_txn_id": "acme_s1",
        "quantity": Decimal("100"),
        "cost_basis": Decimal("2000.00"),
        "proceeds": Decimal("4000.00"),
        "gain_loss": Decimal("2000.00"),
        "term": "long",
        "basis_incomplete": False,
    },
    {
        "disposal_txn_id": "acme_s1",
        "quantity": Decimal("50"),
        "cost_basis": Decimal("2000.00"),
        "proceeds": Decimal("2000.00"),
        "gain_loss": Decimal("0.00"),
        "term": "short",
        "basis_incomplete": False,
    },
    {
        "disposal_txn_id": "zeta_s1",
        "quantity": Decimal("10"),
        "cost_basis": Decimal("500.00"),
        "proceeds": Decimal("1000.00"),
        "gain_loss": Decimal("500.00"),
        "term": "short",
        "basis_incomplete": False,
    },
    {
        "disposal_txn_id": "zeta_s1",
        "quantity": Decimal("5"),
        "cost_basis": Decimal("0.00"),
        "proceeds": Decimal("500.00"),
        "gain_loss": Decimal("500.00"),
        "term": "short",
        "basis_incomplete": True,
    },
]

# term → (proceeds, cost_basis, gain_loss, row_count)
_EXPECTED_TERM_TOTALS: dict[str, tuple[Decimal, Decimal, Decimal, int]] = {
    "long": (Decimal("4000.00"), Decimal("2000.00"), Decimal("2000.00"), 1),
    "short": (Decimal("3500.00"), Decimal("2500.00"), Decimal("1000.00"), 3),
}

# Expected ACME lots keyed by acquisition_date:
# (remaining_quantity, cost_basis_remaining, is_open, acquisition_type)
_EXPECTED_ACME_LOTS: dict[str, tuple[Decimal, Decimal, bool, str]] = {
    "2022-01-01": (Decimal("0"), Decimal("0.00"), False, "buy"),
    "2024-01-01": (Decimal("100"), Decimal("1900.00"), True, "buy"),
    "2024-02-01": (Decimal("40"), Decimal("160.00"), True, "reinvest"),
}


def _seed_ledger(db: Database) -> None:
    """Seed app.securities + the hand-labeled full-year raw investment ledger."""
    insert_security(db, security_id="ACME", name="Acme Corp", security_type="equity")
    insert_security(db, security_id="ZETA", name="Zeta Inc", security_type="equity")

    # --- ACME ledger ---------------------------------------------------------
    insert_event(
        db,
        investment_transaction_id="acme_b1",
        account_id=_ACCOUNT,
        security_id="ACME",
        type_="buy",
        trade_date="2022-01-01",
        quantity="100",
        amount="-2000.00",
    )
    insert_event(
        db,
        investment_transaction_id="acme_b2",
        account_id=_ACCOUNT,
        security_id="ACME",
        type_="buy",
        trade_date="2024-01-01",
        quantity="100",
        amount="-4000.00",
    )
    # Reinvest CONVENIENCE shape: acquisition leg + paired dividend income leg,
    # both carrying the shared event_group_id (unit-covered by
    # test_investment_service.py::TestReinvestPairing::
    # test_reinvest_writes_two_rows_sharing_group_id).
    insert_event(
        db,
        investment_transaction_id="acme_ri_acq",
        account_id=_ACCOUNT,
        security_id="ACME",
        type_="reinvest",
        subtype="dividend",
        event_group_id=_REINVEST_GROUP,
        trade_date="2024-02-01",
        quantity="20",
        amount="-200.00",
    )
    insert_event(
        db,
        investment_transaction_id="acme_ri_income",
        account_id=_ACCOUNT,
        security_id="ACME",
        type_="dividend",
        event_group_id=_REINVEST_GROUP,
        trade_date="2024-02-01",
        amount="200.00",
    )
    insert_event(
        db,
        investment_transaction_id="acme_s1",
        account_id=_ACCOUNT,
        security_id="ACME",
        type_="sell",
        trade_date="2024-03-01",
        quantity="-150",
        amount="6000.00",
    )
    # 2:1 split — multiplier in quantity; price/amount/fees NULL (Decision D6).
    insert_event(
        db,
        investment_transaction_id="acme_split",
        account_id=_ACCOUNT,
        security_id="ACME",
        type_="split",
        trade_date="2024-06-01",
        quantity="2",
    )
    insert_event(
        db,
        investment_transaction_id="acme_roc",
        account_id=_ACCOUNT,
        security_id="ACME",
        type_="return_of_capital",
        trade_date="2024-07-01",
        # return_of_capital is cash-IN (positive amount) — record_event's
        # _AMOUNT_POSITIVE set rejects amount <= 0; the engine abs()es it either
        # way, so the pro-rata basis reduction of 140.00 is unchanged.
        amount="140.00",
    )
    insert_event(
        db,
        investment_transaction_id="acme_div",
        account_id=_ACCOUNT,
        security_id="ACME",
        type_="dividend",
        trade_date="2024-10-01",
        amount="50.00",
    )

    # --- ZETA ledger (the one oversold disposal) -----------------------------
    insert_event(
        db,
        investment_transaction_id="zeta_b1",
        account_id=_ACCOUNT,
        security_id="ZETA",
        type_="buy",
        trade_date="2024-04-01",
        quantity="10",
        amount="-500.00",
    )
    insert_event(
        db,
        investment_transaction_id="zeta_s1",
        account_id=_ACCOUNT,
        security_id="ZETA",
        type_="sell",
        trade_date="2024-08-01",
        quantity="-15",
        amount="1500.00",
    )


@pytest.mark.scenarios
@pytest.mark.slow
def test_investments_1099b_reconciliation() -> None:
    """Full-tax-year ledger reconciles to hand-computed 1099-B ground truth."""
    scenario = load_shipped_scenario("investments-1099b-reconciliation")
    assert scenario is not None

    with scenario_env(scenario) as (db, _tmp, env):
        _seed_ledger(db)
        run_step("transform", scenario.setup, db, env=env)

        # --- core.fct_realized_gains: exact per-row reconciliation ----------
        gain_rows = db.execute(
            """
            SELECT disposal_txn_id, quantity, cost_basis, proceeds, gain_loss,
                   term, basis_incomplete
            FROM core.fct_realized_gains
            """
        ).fetchall()
        assert len(gain_rows) == len(_EXPECTED_ROWS), (
            f"expected {len(_EXPECTED_ROWS)} realized-gain rows, got {len(gain_rows)}"
        )
        actual_sigs = sorted(
            (str(r[0]), r[1], r[2], r[3], r[4], str(r[5]), bool(r[6]))
            for r in gain_rows
        )
        expected_sigs = sorted(
            (
                str(row["disposal_txn_id"]),
                row["quantity"],
                row["cost_basis"],
                row["proceeds"],
                row["gain_loss"],
                str(row["term"]),
                bool(row["basis_incomplete"]),
            )
            for row in _EXPECTED_ROWS
        )
        assert actual_sigs == expected_sigs, "per-lot realized gains mismatch"

        # --- per-term (ST/LT) totals ----------------------------------------
        term_rows = db.execute(
            """
            SELECT term, SUM(proceeds), SUM(cost_basis), SUM(gain_loss), COUNT(*)
            FROM core.fct_realized_gains
            GROUP BY term
            """
        ).fetchall()
        actual_terms = {str(r[0]): (r[1], r[2], r[3], int(r[4])) for r in term_rows}
        assert actual_terms.keys() == _EXPECTED_TERM_TOTALS.keys()
        for term, (exp_p, exp_b, exp_g, exp_n) in _EXPECTED_TERM_TOTALS.items():
            proc, basis, gain, count = actual_terms[term]
            assert proc == exp_p, f"{term} proceeds"
            assert basis == exp_b, f"{term} cost_basis"
            assert gain == exp_g, f"{term} gain_loss"
            assert count == exp_n, f"{term} row count"

        # --- basis_incomplete appears ONLY on the oversold slice ------------
        incomplete = db.execute(
            """
            SELECT security_id, quantity, cost_basis
            FROM core.fct_realized_gains
            WHERE basis_incomplete
            """
        ).fetchall()
        assert len(incomplete) == 1, "exactly one basis_incomplete row expected"
        assert str(incomplete[0][0]) == "ZETA"
        assert incomplete[0][1] == Decimal("5")
        assert incomplete[0][2] == Decimal("0.00")
        complete_count = db.execute(
            "SELECT COUNT(*) FROM core.fct_realized_gains WHERE NOT basis_incomplete"
        ).fetchone()
        assert complete_count is not None and int(complete_count[0]) == 3

        # --- reinvest pairing: both rows exist, share the group id ----------
        pair = db.execute(
            """
            SELECT type, quantity, amount, security_id
            FROM core.fct_investment_transactions
            WHERE event_group_id = ?
            ORDER BY type
            """,
            [_REINVEST_GROUP],
        ).fetchall()
        assert len(pair) == 2, "reinvest must write exactly two paired rows"
        by_type = {str(r[0]): r for r in pair}
        assert by_type.keys() == {"reinvest", "dividend"}
        # Acquisition leg: positive qty, negative amount, carries the security.
        assert by_type["reinvest"][1] == Decimal("20")
        assert by_type["reinvest"][2] == Decimal("-200.00")
        assert str(by_type["reinvest"][3]) == "ACME"
        # Income leg: NULL qty, positive amount, carries the security.
        assert by_type["dividend"][1] is None
        assert by_type["dividend"][2] == Decimal("200.00")
        assert str(by_type["dividend"][3]) == "ACME"

        # --- reinvest opens a lot (acquisition_type='reinvest') -------------
        reinvest_lots = db.execute(
            """
            SELECT acquisition_date::VARCHAR, original_quantity, acquisition_type
            FROM core.fct_investment_lots
            WHERE acquisition_type = 'reinvest'
            """
        ).fetchall()
        assert len(reinvest_lots) == 1, "reinvest must open exactly one lot"
        assert str(reinvest_lots[0][0]) == "2024-02-01"

        # --- ACME lots end-state (split + RoC applied in place) -------------
        acme_lot_rows = db.execute(
            """
            SELECT acquisition_date::VARCHAR, remaining_quantity,
                   cost_basis_remaining, is_open, acquisition_type
            FROM core.fct_investment_lots
            WHERE security_id = 'ACME'
            """
        ).fetchall()
        actual_acme = {
            str(r[0]): (r[1], r[2], bool(r[3]), str(r[4])) for r in acme_lot_rows
        }
        assert actual_acme.keys() == _EXPECTED_ACME_LOTS.keys()
        for acq_date, (
            exp_qty,
            exp_basis,
            exp_open,
            exp_type,
        ) in _EXPECTED_ACME_LOTS.items():
            qty, basis, is_open, acq_type = actual_acme[acq_date]
            assert qty == exp_qty, f"ACME {acq_date} remaining_quantity"
            assert basis == exp_basis, f"ACME {acq_date} cost_basis_remaining"
            assert is_open == exp_open, f"ACME {acq_date} is_open"
            assert acq_type == exp_type, f"ACME {acq_date} acquisition_type"

        # --- income aggregation: reinvested dividend counted EXACTLY once ----
        income_rows = db.execute(
            """
            SELECT amount
            FROM core.fct_investment_transactions
            WHERE type IN ('dividend', 'interest', 'capital_gain_distribution')
            """
        ).fetchall()
        income_amounts = sorted(r[0] for r in income_rows)
        # Two income rows only: reinvested dividend (200) + standalone (50).
        # The reinvest acquisition leg (-200) is NOT income-typed → not counted.
        assert income_amounts == [Decimal("50.00"), Decimal("200.00")], (
            "income-typed rows must count the reinvested dividend exactly once"
        )
        assert sum(income_amounts, Decimal("0")) == Decimal("250.00")
