"""Unit tests for the cost-basis engine (FIFO, HIFO, specific, average).

Correctness-critical: these tests pin the consumption orders, penny
conservation, deterministic IDs, ST/LT split (per-lot, independent of
consumption order), average-cost pooling, and oversold handling that a 1099-B
reconciliation later depends on. All monetary literals are ``Decimal`` — never
float — because float rounding would defeat the penny-conservation guarantees
under test.
"""

import hashlib
from collections.abc import Callable
from datetime import date
from decimal import Decimal

import pytest

from moneybin.investments.cost_basis import (
    LedgerEvent,
    Lot,
    RealizedGain,
    compute_lots_and_gains,
)

pytestmark = pytest.mark.unit

D = Decimal


def _fifo(_account_id: str, _security_id: str) -> str:
    return "fifo"


def _hifo(_account_id: str, _security_id: str) -> str:
    return "hifo"


def _no_selections(_disposal_txn_id: str) -> list[tuple[str, Decimal]]:
    return []


def _specific(_account_id: str, _security_id: str) -> str:
    return "specific"


def _average(_account_id: str, _security_id: str) -> str:
    return "average"


def _event(
    txn_id: str,
    *,
    event_type: str,
    trade_date: date,
    account_id: str = "acct1",
    security_id: str | None = "sec1",
    original_acquisition_date: date | None = None,
    quantity: Decimal | None = None,
    price: Decimal | None = None,
    amount: Decimal | None = None,
    fees: Decimal | None = None,
    currency_code: str | None = "USD",
) -> LedgerEvent:
    return LedgerEvent(
        investment_transaction_id=txn_id,
        account_id=account_id,
        security_id=security_id,
        trade_date=trade_date,
        original_acquisition_date=original_acquisition_date,
        type=event_type,
        quantity=quantity,
        price=price,
        amount=amount,
        fees=fees,
        currency_code=currency_code,
    )


def _run(
    events: list[LedgerEvent],
    method_for: Callable[[str, str], str] = _fifo,
    selections_for: Callable[[str], list[tuple[str, Decimal]]] = _no_selections,
) -> tuple[list[Lot], list[RealizedGain]]:
    lots, gains = compute_lots_and_gains(
        events,
        method_for=method_for,
        selections_for=selections_for,
    )
    return list(lots), gains


def test_single_buy_opens_one_lot_basis_is_abs_amount() -> None:
    # amount already folds in the 4.95 fee: basis = |amount|, NOT |amount| + fees.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            price=D("150"),
            amount=D("-1504.95"),
            fees=D("4.95"),
        )
    ]
    lots, gains = _run(events)

    assert gains == []
    assert len(lots) == 1
    lot = lots[0]
    assert lot.acquisition_type == "buy"
    assert lot.acquisition_date == date(2024, 1, 1)
    assert lot.original_quantity == D("10")
    assert lot.remaining_quantity == D("10")
    assert lot.cost_basis_total == D("1504.95")
    assert lot.cost_basis_remaining == D("1504.95")
    assert lot.cost_basis_method == "fifo"


def test_two_buys_partial_sell_consumes_oldest_first() -> None:
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-2000.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-5"),
            amount=D("600.00"),
        ),
    ]
    lots, gains = _run(events)

    # Oldest lot (b1) partially consumed; b2 untouched.
    by_src = {lot.source_transaction_id: lot for lot in lots}
    b1 = by_src["b1"]
    b2 = by_src["b2"]
    assert b1.remaining_quantity == D("5")
    assert b1.cost_basis_remaining == D("500.00")
    assert b2.remaining_quantity == D("10")
    assert b2.cost_basis_remaining == D("2000.00")

    assert len(gains) == 1
    g = gains[0]
    assert g.lot_id == b1.lot_id
    assert g.quantity == D("5")
    assert g.cost_basis == D("500.00")
    assert g.proceeds == D("600.00")
    assert g.gain_loss == D("100.00")
    assert g.term == "short"
    assert g.basis_incomplete is False


def test_sell_spanning_two_lots_splits_terms_and_prorates_proceeds() -> None:
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2023, 1, 1),  # long-term at sale
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 6, 1),  # short-term at sale
            quantity=D("10"),
            amount=D("-2000.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 7, 1),
            quantity=D("-15"),
            amount=D("3000.00"),
        ),
    ]
    lots, gains = _run(events)

    assert len(gains) == 2
    long_leg = next(g for g in gains if g.term == "long")
    short_leg = next(g for g in gains if g.term == "short")

    # b1 fully consumed (long, basis taken whole); 5 of b2 consumed (short).
    assert long_leg.quantity == D("10")
    assert long_leg.cost_basis == D("1000.00")
    assert long_leg.proceeds == D("2000.00")
    assert long_leg.gain_loss == D("1000.00")

    assert short_leg.quantity == D("5")
    assert short_leg.cost_basis == D("1000.00")
    assert short_leg.proceeds == D("1000.00")
    assert short_leg.gain_loss == D("0.00")

    # Proceeds penny-conserved across the two legs.
    assert sum((g.proceeds for g in gains), D("0")) == D("3000.00")

    # A multi-lot disposal must emit a distinct realized_gain_id per lot.
    ids = [g.realized_gain_id for g in gains]
    assert len(ids) == len(set(ids))

    by_src = {lot.source_transaction_id: lot for lot in lots}
    assert by_src["b1"].remaining_quantity == D("0")
    assert by_src["b2"].remaining_quantity == D("5")
    assert by_src["b2"].cost_basis_remaining == D("1000.00")


def test_exactly_365_days_is_short() -> None:
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2023, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 1, 1),  # exactly 365 days later
            quantity=D("-10"),
            amount=D("1200.00"),
        ),
    ]
    _lots, gains = _run(events)
    assert (date(2024, 1, 1) - date(2023, 1, 1)).days == 365
    assert len(gains) == 1
    assert gains[0].term == "short"


def test_366_days_is_long() -> None:
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2023, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 1, 2),  # 366 days later
            quantity=D("-10"),
            amount=D("1200.00"),
        ),
    ]
    _lots, gains = _run(events)
    assert (date(2024, 1, 2) - date(2023, 1, 1)).days == 366
    assert len(gains) == 1
    assert gains[0].term == "long"


def test_leap_year_one_year_hold_is_short() -> None:
    # Bug guard: a lot held exactly one calendar year whose span crosses a leap
    # day (Feb 29 2020) is 366 days, but the IRS "more than one year" rule makes
    # the exact one-year anniversary SHORT-term. A day-count `> 365` heuristic
    # wrongly classified it long, applying the wrong capital-gains rate.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2020, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2021, 1, 1),  # exact one-year anniversary
            quantity=D("-10"),
            amount=D("1200.00"),
        ),
    ]
    _lots, gains = _run(events)
    assert (date(2021, 1, 1) - date(2020, 1, 1)).days == 366  # leap-day span
    assert len(gains) == 1
    assert gains[0].term == "short"


def test_leap_day_acquisition_long_term_boundary_is_march_1() -> None:
    # Feb 29 has no calendar anniversary; convention (anniversary treated as the
    # non-leap Feb 28): long-term on/after Mar 1 of the following year. Also
    # proves the calendar helper does not raise on a Feb 29 acquisition.
    def _term_when_sold(day: date) -> str:
        _lots, gains = _run([
            _event(
                "b1",
                event_type="buy",
                trade_date=date(2020, 2, 29),
                quantity=D("10"),
                amount=D("-1000.00"),
            ),
            _event(
                "s1",
                event_type="sell",
                trade_date=day,
                quantity=D("-10"),
                amount=D("1200.00"),
            ),
        ])
        return gains[0].term

    assert _term_when_sold(date(2021, 2, 28)) == "short"
    assert _term_when_sold(date(2021, 3, 1)) == "long"


def test_same_day_split_applies_before_same_day_buy() -> None:
    # Bug guard: a same-day corporate action and acquisition must apply in a
    # deterministic, economically-ordered sequence — NOT arbitrary content-hash
    # order. Convention: a split takes effect at the ex-date (start of day),
    # before same-day trades, so a pre-existing lot doubles but a same-day buy
    # (already at post-split prices) does NOT. The ids are chosen so the buggy
    # content-hash sort would place the buy ("a_buy") before the split
    # ("z_split") and wrongly double the buy's new lot.
    events = [
        _event(
            "b_pre",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "a_buy",
            event_type="buy",
            trade_date=date(2024, 3, 1),
            quantity=D("10"),
            amount=D("-2000.00"),
        ),
        _event(
            "z_split",
            event_type="split",
            trade_date=date(2024, 3, 1),
            quantity=D("2"),  # 2:1 multiplier
        ),
    ]
    lots, _gains = _run(events)
    by_src = {lot.source_transaction_id: lot for lot in lots}
    # Pre-existing lot is doubled by the split.
    assert by_src["b_pre"].remaining_quantity == D("20")
    # Same-day buy processed AFTER the split is not doubled.
    assert by_src["a_buy"].remaining_quantity == D("10")


def test_transfer_in_carries_original_date_and_basis_transfer_out_no_gains() -> None:
    events = [
        _event(
            "ti",
            event_type="transfer_in",
            trade_date=date(2024, 5, 1),
            original_acquisition_date=date(2020, 1, 1),
            quantity=D("10"),
            amount=D("-5000.00"),
        ),
        _event(
            "to",
            event_type="transfer_out",
            trade_date=date(2024, 6, 1),
            quantity=D("-4"),
            amount=None,
        ),
    ]
    lots, gains = _run(events)

    assert gains == []  # transfer_out yields no realized-gain rows
    assert len(lots) == 1
    lot = lots[0]
    # Holding period transfers with the shares — original date, not trade date.
    assert lot.acquisition_date == date(2020, 1, 1)
    assert lot.acquisition_type == "transfer_in"
    assert lot.cost_basis_total == D("5000.00")
    # transfer_out of 4 of 10 consumes lots without proceeds.
    assert lot.remaining_quantity == D("6")
    assert lot.cost_basis_remaining == D("3000.00")


def test_transfer_in_without_original_date_uses_trade_date() -> None:
    events = [
        _event(
            "ti",
            event_type="transfer_in",
            trade_date=date(2024, 5, 1),
            original_acquisition_date=None,
            quantity=D("10"),
            amount=D("-5000.00"),
        ),
    ]
    lots, _gains = _run(events)
    assert lots[0].acquisition_date == date(2024, 5, 1)


def test_cash_only_and_null_security_events_are_ignored() -> None:
    events = [
        _event(
            "d1",
            event_type="deposit",
            trade_date=date(2024, 1, 1),
            security_id=None,
            amount=D("1000.00"),
        ),
        _event(
            "div",
            event_type="dividend",
            trade_date=date(2024, 2, 1),
            security_id=None,
            amount=D("25.00"),
        ),
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 3, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
    ]
    lots, gains = _run(events)
    assert len(lots) == 1
    assert gains == []


def test_unhandled_type_is_skipped_without_crashing() -> None:
    # split / return_of_capital / reinvest are modeled as of Task 12; any type
    # the engine still doesn't model (e.g. a security-bearing 'fee' or 'other')
    # must be skipped, never raised on, and must not touch open lots.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "fee1",
            event_type="fee",
            trade_date=date(2024, 2, 1),
            amount=D("-9.99"),
        ),
        _event(
            "ot1",
            event_type="other",
            trade_date=date(2024, 3, 1),
            quantity=D("1"),
        ),
    ]
    lots, gains = _run(events)
    assert len(lots) == 1  # only the buy opened a lot
    assert lots[0].remaining_quantity == D("10")
    assert lots[0].cost_basis_remaining == D("1000.00")
    assert gains == []


def test_deterministic_ids_across_runs() -> None:
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-5"),
            amount=D("600.00"),
        ),
    ]
    lots1, gains1 = _run(events)
    lots2, gains2 = _run(events)

    assert [lot.lot_id for lot in lots1] == [lot.lot_id for lot in lots2]
    assert [g.realized_gain_id for g in gains1] == [g.realized_gain_id for g in gains2]

    # Pin the exact documented formula so later tasks can't drift it.
    expected_lot_id = (
        "lot_"
        + hashlib.sha256(
            f"acct1|sec1|{date(2024, 1, 1).isoformat()}|b1".encode()
        ).hexdigest()[:16]
    )
    assert lots1[0].lot_id == expected_lot_id
    expected_rg_id = (
        "rg_" + hashlib.sha256(f"s1|{expected_lot_id}".encode()).hexdigest()[:16]
    )
    assert gains1[0].realized_gain_id == expected_rg_id


def test_oversold_emits_zero_basis_incomplete_slice_without_raising() -> None:
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 2, 1),
            quantity=D("-15"),  # 5 more than tracked
            amount=D("1500.00"),
        ),
    ]
    lots, gains = _run(events)

    assert len(gains) == 2
    matched = [g for g in gains if not g.basis_incomplete]
    unmatched = [g for g in gains if g.basis_incomplete]
    assert len(matched) == 1
    assert len(unmatched) == 1

    assert matched[0].quantity == D("10")
    assert matched[0].cost_basis == D("1000.00")
    assert matched[0].basis_incomplete is False

    assert unmatched[0].quantity == D("5")
    assert unmatched[0].cost_basis == D("0.00")
    assert unmatched[0].basis_incomplete is True
    # acquisition_date == disposal_date => 0 days => short (conservative).
    assert unmatched[0].acquisition_date == date(2024, 2, 1)
    assert unmatched[0].term == "short"

    # Proceeds still penny-conserved across matched + unmatched.
    assert sum((g.proceeds for g in gains), D("0")) == D("1500.00")

    # Lot fully drained.
    assert lots[0].remaining_quantity == D("0")


def test_penny_conservation_three_way_prorata_split() -> None:
    # 3 lots of 1 unit each, sold together for 100.00.
    # Naive per-slice rounding (33.33 * 3 = 99.99) would lose a cent;
    # the residual must land on the last slice so the sum is exact.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("1"),
            amount=D("-30.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 1, 2),
            quantity=D("1"),
            amount=D("-30.00"),
        ),
        _event(
            "b3",
            event_type="buy",
            trade_date=date(2024, 1, 3),
            quantity=D("1"),
            amount=D("-30.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 2, 1),
            quantity=D("-3"),
            amount=D("100.00"),
        ),
    ]
    _lots, gains = _run(events)

    assert len(gains) == 3
    proceeds = [g.proceeds for g in gains]
    assert proceeds == [D("33.33"), D("33.33"), D("33.34")]
    assert sum(proceeds, D("0")) == D("100.00")


def test_independent_grouping_per_account_security() -> None:
    # Same security id under two accounts must not cross-consume.
    events = [
        _event(
            "a1b",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            account_id="acctA",
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "b1b",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            account_id="acctB",
            quantity=D("10"),
            amount=D("-2000.00"),
        ),
        _event(
            "a1s",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            account_id="acctA",
            quantity=D("-10"),
            amount=D("1500.00"),
        ),
    ]
    lots, gains = _run(events)

    assert len(lots) == 2
    assert len(gains) == 1
    # The sale drew from acctA's lot only.
    assert gains[0].account_id == "acctA"
    assert gains[0].cost_basis == D("1000.00")
    by_acct = {lot.account_id: lot for lot in lots}
    assert by_acct["acctB"].remaining_quantity == D("10")


def test_hifo_consumes_highest_per_unit_basis_lot_first() -> None:
    # b1: $10/unit, b2: $20/unit. HIFO must consume b2 first despite b1 being
    # older (this is the case that would silently regress to FIFO order).
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-200.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-5"),
            amount=D("150.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_hifo)

    by_src = {lot.source_transaction_id: lot for lot in lots}
    b1 = by_src["b1"]
    b2 = by_src["b2"]
    # b2 ($20/unit) is the one drawn down; b1 ($10/unit) is untouched.
    assert b2.remaining_quantity == D("5")
    assert b2.cost_basis_remaining == D("100.00")
    assert b1.remaining_quantity == D("10")
    assert b1.cost_basis_remaining == D("100.00")

    assert len(gains) == 1
    g = gains[0]
    assert g.lot_id == b2.lot_id
    assert g.quantity == D("5")
    assert g.cost_basis == D("100.00")
    # Single slice: full proceeds land here; gain = 150.00 - 100.00.
    assert g.proceeds == D("150.00")
    assert g.gain_loss == D("50.00")
    assert g.cost_basis_method == "hifo"


def test_hifo_equal_unit_cost_ties_break_oldest_first() -> None:
    # Both lots are $10/unit; HIFO's tie-break must fall back to
    # acquisition_date ascending (favors long-term treatment per spec).
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-5"),
            amount=D("75.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_hifo)

    by_src = {lot.source_transaction_id: lot for lot in lots}
    b1 = by_src["b1"]
    b2 = by_src["b2"]
    # Older lot (b1) consumed first on a tie; b2 is untouched.
    assert b1.remaining_quantity == D("5")
    assert b1.cost_basis_remaining == D("50.00")
    assert b2.remaining_quantity == D("10")
    assert b2.cost_basis_remaining == D("100.00")

    assert len(gains) == 1
    assert gains[0].lot_id == b1.lot_id


def test_hifo_multi_lot_order_and_partial_consumption_leaves_remainder() -> None:
    # Unit costs: b1=$10, b2=$30, b3=$20. HIFO order is b2, b3, b1.
    # Selling 15 fully drains b2 (10 units) and partially drains b3 (5 of 10),
    # leaving b1 completely untouched and b3 with a correct remainder.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-300.00"),
        ),
        _event(
            "b3",
            event_type="buy",
            trade_date=date(2024, 3, 1),
            quantity=D("10"),
            amount=D("-200.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 4, 1),
            quantity=D("-15"),
            amount=D("600.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_hifo)

    by_src = {lot.source_transaction_id: lot for lot in lots}
    assert by_src["b1"].remaining_quantity == D("10")
    assert by_src["b1"].cost_basis_remaining == D("100.00")
    assert by_src["b2"].remaining_quantity == D("0")
    assert by_src["b2"].cost_basis_remaining == D("0.00")
    assert by_src["b3"].remaining_quantity == D("5")
    assert by_src["b3"].cost_basis_remaining == D("100.00")

    assert len(gains) == 2
    by_lot = {g.lot_id: g for g in gains}
    assert by_lot[by_src["b2"].lot_id].quantity == D("10")
    assert by_lot[by_src["b2"].lot_id].cost_basis == D("300.00")
    assert by_lot[by_src["b3"].lot_id].quantity == D("5")
    assert by_lot[by_src["b3"].lot_id].cost_basis == D("100.00")


def test_hifo_term_reflects_each_consumed_lot_not_consumption_order() -> None:
    # b1 is long-term (opened 2022) but low unit cost; b2 is short-term
    # (opened a month before the sale) but high unit cost. HIFO consumes the
    # short-term b2 first, then dips into the long-term b1 — the opposite of
    # FIFO order. Each slice's term must still come from its own lot's dates.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2022, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),  # $10/unit, long-term at sale
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 6, 1),
            quantity=D("10"),
            amount=D("-300.00"),  # $30/unit, short-term at sale
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 7, 1),
            quantity=D("-15"),
            amount=D("600.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_hifo)

    by_src = {lot.source_transaction_id: lot for lot in lots}
    b1 = by_src["b1"]
    b2 = by_src["b2"]

    assert len(gains) == 2
    by_lot = {g.lot_id: g for g in gains}

    # b2 (short-term, high cost) fully consumed first, term reflects ITS dates.
    # Proceeds pro-rata by qty: 600.00 * 10/15 = 400.00; gain = 400 - 300.
    assert by_lot[b2.lot_id].quantity == D("10")
    assert by_lot[b2.lot_id].cost_basis == D("300.00")
    assert by_lot[b2.lot_id].proceeds == D("400.00")
    assert by_lot[b2.lot_id].gain_loss == D("100.00")
    assert by_lot[b2.lot_id].term == "short"

    # Remaining 5 drawn from b1 (long-term, low cost); term reflects ITS dates.
    # Last slice absorbs the residual: 600.00 - 400.00 = 200.00; gain = 200 - 50.
    assert by_lot[b1.lot_id].quantity == D("5")
    assert by_lot[b1.lot_id].cost_basis == D("50.00")
    assert by_lot[b1.lot_id].proceeds == D("200.00")
    assert by_lot[b1.lot_id].gain_loss == D("150.00")
    assert by_lot[b1.lot_id].term == "long"

    # Proceeds penny-conserved across both legs.
    assert sum((g.proceeds for g in gains), D("0")) == D("600.00")


def test_fifo_order_unchanged_after_hifo_refactor() -> None:
    # Regression guard for the ordering refactor: FIFO must still consume
    # strictly oldest-first even though b1 has the *lower* per-unit basis
    # (which HIFO would have ordered last, not first).
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),  # $10/unit
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-300.00"),  # $30/unit
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-5"),
            amount=D("150.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_fifo)

    by_src = {lot.source_transaction_id: lot for lot in lots}
    b1 = by_src["b1"]
    b2 = by_src["b2"]
    assert b1.remaining_quantity == D("5")
    assert b1.cost_basis_remaining == D("50.00")
    assert b2.remaining_quantity == D("10")
    assert b2.cost_basis_remaining == D("300.00")

    assert len(gains) == 1
    assert gains[0].lot_id == b1.lot_id
    assert gains[0].cost_basis_method == "fifo"


def test_specific_selection_overrides_fifo_order() -> None:
    # b1 (oldest), b2 (middle), b3 (newest). Selecting b2 must draw from it
    # despite FIFO wanting b1 first.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-200.00"),
        ),
        _event(
            "b3",
            event_type="buy",
            trade_date=date(2024, 3, 1),
            quantity=D("10"),
            amount=D("-300.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 4, 1),
            quantity=D("-10"),
            amount=D("250.00"),
        ),
    ]
    # Lot ids are content hashes independent of method/selections — discover
    # them with a throwaway FIFO pass before wiring the real selection.
    discovery_lots, _discovery_gains = _run(events, method_for=_fifo)
    b2_lot_id = next(
        lot.lot_id for lot in discovery_lots if lot.source_transaction_id == "b2"
    )

    def _select(disposal_txn_id: str) -> list[tuple[str, Decimal]]:
        if disposal_txn_id == "s1":
            return [(b2_lot_id, D("10"))]
        return []

    lots, gains = _run(events, method_for=_specific, selections_for=_select)

    by_src = {lot.source_transaction_id: lot for lot in lots}
    assert by_src["b1"].remaining_quantity == D("10")
    assert by_src["b1"].cost_basis_remaining == D("100.00")
    assert by_src["b2"].remaining_quantity == D("0")
    assert by_src["b2"].cost_basis_remaining == D("0.00")
    assert by_src["b3"].remaining_quantity == D("10")
    assert by_src["b3"].cost_basis_remaining == D("300.00")

    assert len(gains) == 1
    g = gains[0]
    assert g.lot_id == b2_lot_id
    assert g.quantity == D("10")
    assert g.cost_basis == D("200.00")
    assert g.proceeds == D("250.00")
    assert g.gain_loss == D("50.00")
    assert g.term == "short"
    assert g.cost_basis_method == "specific"
    assert g.basis_incomplete is False


def test_specific_partial_selection_falls_back_to_fifo_for_remainder() -> None:
    # A (oldest), B (middle), C (newest). Select 3 units of B; disposal is 8,
    # so 5 more must come from A via the FIFO fallback (the oldest lot with
    # remaining quantity), not from C.
    events = [
        _event(
            "a",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-200.00"),
        ),
        _event(
            "c",
            event_type="buy",
            trade_date=date(2024, 3, 1),
            quantity=D("10"),
            amount=D("-300.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 4, 1),
            quantity=D("-8"),
            amount=D("200.00"),
        ),
    ]
    discovery_lots, _ = _run(events, method_for=_fifo)
    b_lot_id = next(
        lot.lot_id for lot in discovery_lots if lot.source_transaction_id == "b"
    )

    def _select(disposal_txn_id: str) -> list[tuple[str, Decimal]]:
        if disposal_txn_id == "s1":
            return [(b_lot_id, D("3"))]
        return []

    lots, gains = _run(events, method_for=_specific, selections_for=_select)

    by_src = {lot.source_transaction_id: lot for lot in lots}
    # A partially drawn by the FIFO fallback; B partially drawn by selection;
    # C untouched.
    assert by_src["a"].remaining_quantity == D("5")
    assert by_src["a"].cost_basis_remaining == D("50.00")
    assert by_src["b"].remaining_quantity == D("7")
    assert by_src["b"].cost_basis_remaining == D("140.00")
    assert by_src["c"].remaining_quantity == D("10")
    assert by_src["c"].cost_basis_remaining == D("300.00")

    assert len(gains) == 2
    by_lot = {g.lot_id: g for g in gains}
    b_leg = by_lot[b_lot_id]
    a_leg = by_lot[by_src["a"].lot_id]

    assert b_leg.quantity == D("3")
    assert b_leg.cost_basis == D("60.00")
    assert b_leg.proceeds == D("75.00")
    assert b_leg.gain_loss == D("15.00")
    assert b_leg.term == "short"
    assert b_leg.basis_incomplete is False

    assert a_leg.quantity == D("5")
    assert a_leg.cost_basis == D("50.00")
    assert a_leg.proceeds == D("125.00")
    assert a_leg.gain_loss == D("75.00")
    assert a_leg.term == "short"
    assert a_leg.basis_incomplete is False

    assert sum((g.proceeds for g in gains), D("0")) == D("200.00")


def test_specific_partially_selected_lot_remainder_reused_by_fifo_fallback() -> None:
    # The double-appearance edge: lot A has 10 units; select 3 of them.
    # Disposal is 10, so the FIFO fallback (over ALL open lots, A included)
    # draws A's live remaining 7 next since A is the only (and therefore
    # oldest) open lot. A is fully consumed via two internal slices, but the
    # (disposal x lot) grain requires those to MERGE into ONE realized-gain
    # row — two rows would collide on the realized_gain_id PK.
    events = [
        _event(
            "a",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 2, 1),
            quantity=D("-10"),
            amount=D("1200.00"),
        ),
    ]
    discovery_lots, _ = _run(events, method_for=_fifo)
    a_lot_id = discovery_lots[0].lot_id

    def _select(disposal_txn_id: str) -> list[tuple[str, Decimal]]:
        if disposal_txn_id == "s1":
            return [(a_lot_id, D("3"))]
        return []

    lots, gains = _run(events, method_for=_specific, selections_for=_select)

    assert lots[0].remaining_quantity == D("0")
    assert lots[0].cost_basis_remaining == D("0.00")

    # Exactly one merged row for lot A: the 3 selected + 7 fallback units.
    assert len(gains) == 1
    g = gains[0]
    assert g.lot_id == a_lot_id
    assert g.quantity == D("10")
    assert g.cost_basis == D("1000.00")
    assert g.proceeds == D("1200.00")
    assert g.gain_loss == D("200.00")
    assert g.term == "short"
    assert g.basis_incomplete is False

    # Regression guard: realized_gain_id must be unique within a disposal.
    ids = [g.realized_gain_id for g in gains]
    assert len(ids) == len(set(ids))


def test_specific_selection_of_unknown_lot_is_ignored_fifo_covers_disposal() -> None:
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 2, 1),
            quantity=D("-10"),
            amount=D("1200.00"),
        ),
    ]

    def _select(_disposal_txn_id: str) -> list[tuple[str, Decimal]]:
        return [("lot_does_not_exist_00000000", D("5"))]

    lots, gains = _run(events, method_for=_specific, selections_for=_select)

    assert lots[0].remaining_quantity == D("0")
    assert len(gains) == 1
    assert gains[0].lot_id == lots[0].lot_id
    assert gains[0].quantity == D("10")
    assert gains[0].cost_basis == D("1000.00")
    assert gains[0].proceeds == D("1200.00")
    assert gains[0].gain_loss == D("200.00")
    assert gains[0].basis_incomplete is False


def test_specific_selection_of_closed_lot_is_ignored_fifo_covers_disposal() -> None:
    # b1 is fully closed by s0 (no selection — specific falls back to plain
    # FIFO). s1 then selects the now-closed b1: that selection must be
    # ignored, and the fresh b2 lot must cover the disposal via FIFO.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "s0",
            event_type="sell",
            trade_date=date(2024, 2, 1),
            quantity=D("-10"),
            amount=D("1200.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 3, 1),
            quantity=D("10"),
            amount=D("-2000.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 4, 1),
            quantity=D("-5"),
            amount=D("600.00"),
        ),
    ]
    discovery_lots, _ = _run(events, method_for=_fifo)
    b1_lot_id = next(
        lot.lot_id for lot in discovery_lots if lot.source_transaction_id == "b1"
    )

    def _select(disposal_txn_id: str) -> list[tuple[str, Decimal]]:
        if disposal_txn_id == "s1":
            return [(b1_lot_id, D("3"))]  # b1 is closed by the time s1 runs
        return []

    lots, gains = _run(events, method_for=_specific, selections_for=_select)

    by_src = {lot.source_transaction_id: lot for lot in lots}
    assert by_src["b1"].remaining_quantity == D("0")  # untouched by s1
    assert by_src["b2"].remaining_quantity == D("5")

    s1_gains = [g for g in gains if g.disposal_txn_id == "s1"]
    assert len(s1_gains) == 1
    assert s1_gains[0].lot_id == by_src["b2"].lot_id
    assert s1_gains[0].quantity == D("5")
    assert s1_gains[0].cost_basis == D("1000.00")
    assert s1_gains[0].proceeds == D("600.00")
    assert s1_gains[0].gain_loss == D("-400.00")
    assert s1_gains[0].basis_incomplete is False


def test_specific_selection_qty_exceeding_remaining_is_capped_remainder_fifo() -> None:
    # B (oldest, 10 units @ $50/unit) is not selected. A (newer, only 6 units
    # @ $100/unit) is selected for 15 units — far more than it has. The
    # selection is capped at A's remaining 6; the other 4 units needed to
    # cover the 10-unit disposal fall back to FIFO, drawing from B.
    events = [
        _event(
            "b",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-500.00"),
        ),
        _event(
            "a",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("6"),
            amount=D("-600.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-10"),
            amount=D("1000.00"),
        ),
    ]
    discovery_lots, _ = _run(events, method_for=_fifo)
    a_lot_id = next(
        lot.lot_id for lot in discovery_lots if lot.source_transaction_id == "a"
    )

    def _select(disposal_txn_id: str) -> list[tuple[str, Decimal]]:
        if disposal_txn_id == "s1":
            return [(a_lot_id, D("15"))]  # far exceeds A's remaining 6
        return []

    lots, gains = _run(events, method_for=_specific, selections_for=_select)

    by_src = {lot.source_transaction_id: lot for lot in lots}
    assert by_src["a"].remaining_quantity == D("0")
    assert by_src["a"].cost_basis_remaining == D("0.00")
    assert by_src["b"].remaining_quantity == D("6")
    assert by_src["b"].cost_basis_remaining == D("300.00")

    assert len(gains) == 2
    by_lot = {g.lot_id: g for g in gains}
    a_leg = by_lot[a_lot_id]
    b_leg = by_lot[by_src["b"].lot_id]

    assert a_leg.quantity == D("6")
    assert a_leg.cost_basis == D("600.00")
    assert a_leg.proceeds == D("600.00")
    assert a_leg.gain_loss == D("0.00")

    assert b_leg.quantity == D("4")
    assert b_leg.cost_basis == D("200.00")
    assert b_leg.proceeds == D("400.00")
    assert b_leg.gain_loss == D("200.00")

    assert sum((g.proceeds for g in gains), D("0")) == D("1000.00")


def test_specific_with_no_selections_behaves_like_fifo() -> None:
    # selections_for returning [] for every disposal is the FIFO/HIFO tests'
    # default; specific-ID must degrade to plain FIFO consumption order.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-300.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-5"),
            amount=D("150.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_specific)

    by_src = {lot.source_transaction_id: lot for lot in lots}
    assert by_src["b1"].remaining_quantity == D("5")
    assert by_src["b2"].remaining_quantity == D("10")
    assert len(gains) == 1
    assert gains[0].lot_id == by_src["b1"].lot_id
    assert gains[0].cost_basis_method == "specific"


def test_average_partial_sell_uses_pooled_basis_and_rescales_pool() -> None:
    # Pool = 20 units / $300 (avg $15). Sell 5 => realized basis 5 * $15 = $75,
    # NOT FIFO's 5 * $10 = $50. Pool after = 15 units / $225 (avg still $15),
    # split across the two open lots as their pooled-average share.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-200.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-5"),
            amount=D("90.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_average)

    assert len(gains) == 1
    g = gains[0]
    by_src = {lot.source_transaction_id: lot for lot in lots}
    # Oldest lot supplies the holding-period attribution; basis is the pool's.
    assert g.lot_id == by_src["b1"].lot_id
    assert g.quantity == D("5")
    assert g.cost_basis == D("75.00")  # pooled avg $15/unit, not FIFO's $50
    assert g.proceeds == D("90.00")
    assert g.gain_loss == D("15.00")
    assert g.cost_basis_method == "average"
    assert g.basis_incomplete is False

    # Pool rescaled to 15 units / $225; each open lot carries its average share.
    assert by_src["b1"].remaining_quantity == D("5")
    assert by_src["b1"].cost_basis_remaining == D("75.00")  # 5 * $15
    assert by_src["b2"].remaining_quantity == D("10")
    assert by_src["b2"].cost_basis_remaining == D("150.00")  # 10 * $15
    remaining_pool = sum(
        (lot.cost_basis_remaining for lot in lots if lot.remaining_quantity > 0),
        D("0"),
    )
    assert remaining_pool == D("225.00")


def test_average_diverges_from_fifo_on_identical_events() -> None:
    # The distinctness guard: the same ledger yields a different realized basis
    # under average vs FIFO, and running average must not perturb FIFO's result.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-200.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-5"),
            amount=D("90.00"),
        ),
    ]
    _fifo_lots, fifo_gains = _run(events, method_for=_fifo)
    _avg_lots, avg_gains = _run(events, method_for=_average)
    # FIFO draws the oldest lot's actual $10/unit basis; average blends to $15.
    assert fifo_gains[0].cost_basis == D("50.00")
    assert avg_gains[0].cost_basis == D("75.00")


def test_average_subsequent_buy_shifts_the_running_average() -> None:
    # A later acquisition mutates the pool: the next disposal uses the new avg.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-200.00"),
        ),
        _event(
            "s1",  # pool 20u/$300, avg $15 => basis 5 * $15 = $75
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-5"),
            amount=D("90.00"),
        ),
        _event(
            "b3",  # pool 15u/$225 + 5u/$150 => 20u/$375, avg $18.75
            event_type="buy",
            trade_date=date(2024, 4, 1),
            quantity=D("5"),
            amount=D("-150.00"),
        ),
        _event(
            "s2",  # basis 5 * $18.75 = $93.75 (the shifted average)
            event_type="sell",
            trade_date=date(2024, 5, 1),
            quantity=D("-5"),
            amount=D("100.00"),
        ),
    ]
    _lots, gains = _run(events, method_for=_average)

    by_txn = {g.disposal_txn_id: g for g in gains}
    assert by_txn["s1"].cost_basis == D("75.00")
    assert by_txn["s2"].cost_basis == D("93.75")
    assert by_txn["s2"].quantity == D("5")


def test_average_sell_spanning_st_and_lt_lots_splits_terms_pooled_basis() -> None:
    # Long-term lot (2023) at $10/unit and short-term lot (2024) at $20/unit;
    # pool = 20 units / $300, avg $15. Selling 15 across both: each slice's term
    # comes from its own lot's dates, but the basis is the pooled $15/unit — not
    # the lot's actual cost.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2023, 1, 1),  # long-term at sale
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 6, 1),  # short-term at sale
            quantity=D("10"),
            amount=D("-200.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 7, 1),
            quantity=D("-15"),
            amount=D("450.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_average)

    assert len(gains) == 2
    by_term = {g.term: g for g in gains}
    long_leg = by_term["long"]
    short_leg = by_term["short"]

    # Long lot fully consumed (10 units); pooled basis 10 * $15 = $150.
    assert long_leg.quantity == D("10")
    assert long_leg.cost_basis == D("150.00")
    # Short lot partially consumed (5 units); pooled basis 5 * $15 = $75.
    assert short_leg.quantity == D("5")
    assert short_leg.cost_basis == D("75.00")

    # Blended basis = $15/unit * 15 units = $225 (one 1099-B figure, ST/LT split).
    assert long_leg.cost_basis + short_leg.cost_basis == D("225.00")
    assert sum((g.proceeds for g in gains), D("0")) == D("450.00")

    by_src = {lot.source_transaction_id: lot for lot in lots}
    assert by_src["b1"].remaining_quantity == D("0")
    assert by_src["b2"].remaining_quantity == D("5")
    assert by_src["b2"].cost_basis_remaining == D("75.00")  # 5 * $15


def test_average_holdings_reconciliation_sum_equals_remaining_pooled_cost() -> None:
    # Contributions: $100 + $300 + $150 = $550 over 25 units (derived from the
    # inputs, not the output). Realized basis: sell 5 @ avg $20 => $100; sell 10
    # @ avg $22.50 => $225. Remaining pooled cost = 550 - 100 - 225 = $225.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",  # pool 20u/$400, avg $20
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-300.00"),
        ),
        _event(
            "s1",  # basis 5 * $20 = $100; pool 15u/$300
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-5"),
            amount=D("120.00"),
        ),
        _event(
            "b3",  # pool 20u/$450, avg $22.50
            event_type="buy",
            trade_date=date(2024, 4, 1),
            quantity=D("5"),
            amount=D("-150.00"),
        ),
        _event(
            "s2",  # basis 10 * $22.50 = $225; pool 10u/$225
            event_type="sell",
            trade_date=date(2024, 5, 1),
            quantity=D("-10"),
            amount=D("250.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_average)

    realized_basis = sum((g.cost_basis for g in gains), D("0"))
    assert realized_basis == D("325.00")  # $100 + $225

    # The reconciliation dim_holdings depends on: SUM(open cost_basis_remaining)
    # == remaining pooled cost, derived independently as $550 - $325.
    open_basis = sum(
        (lot.cost_basis_remaining for lot in lots if lot.remaining_quantity > 0),
        D("0"),
    )
    assert open_basis == D("225.00")

    by_src = {lot.source_transaction_id: lot for lot in lots}
    assert by_src["b1"].remaining_quantity == D("0")
    assert by_src["b1"].cost_basis_remaining == D("0.00")
    # Open lots B and C (5 units each) carry the $22.50/unit average share.
    assert by_src["b2"].remaining_quantity == D("5")
    assert by_src["b2"].cost_basis_remaining == D("112.50")
    assert by_src["b3"].remaining_quantity == D("5")
    assert by_src["b3"].cost_basis_remaining == D("112.50")


def test_average_oversold_consumes_available_at_avg_then_zero_basis_remainder() -> None:
    # Pool = 10 units / $100 (avg $10). Selling 15 draws all 10 at avg (a full
    # pool close taking all remaining cost) then emits a zero-basis, incomplete
    # slice for the 5 unmatched units. The engine never raises.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 2, 1),
            quantity=D("-15"),  # 5 more than pooled
            amount=D("225.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_average)

    assert len(gains) == 2
    matched = next(g for g in gains if not g.basis_incomplete)
    unmatched = next(g for g in gains if g.basis_incomplete)

    assert matched.quantity == D("10")
    assert matched.cost_basis == D("100.00")  # all 10 units at avg $10
    assert unmatched.quantity == D("5")
    assert unmatched.cost_basis == D("0.00")
    assert unmatched.term == "short"
    assert sum((g.proceeds for g in gains), D("0")) == D("225.00")

    assert lots[0].remaining_quantity == D("0")
    assert lots[0].cost_basis_remaining == D("0.00")


def test_average_full_pool_close_takes_all_remaining_conserves_pennies() -> None:
    # Non-terminating average: 15 units / $150.05 => $10.0033.../unit. Two sells
    # drain the pool; realized basis across both must sum to the exact $150.05
    # contributed — no penny stranded in the (now empty) pool.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("5"),
            amount=D("-50.05"),
        ),
        _event(
            "s1",  # basis _money(5 * 150.05/15) = $50.02; pool 10u/$100.03
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-5"),
            amount=D("60.00"),
        ),
        _event(
            "s2",  # full close: basis = all remaining $100.03
            event_type="sell",
            trade_date=date(2024, 4, 1),
            quantity=D("-10"),
            amount=D("120.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_average)

    realized_basis = sum((g.cost_basis for g in gains), D("0"))
    assert realized_basis == D("150.05")  # exactly the total contributed

    # Pool fully drained: every lot closed with zero remaining basis.
    for lot in lots:
        assert lot.remaining_quantity == D("0")
        assert lot.cost_basis_remaining == D("0.00")

    s1 = next(g for g in gains if g.disposal_txn_id == "s1")
    assert s1.cost_basis == D("50.02")  # _money(5 * 150.05/15)


def test_average_reconciliation_residual_lands_on_last_open_lot() -> None:
    # Locks _reconcile_average_lots's residual-to-last branch: three OPEN lots
    # of unequal quantity (0.4, 0.7, 1.9 = 3.0 units) over $100.00 pooled cost
    # give avg = $33.3333.../unit — non-terminating. Naive per-lot rounding
    # (_money(rq * avg)) sums to only $99.99, stranding a cent; the last open lot
    # must absorb it so SUM(cost_basis_remaining) == pooled cost EXACTLY. A naive
    # rewrite (last lot = _money(rq * avg) = $63.33) would drop this cent and this
    # test alone would catch it. No disposals: all three lots stay open.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("0.4"),
            amount=D("-30.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("0.7"),
            amount=D("-30.00"),
        ),
        _event(
            "b3",
            event_type="buy",
            trade_date=date(2024, 3, 1),
            quantity=D("1.9"),
            amount=D("-40.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_average)

    assert gains == []  # holdings only, no disposals
    by_src = {lot.source_transaction_id: lot for lot in lots}

    # Reconciliation invariant: SUM over open lots == remaining pooled cost.
    open_basis = sum(
        (lot.cost_basis_remaining for lot in lots if lot.remaining_quantity > 0),
        D("0"),
    )
    assert open_basis == D("100.00")  # exact to the cent

    # Pinned per-lot shares prove the residual landed on the LAST open lot (b3):
    # naive _money(rq * $33.3333...) gives $13.33 / $23.33 / $63.33 (sum $99.99);
    # b3 instead carries $63.34, the $99.99 + residual reconciliation.
    assert by_src["b1"].cost_basis_remaining == D("13.33")  # 0.4 * avg, rounded
    assert by_src["b2"].cost_basis_remaining == D("23.33")  # 0.7 * avg, rounded
    assert by_src["b3"].cost_basis_remaining == D("63.34")  # 0.7 rounded + residual


# ---------------------------------------------------------------------------
# Corporate actions: split, return_of_capital, reinvest (Task 12).
# split/return_of_capital adjust open lots in place (never a disposal, never a
# realized gain); reinvest opens a lot exactly like a buy but records
# acquisition_type='reinvest'.
# ---------------------------------------------------------------------------


def test_split_two_for_one_doubles_quantity_preserves_total_basis() -> None:
    # 2:1 split — the split row carries the multiplier M=2 in its quantity.
    # 10 units @ $1000 basis => 20 units @ $1000 basis ($50/unit): total basis
    # is preserved exactly, only per-unit basis changes.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "sp",
            event_type="split",
            trade_date=date(2024, 6, 1),
            quantity=D("2"),
        ),
    ]
    lots, gains = _run(events)

    assert gains == []  # a split realizes nothing
    assert len(lots) == 1
    lot = lots[0]
    assert lot.original_quantity == D("20")
    assert lot.remaining_quantity == D("20")
    # Total basis preserved (independently: the buy's |amount|); per-unit halves.
    assert lot.cost_basis_total == D("1000.00")
    assert lot.cost_basis_remaining == D("1000.00")
    assert lot.cost_basis_remaining / lot.remaining_quantity == D("50")


def test_split_three_for_two_scales_quantity_preserving_basis() -> None:
    # 3:2 split — multiplier M=1.5. 10 units => 15 units, basis unchanged.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "sp",
            event_type="split",
            trade_date=date(2024, 6, 1),
            quantity=D("1.5"),
        ),
    ]
    lots, _gains = _run(events)
    lot = lots[0]
    assert lot.original_quantity == D("15")
    assert lot.remaining_quantity == D("15")
    assert lot.cost_basis_total == D("1000.00")
    assert lot.cost_basis_remaining == D("1000.00")


def test_reverse_split_one_for_two_halves_quantity_preserving_basis() -> None:
    # 1:2 reverse split — multiplier M=0.5. 10 units => 5 units, basis unchanged
    # (per-unit basis doubles to $200).
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "sp",
            event_type="split",
            trade_date=date(2024, 6, 1),
            quantity=D("0.5"),
        ),
    ]
    lots, _gains = _run(events)
    lot = lots[0]
    assert lot.original_quantity == D("5")
    assert lot.remaining_quantity == D("5")
    assert lot.cost_basis_total == D("1000.00")
    assert lot.cost_basis_remaining == D("1000.00")
    assert lot.cost_basis_remaining / lot.remaining_quantity == D("200")


def test_split_then_sell_uses_post_split_per_unit_basis() -> None:
    # A 2:1 split turns 10 units @ $1000 into 20 units @ $50/unit; selling 5
    # realizes 5 * $50 = $250 basis (post-split), leaving 15 units @ $750.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "sp",
            event_type="split",
            trade_date=date(2024, 6, 1),
            quantity=D("2"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 7, 1),
            quantity=D("-5"),
            amount=D("400.00"),
        ),
    ]
    lots, gains = _run(events)

    assert len(gains) == 1
    g = gains[0]
    assert g.quantity == D("5")
    assert g.cost_basis == D("250.00")  # 5 * post-split $50/unit
    assert g.proceeds == D("400.00")
    assert g.gain_loss == D("150.00")

    lot = lots[0]
    assert lot.remaining_quantity == D("15")
    assert lot.cost_basis_remaining == D("750.00")


def test_split_under_average_scales_pool_units_leaves_cost() -> None:
    # Pool 10 units / $100 (avg $10). A 2:1 split doubles pooled units to 20
    # while pooled cost stays $100, so the average halves to $5/unit — the next
    # disposal draws basis at the halved average.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "sp",
            event_type="split",
            trade_date=date(2024, 6, 1),
            quantity=D("2"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 7, 1),
            quantity=D("-5"),
            amount=D("50.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_average)

    assert len(gains) == 1
    g = gains[0]
    assert g.cost_basis == D("25.00")  # 5 * halved avg $5, not pre-split $10
    assert g.cost_basis_method == "average"

    lot = lots[0]
    assert lot.remaining_quantity == D("15")  # 20 post-split - 5 sold
    assert lot.cost_basis_remaining == D("75.00")  # $100 pooled - $25 realized


def test_return_of_capital_reduces_open_lots_prorata_by_quantity() -> None:
    # Two open lots (10 and 30 units); a $40 RoC spreads pro-rata by remaining
    # quantity: $10 to the 10-unit lot, $30 to the 30-unit lot. Neither clamps.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("30"),
            amount=D("-300.00"),
        ),
        _event(
            "rc",
            event_type="return_of_capital",
            trade_date=date(2024, 3, 1),
            amount=D("-40.00"),
        ),
    ]
    lots, gains = _run(events)

    assert gains == []  # RoC is not a disposal
    by_src = {lot.source_transaction_id: lot for lot in lots}
    assert by_src["b1"].cost_basis_remaining == D("90.00")  # 100 - 40*10/40
    assert by_src["b2"].cost_basis_remaining == D("270.00")  # 300 - 40*30/40
    # Quantities are untouched by RoC.
    assert by_src["b1"].remaining_quantity == D("10")
    assert by_src["b2"].remaining_quantity == D("30")
    # Total reduction equals the distribution exactly (penny-conserved).
    total = by_src["b1"].cost_basis_remaining + by_src["b2"].cost_basis_remaining
    assert total == D("360.00")  # 400 contributed - 40 returned


def test_return_of_capital_exceeding_basis_clamps_to_zero_drops_excess() -> None:
    # Total open basis is $300; a $500 RoC drives every lot to zero basis and
    # the $200 excess is dropped — v1 does NOT realize it as a gain.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-200.00"),
        ),
        _event(
            "rc",
            event_type="return_of_capital",
            trade_date=date(2024, 3, 1),
            amount=D("-500.00"),
        ),
    ]
    lots, gains = _run(events)

    assert gains == []  # excess is dropped, never a realized gain in v1
    by_src = {lot.source_transaction_id: lot for lot in lots}
    assert by_src["b1"].cost_basis_remaining == D("0.00")
    assert by_src["b2"].cost_basis_remaining == D("0.00")
    # Quantities unchanged: RoC is not a disposal.
    assert by_src["b1"].remaining_quantity == D("10")
    assert by_src["b2"].remaining_quantity == D("10")


def test_return_of_capital_clamp_overflow_is_dropped_v1() -> None:
    # PINS a known v1 quirk (NOT a target behavior): with uneven per-unit basis a
    # single lot's quantity-share can exceed its own basis even when the aggregate
    # RoC is within total basis, and the clamp-overflow is DROPPED rather than
    # redistributed. Lot A (10u/$200, $20/unit) + lot B (100u/$10, $0.10/unit, the
    # last open lot). RoC $100, aggregate ($100) <= total basis ($210).
    # Pro-rata-by-quantity would hand B ~$90.91, but B clamps at its $10 basis and
    # residual-to-last only redistributes rounding pennies, so ~$80.91 of
    # basis-reduction is dropped: the position keeps $190.91 when $110 is
    # economically correct. On eventual sale this UNDER-reports realized gain
    # (taxpayer-favorable / IRS-unfavorable) — a conscious, locked v1 contract.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-200.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("100"),
            amount=D("-10.00"),
        ),
        _event(
            "rc",
            event_type="return_of_capital",
            trade_date=date(2024, 3, 1),
            amount=D("-100.00"),
        ),
    ]
    lots, gains = _run(events)

    assert gains == []  # RoC is not a disposal
    by_src = {lot.source_transaction_id: lot for lot in lots}
    # A (not last) takes only its tiny quantity-share; B (last) clamps at $10.
    assert by_src["b1"].cost_basis_remaining == D("190.91")  # 200 - _money(100*10/110)
    assert by_src["b2"].cost_basis_remaining == D("0.00")  # clamped at its $10 basis
    # Quantities untouched (RoC is not a disposal).
    assert by_src["b1"].remaining_quantity == D("10")
    assert by_src["b2"].remaining_quantity == D("100")

    # The dropped clamp-overflow: total reduction ($19.09) is far below
    # min(RoC $100, Sigma basis $210) = $100, proving overflow is NOT
    # redistributed in v1 (a full fix would reduce the full $100).
    open_basis = by_src["b1"].cost_basis_remaining + by_src["b2"].cost_basis_remaining
    assert open_basis == D("190.91")
    total_reduction = D("210.00") - open_basis
    assert total_reduction == D("19.09")
    assert total_reduction < D("100.00")  # min(RoC, Sigma basis); overflow dropped


def test_return_of_capital_under_average_reduces_pooled_cost() -> None:
    # Pool 10 units / $100 (avg $10). A $50 RoC cuts pooled cost to $50 (avg
    # $5); the next disposal's basis reflects the reduced average.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "rc",
            event_type="return_of_capital",
            trade_date=date(2024, 2, 1),
            amount=D("-50.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-5"),
            amount=D("60.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_average)

    assert len(gains) == 1
    assert gains[0].cost_basis == D("25.00")  # 5 * reduced avg $5
    lot = lots[0]
    assert lot.remaining_quantity == D("5")
    assert lot.cost_basis_remaining == D("25.00")  # $50 pooled - $25 realized


def test_reinvest_opens_lot_with_reinvest_acquisition_type() -> None:
    # A reinvest is a buy leg whose acquisition_type records the funding source;
    # it opens a lot dated at the trade date with basis = |amount|.
    events = [
        _event(
            "ri",
            event_type="reinvest",
            trade_date=date(2024, 1, 1),
            quantity=D("5"),
            amount=D("-75.00"),
        ),
    ]
    lots, gains = _run(events)

    assert gains == []
    assert len(lots) == 1
    lot = lots[0]
    assert lot.acquisition_type == "reinvest"
    assert lot.acquisition_date == date(2024, 1, 1)
    assert lot.original_quantity == D("5")
    assert lot.remaining_quantity == D("5")
    assert lot.cost_basis_total == D("75.00")
    assert lot.cost_basis_remaining == D("75.00")


def test_reinvest_under_average_adds_to_the_pool() -> None:
    # Buy 10 @ $100 then reinvest 10 @ $300 => pool 20 units / $400 (avg $20).
    # A later sell of 5 draws 5 * $20 = $100 — the reinvest must have entered
    # the pool, not been skipped as an unhandled type.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "ri",
            event_type="reinvest",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-300.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-5"),
            amount=D("150.00"),
        ),
    ]
    _lots, gains = _run(events, method_for=_average)

    assert len(gains) == 1
    assert gains[0].cost_basis == D("100.00")  # 5 * avg $20 (reinvest pooled)


def test_reinvest_then_sell_consumes_the_reinvested_lot() -> None:
    # FIFO across a buy (10 @ $100) and a reinvest (5 @ $75). Selling 12 fully
    # consumes the buy and dips 2 units into the reinvested lot.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "ri",
            event_type="reinvest",
            trade_date=date(2024, 2, 1),
            quantity=D("5"),
            amount=D("-75.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-12"),
            amount=D("600.00"),
        ),
    ]
    lots, gains = _run(events)

    by_src = {lot.source_transaction_id: lot for lot in lots}
    ri = by_src["ri"]
    assert ri.acquisition_type == "reinvest"
    assert ri.remaining_quantity == D("3")  # 5 - 2 consumed
    assert ri.cost_basis_remaining == D("45.00")  # $75 - _money(75*2/5)=$30

    assert len(gains) == 2
    by_lot = {g.lot_id: g for g in gains}
    ri_leg = by_lot[ri.lot_id]
    assert ri_leg.quantity == D("2")
    assert ri_leg.cost_basis == D("30.00")  # _money(75 * 2/5)
    # Proceeds penny-conserved across both legs.
    assert sum((g.proceeds for g in gains), D("0")) == D("600.00")


def test_oversold_under_hifo_generalizes_zero_basis_remainder() -> None:
    # Oversold generalizes past FIFO: two lots ($200 then $100 in HIFO order)
    # are fully consumed and the 5-unit remainder realizes at zero basis with
    # basis_incomplete=True. The engine never raises.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-100.00"),
        ),
        _event(
            "b2",
            event_type="buy",
            trade_date=date(2024, 2, 1),
            quantity=D("10"),
            amount=D("-200.00"),
        ),
        _event(
            "s1",
            event_type="sell",
            trade_date=date(2024, 3, 1),
            quantity=D("-25"),  # 5 more than tracked
            amount=D("1000.00"),
        ),
    ]
    lots, gains = _run(events, method_for=_hifo)

    assert len(gains) == 3
    matched = [g for g in gains if not g.basis_incomplete]
    unmatched = [g for g in gains if g.basis_incomplete]
    assert len(matched) == 2
    assert len(unmatched) == 1

    u = unmatched[0]
    assert u.quantity == D("5")
    assert u.cost_basis == D("0.00")
    assert u.term == "short"  # acquisition_date == disposal_date => 0 days
    assert u.cost_basis_method == "hifo"

    # Matched legs carry their real HIFO-ordered bases ($200 then $100).
    matched_basis = sum((g.cost_basis for g in matched), D("0"))
    assert matched_basis == D("300.00")

    # Proceeds conserved across matched + unmatched; both real lots drained.
    assert sum((g.proceeds for g in gains), D("0")) == D("1000.00")
    for lot in lots:
        assert lot.remaining_quantity == D("0")
