"""Unit tests for the FIFO (Task 8) and HIFO (Task 9) cost-basis engine.

Correctness-critical: these tests pin the FIFO and HIFO consumption orders,
penny conservation, deterministic IDs, ST/LT split (per-lot, independent of
consumption order), and oversold handling that a 1099-B reconciliation later
depends on. All monetary literals are ``Decimal`` — never float — because
float rounding would defeat the penny-conservation guarantees under test.
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
) -> tuple[list[Lot], list[RealizedGain]]:
    lots, gains = compute_lots_and_gains(
        events,
        method_for=method_for,
        selections_for=_no_selections,
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
    # reinvest / split belong to Tasks 11-12; the engine must not crash on them.
    events = [
        _event(
            "b1",
            event_type="buy",
            trade_date=date(2024, 1, 1),
            quantity=D("10"),
            amount=D("-1000.00"),
        ),
        _event(
            "rc",
            event_type="return_of_capital",
            trade_date=date(2024, 2, 1),
            amount=D("-50.00"),
        ),
        _event(
            "sp",
            event_type="split",
            trade_date=date(2024, 3, 1),
            quantity=D("10"),
        ),
    ]
    lots, gains = _run(events)
    assert len(lots) == 1  # only the buy opened a lot
    assert lots[0].remaining_quantity == D("10")
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
