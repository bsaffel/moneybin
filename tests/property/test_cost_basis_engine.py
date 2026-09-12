"""Properties of the cost-basis engine's conservation and ordering contracts.

Example tests in ``test_cost_basis_engine.py`` pin tax treatment and worked
cases. These properties quantify over many valid ledgers, where a finite set
of examples cannot establish that all four methods preserve quantity and that
FIFO/HIFO are independent of the caller's container order.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, timedelta
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from moneybin.investments.cost_basis import (
    LedgerEvent,
    Lot,
    RealizedGain,
    compute_lots_and_gains,
)

D = Decimal
_BASE_DATE = date(2024, 1, 1)


def _method(method: str) -> Callable[[str, str], str]:
    return lambda _account_id, _security_id: method


def _no_selections(_disposal_txn_id: str) -> list[tuple[str, Decimal]]:
    return []


@st.composite
def _covered_ledger(
    draw: st.DrawFn,
) -> tuple[list[LedgerEvent], Decimal]:
    """Create purchases followed by a sale that cannot be oversold."""
    purchases = draw(
        st.lists(
            st.tuples(
                st.integers(min_value=1, max_value=100),
                st.integers(min_value=1, max_value=100_000),
            ),
            min_size=1,
            max_size=6,
        )
    )
    acquired_quantity = sum((D(quantity) for quantity, _cents in purchases), D("0"))
    sold_quantity = D(draw(st.integers(min_value=1, max_value=int(acquired_quantity))))
    sale_cents = draw(st.integers(min_value=1, max_value=100_000))

    events = [
        LedgerEvent(
            investment_transaction_id=f"buy-{index}",
            account_id="account",
            security_id="security",
            trade_date=_BASE_DATE + timedelta(days=index),
            original_acquisition_date=None,
            type="buy",
            quantity=D(quantity),
            price=None,
            amount=-(D(cents) / D("100")),
            fees=None,
            currency_code="USD",
        )
        for index, (quantity, cents) in enumerate(purchases)
    ]
    events.append(
        LedgerEvent(
            investment_transaction_id="sell",
            account_id="account",
            security_id="security",
            trade_date=_BASE_DATE + timedelta(days=len(purchases) + 1),
            original_acquisition_date=None,
            type="sell",
            quantity=-sold_quantity,
            price=None,
            amount=D(sale_cents) / D("100"),
            fees=None,
            currency_code="USD",
        )
    )
    return events, acquired_quantity


def _run(
    events: list[LedgerEvent], method: str
) -> tuple[list[Lot], list[RealizedGain]]:
    return compute_lots_and_gains(
        events,
        method_for=_method(method),
        selections_for=_no_selections,
    )


@given(
    ledger=_covered_ledger(),
    method=st.sampled_from(("fifo", "hifo", "specific", "average")),
)
def test_cost_basis_methods_conserve_acquired_quantity(
    ledger: tuple[list[LedgerEvent], Decimal], method: str
) -> None:
    """Every acquired unit is either open or represented by one realized gain."""
    events, acquired_quantity = ledger
    lots, gains = _run(events, method)

    remaining_quantity = sum((lot.remaining_quantity for lot in lots), D("0"))
    realized_quantity = sum((gain.quantity for gain in gains), D("0"))

    assert remaining_quantity + realized_quantity == acquired_quantity


@given(
    ledger=_covered_ledger(),
    method=st.sampled_from(("fifo", "hifo")),
    data=st.data(),
)
def test_fifo_and_hifo_ignore_input_order_when_transaction_identities_and_dates_are_stable(
    ledger: tuple[list[LedgerEvent], Decimal], method: str, data: st.DataObject
) -> None:
    """Permuting input must not alter an economically ordered ledger's output."""
    events, _acquired_quantity = ledger
    permuted_events = list(data.draw(st.permutations(events), label="input-order"))

    lots, gains = _run(events, method)
    reordered_lots, reordered_gains = _run(permuted_events, method)

    assert _lot_snapshot(lots) == _lot_snapshot(reordered_lots)
    assert _gain_snapshot(gains) == _gain_snapshot(reordered_gains)


@given(
    ledger=_covered_ledger(),
    method=st.sampled_from(("fifo", "hifo", "specific", "average")),
)
def test_realized_gain_is_proceeds_minus_basis_to_the_cent(
    ledger: tuple[list[LedgerEvent], Decimal], method: str
) -> None:
    """Each persisted gain reconciles its independently persisted monetary terms."""
    events, _acquired_quantity = ledger
    _lots, gains = _run(events, method)

    for gain in gains:
        assert gain.gain_loss == gain.proceeds - gain.cost_basis
        assert gain.gain_loss == gain.gain_loss.quantize(D("0.01"))


def _lot_snapshot(lots: list[Lot]) -> list[tuple[object, ...]]:
    """Compare observable lot state without depending on result-list order."""
    return sorted(
        (
            lot.lot_id,
            lot.remaining_quantity,
            lot.cost_basis_remaining,
            lot.basis_incomplete,
        )
        for lot in lots
    )


def _gain_snapshot(gains: list[RealizedGain]) -> list[tuple[object, ...]]:
    """Compare observable gain state without depending on result-list order."""
    return sorted(
        (
            gain.realized_gain_id,
            gain.lot_id,
            gain.quantity,
            gain.proceeds,
            gain.cost_basis,
            gain.gain_loss,
        )
        for gain in gains
    )
