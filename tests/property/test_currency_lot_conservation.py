"""Property proof for Currency-lot basis conservation."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from hypothesis import given
from hypothesis import strategies as st

from moneybin.currency_lots import sqlmesh_loader

D = Decimal


def _conversion(
    index: int,
    *,
    foreign_units: Decimal,
    home_value: Decimal,
    disposal: bool = False,
) -> sqlmesh_loader.CurrencyConversionRow:
    event_date = date(2026, 1, 1) + timedelta(days=index)
    if disposal:
        from_account, to_account = "acct-eur", "acct-usd"
        from_amount, from_currency = foreign_units, "EUR"
        to_amount, to_currency = home_value, "USD"
    else:
        from_account, to_account = "acct-usd", "acct-eur"
        from_amount, from_currency = home_value, "USD"
        to_amount, to_currency = foreign_units, "EUR"
    return sqlmesh_loader.CurrencyConversionRow(
        conversion_id=f"fxc_{'dispose' if disposal else 'acquire'}_{index}",
        source_shape="linked_two_row",
        transfer_pair_id=f"decision-{index}",
        from_transaction_id=f"from-{index}",
        to_transaction_id=f"to-{index}",
        from_account_id=from_account,
        to_account_id=to_account,
        from_date=event_date,
        to_date=event_date,
        from_amount=from_amount,
        from_currency=from_currency,
        to_amount=to_amount,
        to_currency=to_currency,
        executed_rate=D("1.00000000"),
        home_currency="USD",
        home_value=home_value,
        valuation_rate=D("1.00000000"),
        valuation_rate_date=event_date,
        valuation_source_type="actual",
        from_source_type="manual",
        from_source_origin="property",
        from_source_transaction_id=f"native-from-{index}",
        to_source_type="manual",
        to_source_origin="property",
        to_source_transaction_id=f"native-to-{index}",
        coverage_status="complete",
        coverage_reason=None,
        updated_at=None,
    )


@st.composite
def _bounded_ledger(
    draw: st.DrawFn,
) -> tuple[list[sqlmesh_loader.CurrencyConversionRow], Decimal]:
    quantities = draw(
        st.lists(
            st.integers(min_value=1, max_value=100),
            min_size=1,
            max_size=5,
        )
    )
    bases = draw(
        st.lists(
            st.integers(min_value=1, max_value=10000),
            min_size=len(quantities),
            max_size=len(quantities),
        )
    )
    total_quantity = sum(quantities)
    disposal_quantity = draw(st.integers(min_value=0, max_value=total_quantity))
    acquisitions = [
        _conversion(
            index,
            foreign_units=D(quantity),
            home_value=D(basis) / D("100"),
        )
        for index, (quantity, basis) in enumerate(zip(quantities, bases, strict=True))
    ]
    events = list(acquisitions)
    if disposal_quantity:
        events.append(
            _conversion(
                len(events) + 1,
                foreign_units=D(disposal_quantity),
                home_value=D(disposal_quantity),
                disposal=True,
            )
        )
    contributed_basis = sum(
        (event.home_value or D("0") for event in acquisitions), D("0")
    )
    return events, contributed_basis


@given(ledger=_bounded_ledger(), method=st.sampled_from(["fifo", "average"]))
def test_realized_plus_open_basis_equals_contributed_basis(
    ledger: tuple[list[sqlmesh_loader.CurrencyConversionRow], Decimal], method: str
) -> None:
    events, contributed_basis = ledger

    result = sqlmesh_loader.derive_currency_accounting(events, (), {"acct-eur": method})

    realized_basis = sum(
        (gain.cost_basis or D("0"))
        for gain in result.gains
        if gain.coverage_status == "complete"
    )
    open_basis = sum(
        (lot.cost_basis_remaining or D("0"))
        for lot in result.lots
        if lot.coverage_status == "complete"
    )
    assert realized_basis + open_basis == contributed_basis


def test_oversold_excess_is_incomplete_and_excluded_from_conservation() -> None:
    acquisition = _conversion(0, foreign_units=D("10"), home_value=D("25.00"))
    disposal = _conversion(
        1,
        foreign_units=D("15"),
        home_value=D("45.00"),
        disposal=True,
    )

    result = sqlmesh_loader.derive_currency_accounting((acquisition, disposal), (), {})

    complete_basis = sum(
        (gain.cost_basis or D("0"))
        for gain in result.gains
        if gain.coverage_status == "complete"
    )
    incomplete = [gain for gain in result.gains if gain.coverage_status == "incomplete"]
    assert complete_basis == D("25.00")
    assert len(incomplete) == 1
    assert incomplete[0].disposed_amount == D("5")
    assert incomplete[0].cost_basis is None
    assert incomplete[0].gain_loss is None
    assert incomplete[0].coverage_reason == "negative_inventory"
