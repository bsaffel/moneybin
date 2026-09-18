"""Whole-event coherence must hold for every event type, not just ``buy``.

``_coherent`` reconciles each leg against the cash identity
``quantity * price + amount + fees ≈ 0``. That identity only balances because
AGENTS.md's sign convention puts ``quantity`` and ``amount`` on opposite sides:
a buy takes shares in and cash out, a sell does the reverse. Every other test
in this package builds on a ``buy`` fixture, so a regression that dropped the
disposal direction — or exempted the wrong types from the identity — would
leave the suite green while the ledger silently mis-signed every sale.
"""

from datetime import date
from decimal import Decimal
from typing import Any

from moneybin.investments.event_choices import issue_choices


def _leg(role: str, event_type: str, **overrides: Any) -> dict[str, Any]:
    """One observed leg, keyed by role so a caller reads as an event."""
    leg: dict[str, Any] = {
        "leg_role": role,
        "type": event_type,
        "subtype": event_type,
        "source_type": "manual",
        "source_origin": "manual",
        "native_reference": f"ref_{role}",
        "observation_version": f"v_{role}",
        "trade_date": date(2026, 1, 10),
        "trade_date_basis": "explicit",
        "settlement_date": None,
        "original_acquisition_date": None,
        "quantity": None,
        "price": None,
        "amount": None,
        "fees": Decimal("0"),
    }
    leg.update(overrides)
    return leg


def _choices(*legs: dict[str, Any]) -> tuple[dict[str, Any], ...] | None:
    return issue_choices(list(legs), relationship="rel", date_threshold_days=5)


def test_sell_reconciles_when_quantity_opposes_amount() -> None:
    """Ten shares leaving at $5 bring $50 in: negative quantity, positive amount."""
    sell = _leg(
        "disposal",
        "sell",
        quantity=Decimal("-10"),
        price=Decimal("5"),
        amount=Decimal("50"),
    )
    assert _choices(sell) == ()


def test_sell_carrying_acquisition_signs_is_incoherent() -> None:
    """A sale written with a buy's signs claims shares and cash both arrived."""
    mis_signed = _leg(
        "disposal",
        "sell",
        quantity=Decimal("10"),
        price=Decimal("5"),
        amount=Decimal("50"),
    )
    assert _choices(mis_signed) is None


def test_buy_paying_positive_cash_is_incoherent() -> None:
    """Shares arriving must cost cash; a positive amount doubles the identity."""
    mis_signed = _leg(
        "acquisition",
        "buy",
        quantity=Decimal("10"),
        price=Decimal("5"),
        amount=Decimal("50"),
    )
    assert _choices(mis_signed) is None


def test_fees_fold_into_the_identity_in_both_directions() -> None:
    """Fees are always positive: they deepen a buy's outflow and shrink a sell's."""
    buy = _leg(
        "acquisition",
        "buy",
        quantity=Decimal("10"),
        price=Decimal("5"),
        amount=Decimal("-51"),
        fees=Decimal("1"),
    )
    sell = _leg(
        "disposal",
        "sell",
        quantity=Decimal("-10"),
        price=Decimal("5"),
        amount=Decimal("49"),
        fees=Decimal("1"),
    )
    assert _choices(buy) == ()
    assert _choices(sell) == ()


def test_fee_signed_as_a_rebate_breaks_the_identity() -> None:
    """A negative fee on a buy understates the cash the trade actually consumed."""
    buy = _leg(
        "acquisition",
        "buy",
        quantity=Decimal("10"),
        price=Decimal("5"),
        amount=Decimal("-51"),
        fees=Decimal("-1"),
    )
    assert _choices(buy) is None


def test_cash_only_types_are_exempt_from_the_quantity_identity() -> None:
    """Dividend, interest, and fee move cash with no shares to reconcile."""
    for event_type, role in (
        ("dividend", "income"),
        ("interest", "income"),
        ("fee", "cash"),
    ):
        leg = _leg(role, event_type, amount=Decimal("50"))
        assert _choices(leg) == (), event_type


def test_reinvest_requires_its_income_leg_to_cancel_the_acquisition() -> None:
    """The dividend funds the purchase, so the two legs net to zero cash."""
    acquisition = _leg(
        "acquisition",
        "reinvest",
        quantity=Decimal("10"),
        price=Decimal("5"),
        amount=Decimal("-50"),
    )
    income = _leg("income", "dividend", amount=Decimal("50"))
    assert _choices(acquisition, income) == ()


def test_reinvest_income_leg_signed_as_an_outflow_is_incoherent() -> None:
    """Two negative legs claim the event consumed cash it never received."""
    acquisition = _leg(
        "acquisition",
        "reinvest",
        quantity=Decimal("10"),
        price=Decimal("5"),
        amount=Decimal("-50"),
    )
    income = _leg("income", "dividend", amount=Decimal("-50"))
    assert _choices(acquisition, income) is None


def test_reinvest_income_leg_that_underfunds_the_purchase_is_incoherent() -> None:
    """Right signs, wrong magnitude: $40 of dividend cannot buy $50 of shares."""
    acquisition = _leg(
        "acquisition",
        "reinvest",
        quantity=Decimal("10"),
        price=Decimal("5"),
        amount=Decimal("-50"),
    )
    income = _leg("income", "dividend", amount=Decimal("40"))
    assert _choices(acquisition, income) is None
