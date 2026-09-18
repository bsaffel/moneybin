"""Field-choice Cartesian search must not explode on an incoherent compound event."""

from datetime import date, timedelta
from decimal import Decimal
from time import perf_counter
from typing import Any

import pytest

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.investments.event_assignment import MAX_COMPONENT_STATES
from moneybin.investments.event_choices import issue_choices


def _leg(role: str, event_type: str, index: int, **overrides: Any) -> dict[str, Any]:
    leg = {
        "leg_role": role,
        "type": event_type,
        "subtype": "qualified" if index % 2 == 0 else "non_qualified",
        "source_type": "plaid" if index else "manual",
        "source_origin": f"origin{index}",
        "native_reference": f"ref{role}{index}",
        "observation_version": f"v{index}",
        "trade_date": date(2026, 1, 1) + timedelta(days=index),
        "trade_date_basis": "explicit",
        "settlement_date": date(2026, 1, 5) + timedelta(days=index),
        "original_acquisition_date": None,
        "quantity": Decimal("10"),
        "price": Decimal("5"),
        "amount": Decimal("-50"),
        "fees": Decimal("0"),
    }
    leg.update(overrides)
    return leg


def _incoherent_legs(n_origins: int) -> list[dict[str, Any]]:
    """A 4-origin reinvest whose amounts never reconcile across sources.

    Mirrors the review's own described case: conflicts on both legs' dates,
    subtype, and accounting fields, with no coherent selection — so
    ``issue_choices`` must exhaust the field-choice product before it can
    return ``None``.
    """
    legs: list[dict[str, Any]] = []
    for role, event_type in (("acquisition", "reinvest"), ("income", "income")):
        for index in range(n_origins):
            legs.append(
                _leg(
                    role,
                    event_type,
                    index,
                    quantity=Decimal(str(10 + index)),
                    price=Decimal(str(5 + index)),
                    amount=Decimal(str(-(50 + index * 7))),
                    fees=Decimal(str(index)),
                )
            )
    return legs


def test_incoherent_compound_event_over_the_combination_bound_fails_visibly() -> None:
    """A field-choice search with no coherent combination refuses rather than hang.

    Regression for the PR #608 review finding: the review's own described
    case (a 4-origin reinvest, conflicts on dates/subtype/accounting fields)
    measures ~67M candidate combinations with no coherent selection — well
    over 15s unbounded locally. This refuses in well under a second instead.
    """
    legs = _incoherent_legs(4)
    start = perf_counter()

    with pytest.raises(UserError) as exc_info:
        issue_choices(legs, relationship="rel", date_threshold_days=0)

    # Generous relative to the sub-second refusal measured locally — the
    # point is that the bound aborts the search, not a precise budget.
    assert perf_counter() - start < 5.0
    assert exc_info.value.code == error_codes.INVESTMENT_MATCH_COMPONENT_TOO_LARGE
    assert str(MAX_COMPONENT_STATES) in exc_info.value.message


def test_many_non_accounting_conflicts_resolve_immediately_despite_huge_product() -> (
    None
):
    """A large but immediately-coherent search must not be rejected by size alone.

    200 origins per leg role with identical, self-consistent accounting
    fields (only dates/subtype disagree, which ``_coherent`` never
    inspects) gives a theoretical field-choice product in the billions —
    but the very first combination tried is coherent, so real work stays
    O(1). A bound on the *theoretical* product size would wrongly reject
    this cheap, legitimate input; the bound here is on combinations
    actually examined, so it is never reached.
    """
    legs: list[dict[str, Any]] = []
    for role, event_type, sign in (
        ("acquisition", "reinvest", -1),
        ("income", "income", 1),
    ):
        for index in range(200):
            legs.append(_leg(role, event_type, index, amount=Decimal(sign * 50)))

    start = perf_counter()
    result = issue_choices(legs, relationship="rel", date_threshold_days=0)
    assert perf_counter() - start < 5.0
    assert result is not None
