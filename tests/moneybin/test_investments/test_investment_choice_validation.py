"""Issued observed choices must form a complete, coherent selected event."""

from copy import deepcopy
from dataclasses import replace
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments import event_choices, event_fingerprints
from moneybin.investments.event_planning import build_plan
from moneybin.repositories.investment_match_decisions_repo import (
    InvestmentMatchDecisionsRepo,
)
from moneybin.services.investment_matching_service import InvestmentMatchingService
from moneybin.tables import (
    INVESTMENT_EVENT_EVIDENCE,
    INVESTMENT_EVENT_HEADERS,
    INVESTMENT_EVENT_LEGS,
)
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
    seed_manual_event,
    seed_plaid_event,
)
from tests.moneybin.test_investments.test_event_planning import planning_inputs


def _issued(legs: list[dict[str, Any]]) -> tuple[dict[str, Any], ...]:
    choices = event_choices.issue_choices(
        legs, relationship="relationship", date_threshold_days=5
    )
    assert choices is not None
    return choices


def _selections(
    conflicts: tuple[dict[str, Any], ...], source: str = "plaid"
) -> list[dict[str, str]]:
    return [
        {
            "conflict_id": conflict["conflict_id"],
            "choice_id": next(
                choice["choice_id"]
                for choice in conflict["choices"]
                if choice["source_type"] == source
            ),
        }
        for conflict in conflicts
    ]


def _accounting_conflict() -> list[dict[str, Any]]:
    legs = planning_inputs()[1]
    legs[0].update(quantity=Decimal("2"), price=Decimal("50"))
    return legs


@pytest.mark.parametrize("reverse", [False, True])
def test_defaults_preserve_present_values_explicit_dates_and_source_precedence(
    reverse: bool,
) -> None:
    legs = planning_inputs()[1]
    legs[0].update(
        trade_date=date(2026, 1, 9),
        trade_date_basis="explicit",
        original_acquisition_date=date(2025, 1, 1),
    )
    if reverse:
        legs.reverse()
    before = deepcopy(legs)
    selected = event_choices.validate_selection(legs, _issued(legs), [])
    assert len(selected) == 1
    assert selected[0]["trade_date"] == date(2026, 1, 9)
    assert selected[0]["trade_date_basis"] == "explicit"
    assert selected[0]["original_acquisition_date"] == date(2025, 1, 1)
    assert selected[0]["source_type"] == "plaid"
    assert selected[0]["subtype"] == "buy"
    assert legs == before


@pytest.mark.parametrize("source", ["manual", "plaid"])
def test_each_observed_coherent_accounting_choice_is_selectable(source: str) -> None:
    legs = _accounting_conflict()
    conflicts = _issued(legs)
    assert {conflict["field"] for conflict in conflicts} == {"quantity", "price"}
    selected = event_choices.validate_selection(
        legs, conflicts, _selections(conflicts, source)
    )
    expected = next(leg for leg in legs if leg["source_type"] == source)
    assert selected[0]["quantity"] == expected["quantity"]
    assert selected[0]["price"] == expected["price"]


def test_below_tolerance_price_is_offered_when_alternate_quantity_needs_it() -> None:
    legs = planning_inputs()[1]
    legs[0].update(
        quantity=Decimal("1000"), price=Decimal("100"), amount=Decimal("-100000")
    )
    legs[1].update(
        quantity=Decimal("1000.01"),
        price=Decimal("99.99900001"),
        amount=Decimal("-100000"),
    )
    conflicts = _issued(legs)
    assert {conflict["field"] for conflict in conflicts} == {"quantity", "price"}
    for source in ("manual", "plaid"):
        assert event_choices.validate_selection(
            legs, conflicts, _selections(conflicts, source)
        )


@pytest.mark.parametrize(
    "invalid", ["missing", "duplicate", "unknown_conflict", "unknown_choice", "stale"]
)
def test_invalid_choice_sets_fail_with_sanitized_input_error(invalid: str) -> None:
    legs = planning_inputs()[1]
    legs[0].update(trade_date=date(2026, 1, 9), trade_date_basis="explicit")
    legs[1]["trade_date_basis"] = "explicit"
    conflicts = _issued(legs)
    selections = _selections(conflicts)
    if invalid == "missing":
        selections.clear()
    elif invalid == "duplicate":
        selections += deepcopy(selections)
    elif invalid == "unknown_conflict":
        selections[0]["conflict_id"] = "private-untrusted-value"
    elif invalid == "unknown_choice":
        selections[0]["choice_id"] = "private-untrusted-value"
    else:
        legs[1]["observation_version"] = "corrected"
        conflicts = _issued(legs)
    with pytest.raises(UserError) as exc:
        event_choices.validate_selection(legs, conflicts, selections)
    assert exc.value.code == error_codes.MUTATION_INVALID_INPUT
    assert "private-untrusted-value" not in exc.value.message


def test_individually_issued_choices_cannot_create_incoherent_accounting() -> None:
    legs = _accounting_conflict()
    conflicts = _issued(legs)
    selections = _selections(conflicts)
    selections[0] = _selections(conflicts, "manual")[0]
    with pytest.raises(UserError) as exc:
        event_choices.validate_selection(legs, conflicts, selections)
    assert exc.value.code == error_codes.MUTATION_INVALID_INPUT


def test_no_conflict_proposal_rejects_unissued_choices() -> None:
    legs = planning_inputs()[1]
    with pytest.raises(UserError) as exc:
        event_choices.validate_selection(
            legs, (), [{"conflict_id": "unknown", "choice_id": "unknown"}]
        )
    assert exc.value.code == error_codes.MUTATION_INVALID_INPUT


def test_aggregator_ties_use_stable_source_tuple() -> None:
    legs = planning_inputs(("origin_b", "origin_a"))[1]
    legs[0]["description"] = "B"
    legs[1]["description"] = "A"
    assert event_choices.validate_selection(legs, (), [])[0]["description"] == "A"
    legs.reverse()
    assert event_choices.validate_selection(legs, (), [])[0]["description"] == "A"


def test_selected_trade_date_carries_its_observed_date_basis() -> None:
    legs = planning_inputs()[1]
    legs[0].update(trade_date=date(2026, 1, 1), trade_date_basis="explicit")
    conflicts = _issued(legs)
    selected = event_choices.validate_selection(
        legs, conflicts, _selections(conflicts, "plaid")
    )
    assert selected[0]["trade_date"] == date(2026, 1, 10)
    assert selected[0]["trade_date_basis"] == "posting_fallback"


@pytest.mark.parametrize(
    "changes",
    [
        {"quantity": Decimal("-1"), "amount": Decimal("100")},
        {"quantity": None},
        {"amount": None},
        {"security_id": None},
        {"type": "deposit", "quantity": None, "amount": Decimal("100")},
        {"type": "dividend", "amount": Decimal("100")},
        {"type": "fee", "quantity": None, "amount": Decimal("100")},
        {"type": "split", "quantity": Decimal("2")},
    ],
)
def test_selected_leg_enforces_ledger_required_forbidden_and_sign_fields(
    changes: dict[str, Any],
) -> None:
    leg = planning_inputs()[1][0]
    leg.update(changes)
    with pytest.raises(UserError) as exc:
        event_choices.validate_selection([leg], (), [])
    assert exc.value.code == error_codes.MUTATION_INVALID_INPUT


def _reinvest() -> list[dict[str, Any]]:
    acquisition = planning_inputs()[1][0]
    acquisition.update(type="reinvest", subtype="dividend")
    income = dict(acquisition)
    income.update(
        leg_role="income",
        type="dividend",
        subtype="qualified",
        quantity=None,
        price=None,
        amount=Decimal("100"),
        native_reference="income",
    )
    return [acquisition, income]


def test_complete_reinvest_uses_normalized_subtype_and_validates_both_legs() -> None:
    legs = _reinvest()
    assert len(event_choices.validate_selection(legs, _issued(legs), [])) == 2


@pytest.mark.parametrize(
    "invalid",
    [
        "missing_income",
        "wrong_income_type",
        "wrong_currency",
        "wrong_security",
        "underfunded",
        "income_fee",
        "extra_leg",
    ],
)
def test_selected_reinvest_requires_complete_corresponding_income(invalid: str) -> None:
    legs = _reinvest()
    if invalid == "missing_income":
        legs.pop()
    elif invalid == "wrong_income_type":
        legs[1]["type"] = "interest"
    elif invalid == "wrong_currency":
        legs[1]["currency_code"] = "EUR"
    elif invalid == "wrong_security":
        legs[1]["security_id"] = "another_security"
    elif invalid == "underfunded":
        legs[1]["amount"] = Decimal("99")
    elif invalid == "income_fee":
        legs[1]["fees"] = Decimal("1")
    else:
        legs.append(dict(legs[1], leg_role="cash"))
    with pytest.raises(UserError) as exc:
        event_choices.validate_selection(legs, (), [])
    assert exc.value.code == error_codes.MUTATION_INVALID_INPUT


def test_persisted_previous_algorithm_proposal_is_stale_after_choice_upgrade(
    comparison_db: Database,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_manual_event(comparison_db, "manual")
    seed_plaid_event(comparison_db, "native")
    install_comparison_models(comparison_db)
    facts: list[list[dict[str, Any]]] = []
    for table in (
        INVESTMENT_EVENT_HEADERS,
        INVESTMENT_EVENT_LEGS,
        INVESTMENT_EVENT_EVIDENCE,
    ):
        cursor = comparison_db.execute(
            f"SELECT * FROM {table.full_name}"  # noqa: S608  # fixed TableRef
        )
        assert cursor.description is not None
        names = [column[0] for column in cursor.description]
        facts.append([dict(zip(names, row, strict=True)) for row in cursor.fetchall()])
    with monkeypatch.context() as old:
        old.setattr(event_fingerprints, "ALGORITHM_VERSION", "investment_match_v1")
        proposal = replace(
            build_plan(*facts)[0], algorithm_version="investment_match_v1"
        )
    proposal_id = InvestmentMatchDecisionsRepo(comparison_db).insert_pending(
        proposal, actor="test"
    )
    service = InvestmentMatchingService(comparison_db)
    with pytest.raises(UserError, match="stale"):
        service.prepare_decision(proposal_id, decision="reject")
    result = service.run()
    assert result.stale == 1
    assert result.proposed == 1
    pending = service.pending()
    assert len(pending) == 1
    assert pending[0]["algorithm_version"] == event_fingerprints.ALGORITHM_VERSION
    assert pending[0]["proposal_id"] != proposal_id
