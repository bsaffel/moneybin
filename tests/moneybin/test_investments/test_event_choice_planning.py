"""Observed choices must permit a coherent whole-event accounting result."""

from decimal import Decimal

from moneybin.investments.event_planning import build_plan
from tests.moneybin.test_investments.test_event_planning import planning_inputs


def test_incoherent_defaults_issue_accounting_choices_with_valid_combination() -> None:
    inputs = planning_inputs()
    inputs[1][1].update(
        quantity=Decimal("0.999999"), price=None, amount=Decimal("-100.01")
    )
    inputs[2][0]["is_exact_economic_identity"] = False
    plan = build_plan(*inputs)
    assert len(plan) == 1
    assert {conflict["field"] for conflict in plan[0].field_choices} == {
        "quantity",
        "amount",
    }
    assert not plan[0].auto_eligible


def test_no_coherent_observed_combination_creates_no_unresolvable_proposal() -> None:
    inputs = planning_inputs()
    inputs[1][0]["amount"] = Decimal("-110")
    inputs[1][1]["amount"] = Decimal("-110.01")
    assert build_plan(*inputs) == ()


def test_present_original_date_outranks_missing_without_conflict() -> None:
    inputs = planning_inputs()
    inputs[1][0]["original_acquisition_date"] = inputs[1][0]["trade_date"]
    plan = build_plan(*inputs)
    assert plan[0].field_choices == ()
    assert plan[0].auto_eligible


def test_native_numeric_conflict_uses_symmetric_relative_scale() -> None:
    inputs = planning_inputs()
    inputs[2][0]["has_validated_native_relationship"] = True
    inputs[1][0].update(
        type="transfer_in", quantity=Decimal("0"), price=None, amount=None
    )
    inputs[1][1].update(
        type="transfer_in", quantity=Decimal("0.000001"), price=None, amount=None
    )
    inputs[0][0]["event_type"] = inputs[0][1]["event_type"] = "transfer_in"
    inputs[2][0]["type"] = "transfer_in"
    before = build_plan(*inputs)[0]
    assert before.field_choices == ()
    inputs[1].reverse()
    assert build_plan(*inputs)[0].field_choices == before.field_choices
    inputs[1][0]["quantity"] = Decimal("0.00000101")
    after = build_plan(*inputs)[0]
    assert [conflict["field"] for conflict in after.field_choices] == ["quantity"]


def test_reconciled_cash_equations_keep_different_prices_within_tolerance() -> None:
    inputs = planning_inputs()
    for leg in inputs[1]:
        leg.update(quantity=Decimal("0.000001"), amount=Decimal("-0.01"))
    inputs[1][1]["price"] = Decimal("200")
    inputs[2][0]["is_exact_economic_identity"] = False
    plan = build_plan(*inputs)
    assert len(plan) == 1
    assert plan[0].field_choices == ()
