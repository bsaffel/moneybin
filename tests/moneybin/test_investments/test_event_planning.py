"""Proposal construction binds evidence, whole events, and review choices."""

from copy import deepcopy
from datetime import date
from decimal import Decimal
from typing import Any

from moneybin.investments.event_planning import build_plan


def planning_inputs(
    origins: tuple[str, ...] = ("manual", "plaid"),
    pairs: tuple[tuple[str, str], ...] = (("a", "b"),),
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    headers: list[dict[str, Any]] = []
    legs: list[dict[str, Any]] = []
    for key, origin in zip("abcd", origins, strict=False):
        source_type = "manual" if origin == "manual" else "plaid"
        headers.append({
            "source_event_key": key,
            "source_type": source_type,
            "source_origin": origin,
            "event_type": "buy",
            "member_count": 1,
            "is_match_eligible": True,
            "supports_split": False,
        })
        legs.append({
            "source_event_key": key,
            "source_type": source_type,
            "source_origin": origin,
            "native_reference": key,
            "observation_version": f"{source_type}_{key}",
            "leg_role": "acquisition",
            "type": "buy",
            "subtype": "buy",
            "account_id": "account",
            "security_id": "security",
            "account_identity_generation": "account_generation",
            "security_identity_generation": "security_generation",
            "source_currency_code": "USD",
            "account_currency_code": "USD",
            "currency_code": "USD",
            "trade_date": date(2026, 1, 10),
            "trade_date_basis": "posting_fallback",
            "settlement_date": None,
            "original_acquisition_date": None,
            "description": "Synthetic",
            "quantity": Decimal("1"),
            "price": Decimal("100"),
            "amount": Decimal("-100"),
            "fees": Decimal("0"),
        })
    evidence = [
        {
            "left_source_event_key": a,
            "right_source_event_key": b,
            "left_native_reference": a,
            "right_native_reference": b,
            "leg_role": "acquisition",
            "type": "buy",
            "is_candidate": True,
            "is_exact_economic_identity": True,
            "has_validated_native_relationship": False,
            "trade_date_conflict": False,
            "original_acquisition_date_conflict": False,
            "subtype_conflict": False,
            "date_threshold_days": 5,
            "date_distance_days": 0,
        }
        for a, b in pairs
    ]
    return headers, legs, evidence


def test_unique_exact_proposal_is_measurement_only_auto_eligible() -> None:
    plan = build_plan(*planning_inputs())
    assert len(plan) == 1
    assert plan[0].members == ("a", "b")
    assert plan[0].auto_eligible is True
    assert plan[0].is_competing is False


def test_neighbor_chain_does_not_create_ineligible_n_way_endpoints() -> None:
    plan = build_plan(
        *planning_inputs(("manual", "one", "two"), (("a", "b"), ("b", "c")))
    )
    assert {p.members for p in plan} == {("a", "b"), ("b", "c")}
    assert all(p.is_competing and not p.auto_eligible for p in plan)


def test_n_way_uses_weakest_pair_and_coverage_first() -> None:
    inputs = planning_inputs(
        ("manual", "one", "two"), (("a", "b"), ("b", "c"), ("a", "c"))
    )
    inputs[2][-1]["is_exact_economic_identity"] = False
    plan = build_plan(*inputs)
    assert [(p.members, p.confidence_band, p.auto_eligible) for p in plan] == [
        (("a", "b", "c"), "fuzzy", False)
    ]


def test_same_source_origin_cannot_form_proposal_even_with_pair_evidence() -> None:
    assert build_plan(*planning_inputs(("one", "one"))) == ()


def test_ineligible_adapter_event_cannot_form_proposal() -> None:
    inputs = planning_inputs()
    inputs[0][0]["is_match_eligible"] = False
    assert build_plan(*inputs) == ()


def test_total_leg_correspondence_required() -> None:
    inputs = planning_inputs()
    inputs[0][0]["member_count"] = 2
    assert build_plan(*inputs) == ()


def test_fingerprints_ignore_input_order_and_display_metadata() -> None:
    inputs = planning_inputs()
    before = build_plan(*inputs)[0]
    inputs[0].reverse()
    inputs[1].reverse()
    inputs[1][0]["account_display_name"] = "Changed label"
    inputs[1][0]["account_updated_at"] = "later"
    after = build_plan(*inputs)[0]
    assert before.relationship_fingerprint == after.relationship_fingerprint
    assert before.candidate_graph_fingerprint == after.candidate_graph_fingerprint


def test_new_alternative_changes_graph_but_not_relationship_fingerprint() -> None:
    old = build_plan(*planning_inputs())[0]
    new = build_plan(
        *planning_inputs(("manual", "plaid", "plaid"), (("a", "b"), ("a", "c")))
    )
    ab = next(p for p in new if p.members == ("a", "b"))
    assert old.relationship_fingerprint == ab.relationship_fingerprint
    assert old.candidate_graph_fingerprint != ab.candidate_graph_fingerprint
    assert ab.is_competing


def test_revision_and_identity_generation_change_relationship() -> None:
    inputs = planning_inputs()
    old = build_plan(*inputs)[0].relationship_fingerprint
    for field in (
        "observation_version",
        "account_identity_generation",
        "security_identity_generation",
        "currency_code",
    ):
        changed = deepcopy(inputs)
        changed[1][0][field] = "changed"
        assert build_plan(*changed)[0].relationship_fingerprint != old


def test_rejected_pair_prunes_unchanged_superset_but_not_other_pair() -> None:
    inputs = planning_inputs(("manual", "one", "two"), (("a", "c"),))
    ac = build_plan(*inputs)[0]
    rejection = {
        "status": "rejected",
        "members": ac.members,
        "relationship_fingerprint": ac.relationship_fingerprint,
    }
    inputs = planning_inputs(
        ("manual", "one", "two"), (("a", "b"), ("a", "c"), ("b", "c"))
    )
    plan = build_plan(*inputs, decisions=[rejection])
    assert {p.members for p in plan} == {("a", "b"), ("b", "c")}
    assert build_plan(*inputs)[0].members == ("a", "b", "c")


def test_rejected_n_way_does_not_decompose_into_pair_rejections() -> None:
    inputs = planning_inputs(
        ("manual", "one", "two"), (("a", "b"), ("a", "c"), ("b", "c"))
    )
    abc = build_plan(*inputs)[0]
    plan = build_plan(
        *inputs,
        decisions=[
            {
                "status": "rejected",
                "members": abc.members,
                "relationship_fingerprint": abc.relationship_fingerprint,
            }
        ],
    )
    assert {p.members for p in plan} == {("a", "b"), ("a", "c"), ("b", "c")}


def test_explicit_dates_issue_stable_observed_choice_ids() -> None:
    inputs = planning_inputs()
    inputs[1][0]["trade_date_basis"] = "explicit"
    inputs[1][1]["trade_date_basis"] = "explicit"
    inputs[1][1]["trade_date"] = date(2026, 1, 11)
    inputs[2][0]["trade_date_conflict"] = True
    first = build_plan(*inputs)[0]
    assert not first.auto_eligible
    assert len(first.field_choices) == 1
    conflict = first.field_choices[0]
    assert conflict["field"] == "trade_date"
    assert conflict["conflict_id"]
    assert len({choice["choice_id"] for choice in conflict["choices"]}) == 2
    inputs[1].reverse()
    assert build_plan(*inputs)[0].field_choices == first.field_choices


def test_original_acquisition_date_disagreement_is_always_material() -> None:
    inputs = planning_inputs()
    inputs[1][0]["original_acquisition_date"] = date(2020, 1, 1)
    inputs[1][1]["original_acquisition_date"] = date(2020, 1, 2)
    inputs[2][0]["original_acquisition_date_conflict"] = True
    plan = build_plan(*inputs)
    assert plan[0].field_choices[0]["field"] == "original_acquisition_date"
    assert not plan[0].auto_eligible
