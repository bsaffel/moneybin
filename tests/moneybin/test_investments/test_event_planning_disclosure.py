"""Review disclosure and eligibility share the complete constrained solve."""

from moneybin.investments.event_planning import build_plan
from tests.moneybin.test_investments.test_event_planning import planning_inputs


def test_fixed_relationship_in_competing_component_is_not_auto_eligible() -> None:
    headers, legs, evidence = planning_inputs(
        ("manual", "one", "two", "three"), (("a", "b"), ("b", "c"), ("c", "d"))
    )
    # Add fifth event without changing the explicit four-node fixture factory.
    headers.append({**headers[-1], "source_event_key": "e", "source_origin": "four"})
    legs.append({
        **legs[-1],
        "source_event_key": "e",
        "source_origin": "four",
        "native_reference": "e",
        "observation_version": "plaid_e",
    })
    evidence[0]["has_validated_native_relationship"] = True
    evidence[1]["is_exact_economic_identity"] = False
    evidence.append({
        **evidence[-1],
        "right_source_event_key": "e",
        "right_native_reference": "e",
    })
    plan = build_plan(headers, legs, evidence)
    fixed = next(proposal for proposal in plan if proposal.members == ("a", "b"))
    assert not fixed.is_competing
    assert not fixed.auto_eligible


def test_competing_review_lists_alternatives_and_no_current_ledger_change() -> None:
    plan = build_plan(
        *planning_inputs(("manual", "one", "one"), (("a", "b"), ("a", "c")))
    )
    ab = next(proposal for proposal in plan if proposal.members == ("a", "b"))
    assert ab.alternatives == (("a", "c"),)
    assert ab.downstream_effects["golden_membership_changed"] is False
    assert ab.downstream_effects["source_events_to_consolidate"] == 2
    assert "lots" in ab.downstream_effects["affected_outputs"]
