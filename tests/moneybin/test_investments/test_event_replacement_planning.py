"""Ratified components remain indivisible under replacement planning."""

from typing import Any

from moneybin.investments.event_planning import build_plan
from tests.moneybin.test_investments.test_event_planning import planning_inputs


def test_active_match_cannot_split_into_two_later_pairs() -> None:
    inputs = planning_inputs(
        ("manual", "one", "two", "three"), (("a", "b"), ("a", "c"), ("b", "d"))
    )
    locked: dict[str, Any] = {
        "decision_id": "prior",
        "members": ("a", "b"),
        "reserved_rows": (("manual", "manual", "a"), ("plaid", "one", "b")),
        "field_resolutions": [],
        "curation_impact": [],
    }
    assert build_plan(*inputs, locked_components=[locked]) == ()


def test_complete_replacement_names_prior_component_and_curation() -> None:
    inputs = planning_inputs(
        ("manual", "one", "two"), (("a", "b"), ("a", "c"), ("b", "c"))
    )
    locked: dict[str, Any] = {
        "decision_id": "prior",
        "members": ("a", "b"),
        "reserved_rows": (("manual", "manual", "a"), ("plaid", "one", "b")),
        "field_resolutions": [{"field": "trade_date", "choice_id": "choice"}],
        "curation_impact": [{"selection_id": "selection"}],
    }
    plan = build_plan(*inputs, locked_components=[locked])
    assert len(plan) == 1
    assert plan[0].members == ("a", "b", "c")
    assert plan[0].supersedes_decision_ids == ("prior",)
    assert not plan[0].auto_eligible
    assert plan[0].supersession[0]["field_resolutions"] == locked["field_resolutions"]
    assert plan[0].supersession[0]["curation_impact"] == locked["curation_impact"]


def test_missing_reserved_successor_blocks_entire_component() -> None:
    inputs = planning_inputs(
        ("manual", "one", "two"), (("a", "b"), ("a", "c"), ("b", "c"))
    )
    locked: dict[str, Any] = {
        "decision_id": "prior",
        "members": ("a", "old"),
        "reserved_rows": (("manual", "manual", "a"), ("plaid", "one", "missing")),
        "field_resolutions": [],
        "curation_impact": [],
    }
    plan = build_plan(*inputs, locked_components=[locked])
    assert [p.members for p in plan] == [("b", "c")]


def test_changed_curation_impact_changes_replacement_fingerprint() -> None:
    inputs = planning_inputs(
        ("manual", "one", "two"), (("a", "b"), ("a", "c"), ("b", "c"))
    )
    locked: dict[str, Any] = {
        "decision_id": "prior",
        "members": ("a", "b"),
        "reserved_rows": (("manual", "manual", "a"), ("plaid", "one", "b")),
        "field_resolutions": [],
        "curation_impact": [],
    }
    before = build_plan(*inputs, locked_components=[locked])[0]
    locked["curation_impact"] = [{"selection_id": "new_selection"}]
    after = build_plan(*inputs, locked_components=[locked])[0]
    assert before.relationship_fingerprint != after.relationship_fingerprint
    assert before.candidate_graph_fingerprint != after.candidate_graph_fingerprint


def test_stale_match_with_same_event_keys_proposes_current_revisions() -> None:
    inputs = planning_inputs()
    inputs[1][1]["observation_version"] = "plaid_revised"
    locked: dict[str, Any] = {
        "decision_id": "prior",
        "status": "stale",
        "members": ("a", "b"),
        "reserved_rows": (("manual", "manual", "a"), ("plaid", "plaid", "b")),
        "field_resolutions": [],
        "curation_impact": [],
    }
    plan = build_plan(*inputs, locked_components=[locked])
    assert len(plan) == 1
    assert plan[0].members == ("a", "b")
    assert plan[0].supersedes_decision_ids == ("prior",)
    assert not plan[0].auto_eligible
