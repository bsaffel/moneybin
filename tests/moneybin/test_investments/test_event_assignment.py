"""Whole-event assignment maximizes evidence coverage without arbitrary ties."""

from itertools import permutations

import pytest

from moneybin.investments.event_assignment import Candidate, solve_candidates


def test_coverage_beats_a_stronger_smaller_proposal() -> None:
    result = solve_candidates([
        Candidate(("a", "b"), "native"),
        Candidate(("a", "b", "c"), "fuzzy"),
    ])
    assert result.objective == (3, 0, 0)
    assert [(p.candidate.members, p.is_competing) for p in result.proposals] == [
        (("a", "b", "c"), False)
    ]


def test_equal_coverage_prefers_fewer_fuzzy_events_before_native_events() -> None:
    result = solve_candidates([
        Candidate(("a", "b"), "native"),
        Candidate(("c", "d"), "fuzzy"),
        Candidate(("a", "c"), "exact"),
        Candidate(("b", "d"), "exact"),
    ])
    assert result.objective == (4, 4, 0)
    assert {p.candidate.members for p in result.proposals} == {("a", "c"), ("b", "d")}
    assert not any(p.is_competing for p in result.proposals)


def test_native_breaks_an_equal_coverage_non_fuzzy_tie() -> None:
    result = solve_candidates([
        Candidate(("a", "b"), "native"),
        Candidate(("a", "c"), "exact"),
    ])
    assert result.objective == (2, 2, 2)
    assert [p.candidate.members for p in result.proposals] == [("a", "b")]


@pytest.mark.parametrize("order", list(permutations(range(4))))
def test_indistinguishable_two_by_two_has_all_four_competing_relationships(
    order: tuple[int, ...],
) -> None:
    candidates = [
        Candidate(("a", "c"), "exact"),
        Candidate(("a", "d"), "exact"),
        Candidate(("b", "c"), "exact"),
        Candidate(("b", "d"), "exact"),
    ]
    result = solve_candidates([candidates[i] for i in order])
    assert result.objective == (4, 4, 0)
    assert {p.candidate.members for p in result.proposals} == {
        ("a", "c"),
        ("a", "d"),
        ("b", "c"),
        ("b", "d"),
    }
    assert all(p.is_competing for p in result.proposals)


def test_inferior_ties_do_not_compete_with_unique_maximum() -> None:
    result = solve_candidates([
        Candidate(("a", "b"), "exact"),
        Candidate(("a", "c"), "fuzzy"),
        Candidate(("a", "d"), "fuzzy"),
    ])
    assert [(p.candidate.members, p.is_competing) for p in result.proposals] == [
        (("a", "b"), False)
    ]


def test_fixed_relationship_is_not_competing_when_other_members_compete() -> None:
    result = solve_candidates([
        Candidate(("a", "b"), "native"),
        Candidate(("c", "d"), "exact"),
        Candidate(("c", "e"), "exact"),
        Candidate(("b", "c"), "fuzzy"),
    ])
    assert {p.candidate.members: p.is_competing for p in result.proposals} == {
        ("a", "b"): False,
        ("c", "d"): True,
        ("c", "e"): True,
    }


def test_no_group_count_tiebreaker_prefers_two_pairs_over_one_four_way() -> None:
    result = solve_candidates([
        Candidate(("a", "b"), "exact"),
        Candidate(("c", "d"), "exact"),
        Candidate(("a", "b", "c", "d"), "exact"),
    ])
    assert result.objective == (4, 4, 0)
    assert len(result.proposals) == 3
    assert all(p.is_competing for p in result.proposals)


def test_replacements_cannot_claim_one_locked_component_twice() -> None:
    result = solve_candidates([
        Candidate(("a", "b"), "exact", ("prior",)),
        Candidate(("c", "d"), "exact", ("prior",)),
    ])
    assert result.objective == (2, 2, 0)
    assert len(result.proposals) == 2
    assert all(p.is_competing for p in result.proposals)


def test_empty_input_leaves_every_event_standalone() -> None:
    result = solve_candidates([])
    assert result.objective == (0, 0, 0)
    assert result.proposals == ()
