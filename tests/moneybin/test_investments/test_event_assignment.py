"""Whole-event assignment maximizes evidence coverage without arbitrary ties."""

from itertools import permutations
from time import perf_counter

import pytest

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.investments.event_assignment import (
    MAX_COMPONENT_STATES,
    Candidate,
    solve_candidates,
)


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


def test_dense_component_over_the_state_bound_fails_visibly() -> None:
    """A densely-overlapping component refuses rather than risk the blowup.

    Regression for the PR #608 review finding: ``_solve_component``'s DP
    memoizes one entry per distinct reachable "available claims" subset, and
    a component of repeated indistinguishable events (same security, amount,
    and date on both histories) makes that grow exponentially — the review
    cited ~7.4s for a 15x15 fully-connected event graph. 12x12 already
    exceeds the bound and, thanks to it, fails in well under a second instead
    of continuing the exponential solve — this test proves both the refusal
    and its speed. Its companion,
    ``test_sparse_star_retains_all_1001_equally_optimal_relationships`` in
    test_event_assignment_sparse.py, proves the bound tracks actual DP work
    rather than raw claim count: that component has far more claims than
    this one but stays under the bound and solves normally, because it is
    sparse rather than densely overlapping.
    """
    n = 12
    candidates = [
        Candidate((f"a{i}", f"b{j}"), "exact") for i in range(n) for j in range(n)
    ]
    start = perf_counter()

    with pytest.raises(UserError) as exc_info:
        solve_candidates(candidates)

    # Generous relative to the ~0.2s measured locally — the point is that the
    # bound aborts the solve, not that it hits a precise budget.
    assert perf_counter() - start < 5.0
    assert exc_info.value.code == error_codes.INVESTMENT_MATCH_COMPONENT_TOO_LARGE
    assert str(len(candidates)) in exc_info.value.message
    assert str(MAX_COMPONENT_STATES) in exc_info.value.message
