"""Clique enumeration must not explode on one densely-connected component."""

from collections import defaultdict
from time import perf_counter

import pytest

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.investments.event_assignment import MAX_COMPONENT_STATES
from moneybin.investments.event_planning import (
    _groups,  # pyright: ignore[reportPrivateUsage]  # direct solver-internals test, matches matching/test_engine.py precedent
)


def _clique(n: int) -> tuple[set[str], dict[str, set[str]]]:
    nodes = {f"n{i}" for i in range(n)}
    neighbors: dict[str, set[str]] = defaultdict(set)
    for node in nodes:
        neighbors[node] = nodes - {node}
    return nodes, neighbors


def test_dense_clique_over_the_subset_bound_fails_visibly() -> None:
    """A densely-connected component refuses rather than risk the blowup.

    Regression for the PR #608 review finding: a fully-connected k-node
    component emits every subset of size >= 2 (2**k - k - 1 groups) before
    ``solve_candidates`` can impose any limit — the review cited ~33M groups
    for 25 mutually compatible histories (measured here too: k=25 emits
    33,554,406 groups in ~16s). k=15 already exceeds the bound and, thanks
    to it, fails in well under a second instead of continuing the
    exponential enumeration. Its companion,
    ``test_many_disjoint_pairs_are_not_penalized_by_one_shared_call``,
    proves the bound is scoped per connected component rather than summed
    across this whole ``_groups()`` call — otherwise many small, disjoint,
    everyday match pairs would trip a limit sized for one dense cluster.
    """
    nodes, neighbors = _clique(15)
    start = perf_counter()

    with pytest.raises(UserError) as exc_info:
        list(_groups(nodes, neighbors))

    # Generous relative to the ~0.03s measured locally — the point is that
    # the bound aborts the enumeration, not that it hits a precise budget.
    assert perf_counter() - start < 5.0
    assert exc_info.value.code == error_codes.INVESTMENT_MATCH_COMPONENT_TOO_LARGE
    assert str(len(nodes)) in exc_info.value.message
    assert str(MAX_COMPONENT_STATES) in exc_info.value.message


def test_many_disjoint_pairs_are_not_penalized_by_one_shared_call() -> None:
    """Many small, disjoint components in one call must not sum toward the bound.

    25,000 independent 2-node pairs (25,000 groups total) is a routine
    portfolio shape — every pair is its own trivial component. A bound
    checked globally across this ``_groups()`` call, rather than reset per
    connected component, would wrongly refuse this cheap, legitimate input
    once the running total crossed MAX_COMPONENT_STATES.
    """
    nodes: set[str] = set()
    neighbors: dict[str, set[str]] = defaultdict(set)
    for i in range(25_000):
        left, right = f"a{i}", f"b{i}"
        nodes |= {left, right}
        neighbors[left] = {right}
        neighbors[right] = {left}

    start = perf_counter()
    groups = list(_groups(nodes, neighbors))
    assert perf_counter() - start < 5.0
    assert len(groups) == 25_000
    assert all(len(group) == 2 for group in groups)
