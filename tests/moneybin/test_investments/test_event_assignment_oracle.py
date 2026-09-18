"""Independent exhaustive and dense repeated-event assignment evidence."""

from itertools import combinations, product
from time import perf_counter

from moneybin.investments.event_assignment import Candidate, solve_candidates


def test_hypergraph_with_prior_match_claims_matches_independent_oracle() -> None:
    candidates = [
        Candidate(("a", "b", "c"), "exact", ("prior",)),
        Candidate(("a", "d"), "native", ("prior",)),
        Candidate(("b", "c", "e"), "fuzzy"),
        Candidate(("d", "e"), "exact"),
        Candidate(("f", "g"), "native", ("prior",)),
        Candidate(("f", "h"), "exact"),
    ]
    for enabled in product((False, True), repeat=len(candidates)):
        active = [item for item, take in zip(candidates, enabled, strict=True) if take]
        feasible: list[tuple[tuple[int, int, int], frozenset[Candidate]]] = []
        for mask in product((False, True), repeat=len(active)):
            chosen = [item for item, take in zip(active, mask, strict=True) if take]
            events = [member for item in chosen for member in item.members]
            decisions = [
                decision for item in chosen for decision in item.supersedes_decision_ids
            ]
            if len(events) != len(set(events)) or len(decisions) != len(set(decisions)):
                continue
            score = (
                len(events),
                sum(
                    len(item.members)
                    for item in chosen
                    if item.confidence_band != "fuzzy"
                ),
                sum(
                    len(item.members)
                    for item in chosen
                    if item.confidence_band == "native"
                ),
            )
            feasible.append((score, frozenset(chosen)))
        best = max(score for score, _ in feasible)
        optima = [chosen for score, chosen in feasible if score == best]
        result = solve_candidates(active)
        assert result.objective == best
        assert {item.candidate: item.is_competing for item in result.proposals} == {
            item: not all(item in chosen for chosen in optima)
            for item in active
            if any(item in chosen for chosen in optima)
        }


def test_every_four_node_pair_graph_matches_exhaustive_assignment_oracle() -> None:
    edges = list(combinations("abcd", 2))
    for bands in product((None, "native", "exact", "fuzzy"), repeat=len(edges)):
        candidates = [
            Candidate(edge, band)
            for edge, band in zip(edges, bands, strict=True)
            if band is not None
        ]
        feasible: list[tuple[tuple[int, int, int], frozenset[int]]] = []
        for selected in product((False, True), repeat=len(candidates)):
            chosen = [i for i, take in enumerate(selected) if take]
            members = [node for i in chosen for node in candidates[i].members]
            if len(set(members)) != len(members):
                continue
            score = (
                len(members),
                sum(2 for i in chosen if candidates[i].confidence_band != "fuzzy"),
                sum(2 for i in chosen if candidates[i].confidence_band == "native"),
            )
            feasible.append((score, frozenset(chosen)))
        maximum = max(score for score, _ in feasible)
        optima = [chosen for score, chosen in feasible if score == maximum]
        expected = {
            candidates[i].members: not all(i in chosen for chosen in optima)
            for i in range(len(candidates))
            if any(i in chosen for chosen in optima)
        }
        result = solve_candidates(candidates)
        assert result.objective == maximum
        assert {
            p.candidate.members: p.is_competing for p in result.proposals
        } == expected


def test_dense_repeated_eight_by_eight_keeps_every_optimal_relationship() -> None:
    candidates = [
        Candidate((f"a{i}", f"b{j}"), "exact") for i in range(8) for j in range(8)
    ]
    start = perf_counter()
    result = solve_candidates(candidates)
    elapsed = perf_counter() - start
    assert result.objective == (16, 16, 0)
    assert len(result.proposals) == 64
    assert all(p.is_competing for p in result.proposals)
    print(f"Dense 8x8: 64 proposals, 40320 optimal assignments, {elapsed:.6f}s")  # noqa: T201  # benchmark evidence for the test log
