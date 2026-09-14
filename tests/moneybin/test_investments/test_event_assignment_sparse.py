"""A large sparse candidate component must not consume Python call depth."""

from moneybin.investments.event_assignment import Candidate, solve_candidates


def test_sparse_star_retains_all_1001_equally_optimal_relationships() -> None:
    candidates = [
        Candidate(("center", f"leaf_{index:04d}"), "exact") for index in range(1001)
    ]
    result = solve_candidates(candidates)
    assert result.objective == (2, 2, 0)
    assert len(result.proposals) == 1001
    assert all(proposal.is_competing for proposal in result.proposals)
