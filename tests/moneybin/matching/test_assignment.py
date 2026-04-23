"""Tests for 1:1 greedy assignment."""

from moneybin.matching.assignment import assign_greedy
from moneybin.matching.scoring import CandidatePair


def _pair(stid_a: str, stid_b: str, score: float, acct: str = "acct1") -> CandidatePair:
    return CandidatePair(
        source_transaction_id_a=stid_a,
        source_type_a="csv",
        source_origin_a="c",
        source_transaction_id_b=stid_b,
        source_type_b="ofx",
        source_origin_b="c",
        account_id=acct,
        date_distance_days=0,
        description_similarity=score,
        confidence_score=score,
        description_a="",
        description_b="",
    )


class TestAssignGreedy:
    """Tests for the assign_greedy function."""

    def test_no_candidates(self) -> None:
        assert assign_greedy([]) == []

    def test_single_pair(self) -> None:
        pairs = [_pair("a", "b", 0.95)]
        result = assign_greedy(pairs)
        assert len(result) == 1
        assert result[0].source_transaction_id_a == "a"

    def test_picks_highest_score_first(self) -> None:
        pairs = [
            _pair("a", "b", 0.90),
            _pair("a", "c", 0.95),  # Higher score
        ]
        result = assign_greedy(pairs)
        # a-c wins because higher score; a-b dropped because a is claimed
        assert len(result) == 1
        assert result[0].source_transaction_id_b == "c"

    def test_non_overlapping_pairs_both_selected(self) -> None:
        pairs = [
            _pair("a", "b", 0.95),
            _pair("c", "d", 0.90),
        ]
        result = assign_greedy(pairs)
        assert len(result) == 2

    def test_conflict_resolution(self) -> None:
        """When b could match both a and c, highest-scoring pair wins."""
        pairs = [
            _pair("a", "b", 0.98),  # a-b highest
            _pair("c", "b", 0.85),  # c-b lower, b already claimed
        ]
        result = assign_greedy(pairs)
        assert len(result) == 1
        assert result[0].source_transaction_id_a == "a"

    def test_three_way_conflict(self) -> None:
        """A, B, C all match X. Only best survives."""
        pairs = [
            _pair("a", "x", 0.90),
            _pair("b", "x", 0.95),
            _pair("c", "x", 0.80),
        ]
        result = assign_greedy(pairs)
        assert len(result) == 1
        assert result[0].source_transaction_id_a == "b"
