"""Tests for 1:1 greedy assignment."""

from decimal import Decimal

from moneybin.matching.assignment import assign_greedy
from moneybin.matching.scoring import CandidatePair
from moneybin.matching.transfer import TransferCandidatePair


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


class TestAssignGreedyTransfers:
    """Tests for assign_greedy with transfer candidate pairs."""

    def test_assigns_best_transfer_pair(self) -> None:
        candidates = [
            TransferCandidatePair(
                source_transaction_id_a="chk_1",
                source_type_a="csv",
                source_origin_a="chase",
                account_id_a="checking",
                source_transaction_id_b="sav_1",
                source_type_b="csv",
                source_origin_b="chase",
                account_id_b="savings",
                amount=Decimal("500.00"),
                date_distance_days=0,
                description_a="TRANSFER",
                description_b="TRANSFER",
                date_distance_score=1.0,
                keyword_score=0.5,
                amount_roundness_score=1.0,
                pair_frequency_score=1.0,
                confidence_score=0.90,
            ),
            TransferCandidatePair(
                source_transaction_id_a="chk_1",
                source_type_a="csv",
                source_origin_a="chase",
                account_id_a="checking",
                source_transaction_id_b="brk_1",
                source_type_b="csv",
                source_origin_b="chase",
                account_id_b="brokerage",
                amount=Decimal("500.00"),
                date_distance_days=1,
                description_a="TRANSFER",
                description_b="DEPOSIT",
                date_distance_score=0.67,
                keyword_score=0.5,
                amount_roundness_score=1.0,
                pair_frequency_score=0.5,
                confidence_score=0.70,
            ),
        ]
        assigned = assign_greedy(candidates)
        assert len(assigned) == 1
        assert assigned[0].confidence_score == 0.90
        assert assigned[0].account_id_b == "savings"

    def test_same_source_id_different_accounts(self) -> None:
        """Same source_transaction_id in different accounts are distinct slots."""
        candidates = [
            TransferCandidatePair(
                source_transaction_id_a="txn_1",
                source_type_a="ofx",
                source_origin_a="bank_a",
                account_id_a="checking_a",
                source_transaction_id_b="txn_1",
                source_type_b="ofx",
                source_origin_b="bank_b",
                account_id_b="savings_a",
                amount=Decimal("200.00"),
                date_distance_days=0,
                description_a="TRANSFER",
                description_b="TRANSFER",
                date_distance_score=1.0,
                keyword_score=1.0,
                amount_roundness_score=1.0,
                pair_frequency_score=1.0,
                confidence_score=0.95,
            ),
            TransferCandidatePair(
                source_transaction_id_a="txn_1",
                source_type_a="ofx",
                source_origin_a="bank_c",
                account_id_a="checking_b",
                source_transaction_id_b="txn_1",
                source_type_b="ofx",
                source_origin_b="bank_d",
                account_id_b="savings_b",
                amount=Decimal("200.00"),
                date_distance_days=0,
                description_a="TRANSFER",
                description_b="TRANSFER",
                date_distance_score=1.0,
                keyword_score=1.0,
                amount_roundness_score=1.0,
                pair_frequency_score=1.0,
                confidence_score=0.90,
            ),
        ]
        assigned = assign_greedy(candidates)
        # Both pairs should be assigned since account_id distinguishes the slots
        assert len(assigned) == 2

    def test_one_to_one_enforcement(self) -> None:
        """Each transaction participates in at most one transfer pair."""
        candidates = [
            TransferCandidatePair(
                source_transaction_id_a="chk_1",
                source_type_a="csv",
                source_origin_a="chase",
                account_id_a="checking",
                source_transaction_id_b="sav_1",
                source_type_b="csv",
                source_origin_b="chase",
                account_id_b="savings",
                amount=Decimal("500.00"),
                date_distance_days=0,
                description_a="TRANSFER",
                description_b="TRANSFER",
                date_distance_score=1.0,
                keyword_score=1.0,
                amount_roundness_score=1.0,
                pair_frequency_score=1.0,
                confidence_score=0.95,
            ),
            TransferCandidatePair(
                source_transaction_id_a="chk_2",
                source_type_a="csv",
                source_origin_a="chase",
                account_id_a="checking",
                source_transaction_id_b="sav_1",
                source_type_b="csv",
                source_origin_b="chase",
                account_id_b="savings",
                amount=Decimal("500.00"),
                date_distance_days=1,
                description_a="TRANSFER",
                description_b="TRANSFER",
                date_distance_score=0.67,
                keyword_score=1.0,
                amount_roundness_score=1.0,
                pair_frequency_score=1.0,
                confidence_score=0.85,
            ),
        ]
        assigned = assign_greedy(candidates)
        assert len(assigned) == 1
        assert assigned[0].source_transaction_id_a == "chk_1"
