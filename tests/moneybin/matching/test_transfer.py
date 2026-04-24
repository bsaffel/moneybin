"""Tests for transfer detection scoring and blocking."""

from decimal import Decimal

import pytest

from moneybin.matching.transfer import (
    compute_amount_roundness,
    compute_keyword_score,
    compute_pair_frequency,
    compute_transfer_confidence,
)


class TestComputeKeywordScore:
    """Tests for transfer keyword detection."""

    def test_no_keywords(self) -> None:
        assert compute_keyword_score("STARBUCKS COFFEE", "GROCERY STORE") == 0.0

    def test_one_keyword(self) -> None:
        assert compute_keyword_score("ONLINE TRANSFER TO SAV", "GROCERY") == 0.5

    def test_two_keywords(self) -> None:
        assert compute_keyword_score("ACH TRANSFER", "PAYMENT") == 0.8

    def test_three_or_more_keywords(self) -> None:
        score = compute_keyword_score("ACH TRANSFER TO SAV", "WIRE FROM CHK")
        assert score == 1.0

    def test_case_insensitive(self) -> None:
        assert compute_keyword_score("transfer from checking", "deposit") == 0.5

    def test_both_descriptions_contribute(self) -> None:
        score = compute_keyword_score("TRANSFER", "ACH DEPOSIT")
        assert score >= 0.8


class TestComputeAmountRoundness:
    """Tests for amount roundness scoring."""

    def test_divisible_by_100(self) -> None:
        assert compute_amount_roundness(Decimal("500")) == 1.0
        assert compute_amount_roundness(Decimal("1000")) == 1.0

    def test_divisible_by_10(self) -> None:
        assert compute_amount_roundness(Decimal("50")) == 0.7
        assert compute_amount_roundness(Decimal("130")) == 0.7

    def test_whole_dollar(self) -> None:
        assert compute_amount_roundness(Decimal("42")) == 0.5
        assert compute_amount_roundness(Decimal("7")) == 0.5

    def test_fractional(self) -> None:
        assert compute_amount_roundness(Decimal("42.50")) == 0.3
        assert compute_amount_roundness(Decimal("99.99")) == 0.3


class TestComputePairFrequency:
    """Tests for account pair frequency scoring."""

    def test_single_pair(self) -> None:
        counts = {("acct1", "acct2"): 1}
        score = compute_pair_frequency("acct1", "acct2", counts, max_count=1)
        assert score == 1.0

    def test_frequent_pair(self) -> None:
        counts = {("acct1", "acct2"): 5, ("acct1", "acct3"): 2}
        score = compute_pair_frequency("acct1", "acct2", counts, max_count=5)
        assert score == 1.0

    def test_infrequent_pair(self) -> None:
        counts = {("acct1", "acct2"): 5, ("acct1", "acct3"): 2}
        score = compute_pair_frequency("acct1", "acct3", counts, max_count=5)
        assert score == pytest.approx(0.4)

    def test_order_independent(self) -> None:
        counts = {("acct1", "acct2"): 3}
        score_ab = compute_pair_frequency("acct1", "acct2", counts, max_count=3)
        score_ba = compute_pair_frequency("acct2", "acct1", counts, max_count=3)
        assert score_ab == score_ba

    def test_unknown_pair(self) -> None:
        counts = {("acct1", "acct2"): 3}
        score = compute_pair_frequency("acct3", "acct4", counts, max_count=3)
        assert score == 0.0


class TestComputeTransferConfidence:
    """Tests for combined transfer confidence scoring."""

    def test_perfect_signals(self) -> None:
        score = compute_transfer_confidence(
            date_distance_days=0,
            date_window_days=3,
            keyword_score=1.0,
            amount_roundness=1.0,
            pair_frequency=1.0,
        )
        assert score == pytest.approx(1.0)

    def test_zero_signals(self) -> None:
        score = compute_transfer_confidence(
            date_distance_days=3,
            date_window_days=3,
            keyword_score=0.0,
            amount_roundness=0.0,
            pair_frequency=0.0,
        )
        assert score == pytest.approx(0.0)

    def test_date_distance_impact(self) -> None:
        same_day = compute_transfer_confidence(
            date_distance_days=0,
            date_window_days=3,
            keyword_score=0.5,
            amount_roundness=0.5,
            pair_frequency=0.5,
        )
        one_day = compute_transfer_confidence(
            date_distance_days=1,
            date_window_days=3,
            keyword_score=0.5,
            amount_roundness=0.5,
            pair_frequency=0.5,
        )
        assert same_day > one_day

    def test_custom_weights(self) -> None:
        weights = {
            "date_distance": 1.0,
            "keyword": 0.0,
            "roundness": 0.0,
            "pair_frequency": 0.0,
        }
        score = compute_transfer_confidence(
            date_distance_days=0,
            date_window_days=3,
            keyword_score=1.0,
            amount_roundness=1.0,
            pair_frequency=1.0,
            weights=weights,
        )
        assert score == pytest.approx(1.0)

    def test_score_between_zero_and_one(self) -> None:
        for days in range(4):
            for kw in [0.0, 0.5, 1.0]:
                score = compute_transfer_confidence(
                    date_distance_days=days,
                    date_window_days=3,
                    keyword_score=kw,
                    amount_roundness=0.5,
                    pair_frequency=0.5,
                )
                assert 0.0 <= score <= 1.0
