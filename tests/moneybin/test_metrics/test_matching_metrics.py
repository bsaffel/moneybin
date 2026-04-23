"""Tests for matching metrics registration."""

from moneybin.metrics.registry import (
    DEDUP_MATCH_CONFIDENCE,
    DEDUP_MATCHES_TOTAL,
    DEDUP_PAIRS_SCORED,
    DEDUP_REVIEW_PENDING,
)


class TestMatchingMetrics:
    """Tests for matching-specific metric definitions in the registry."""

    def test_dedup_matches_total_has_labels(self) -> None:
        assert "match_tier" in DEDUP_MATCHES_TOTAL._labelnames  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
        assert "decided_by" in DEDUP_MATCHES_TOTAL._labelnames  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals

    def test_dedup_pairs_scored_exists(self) -> None:
        # Counter._name stores the base name without the auto-appended _total suffix
        assert DEDUP_PAIRS_SCORED._name == "moneybin_dedup_pairs_scored"  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals

    def test_dedup_review_pending_exists(self) -> None:
        assert DEDUP_REVIEW_PENDING._name == "moneybin_dedup_review_pending"  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals

    def test_dedup_match_confidence_exists(self) -> None:
        assert DEDUP_MATCH_CONFIDENCE._name == "moneybin_dedup_match_confidence"  # type: ignore[reportPrivateUsage,reportUnknownMemberType]  # testing prometheus internals
