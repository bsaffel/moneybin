"""Tests for the cross-channel confidence contract."""

import pytest

from moneybin.extractors.confidence import Confidence, Tier, tier_for


class TestTierFor:
    """Tests for tier_for() banding function."""

    def test_score_at_or_above_t_high_is_high(self) -> None:
        assert tier_for(0.95, t_high=0.90, t_med=0.70) == "high"
        assert tier_for(0.90, t_high=0.90, t_med=0.70) == "high"

    def test_score_between_bands_is_medium(self) -> None:
        assert tier_for(0.80, t_high=0.90, t_med=0.70) == "medium"
        assert tier_for(0.70, t_high=0.90, t_med=0.70) == "medium"

    def test_score_below_t_med_is_low(self) -> None:
        assert tier_for(0.69, t_high=0.90, t_med=0.70) == "low"
        assert tier_for(0.0, t_high=0.90, t_med=0.70) == "low"

    def test_rejects_out_of_range_score(self) -> None:
        with pytest.raises(ValueError, match="score must be in"):
            tier_for(-0.1, t_high=0.90, t_med=0.70)
        with pytest.raises(ValueError, match="score must be in"):
            tier_for(1.1, t_high=0.90, t_med=0.70)

    def test_rejects_inverted_bands(self) -> None:
        with pytest.raises(ValueError, match="t_high must be >= t_med"):
            tier_for(0.5, t_high=0.5, t_med=0.7)


class TestConfidence:
    """Tests for the Confidence dataclass."""

    def test_is_frozen(self) -> None:
        c = Confidence(score=0.9, tier="high", flagged=(), missing_required=())
        with pytest.raises(AttributeError):
            c.score = 0.5  # type: ignore[misc]

    def test_carries_flagged_and_missing(self) -> None:
        c = Confidence(
            score=0.75,
            tier="medium",
            flagged=("description",),
            missing_required=(),
        )
        assert c.flagged == ("description",)
        assert c.missing_required == ()

    def test_low_with_missing_required(self) -> None:
        c = Confidence(
            score=0.3,
            tier="low",
            flagged=(),
            missing_required=("transaction_date",),
        )
        assert c.tier == "low"
        assert "transaction_date" in c.missing_required

    def test_tier_literal_is_three_valued(self) -> None:
        valid: list[Tier] = ["high", "medium", "low"]
        assert len(valid) == 3
