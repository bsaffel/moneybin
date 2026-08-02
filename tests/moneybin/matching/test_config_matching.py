"""Tests for transfer-specific fields in MatchingSettings."""

import pytest
from pydantic import ValidationError

from moneybin.config import MatchingSettings


class TestTransferSettings:
    """Tests for transfer-specific matching configuration."""

    def test_transfer_review_threshold_default(self) -> None:
        settings = MatchingSettings()
        assert settings.transfer_review_threshold == 0.55

    def test_transfer_review_threshold_custom(self) -> None:
        settings = MatchingSettings(transfer_review_threshold=0.85)
        assert settings.transfer_review_threshold == 0.85

    def test_transfer_review_threshold_bounds(self) -> None:
        with pytest.raises(ValidationError):
            MatchingSettings(transfer_review_threshold=1.5)
        with pytest.raises(ValidationError):
            MatchingSettings(transfer_review_threshold=-0.1)

    def test_transfer_signal_weights_default(self) -> None:
        settings = MatchingSettings()
        assert settings.transfer_signal_weights == {
            "date_distance": 0.6,
            "keyword": 0.4,
        }

    def test_transfer_signal_weights_custom(self) -> None:
        custom = {
            "date_distance": 0.7,
            "keyword": 0.3,
        }
        settings = MatchingSettings(transfer_signal_weights=custom)
        assert settings.transfer_signal_weights == custom

    def test_transfer_signal_weights_missing_key(self) -> None:
        with pytest.raises(ValidationError, match="missing keys"):
            MatchingSettings(transfer_signal_weights={"date_distance": 1.0})

    def test_transfer_signal_weights_negative(self) -> None:
        with pytest.raises(ValidationError, match="negative values"):
            MatchingSettings(
                transfer_signal_weights={
                    "date_distance": 1.2,
                    "keyword": -0.2,
                }
            )

    def test_transfer_signal_weights_bad_sum(self) -> None:
        with pytest.raises(ValidationError, match="must sum to 1.0"):
            MatchingSettings(
                transfer_signal_weights={
                    "date_distance": 0.5,
                    "keyword": 0.4,
                }
            )

    def test_transfer_signal_weights_rejects_legacy_keys(self) -> None:
        # An upgraded env var still carrying the retired 4-signal shape sums
        # to 1.0 and includes both required keys, so the older validator
        # passed it — but compute_transfer_confidence only reads
        # date_distance + keyword, silently capping max confidence below 1.0.
        with pytest.raises(ValidationError, match="unrecognised keys"):
            MatchingSettings(
                transfer_signal_weights={
                    "date_distance": 0.4,
                    "keyword": 0.3,
                    "roundness": 0.15,
                    "pair_frequency": 0.15,
                }
            )


class TestDateWindowBound:
    """The window may not widen far enough to make a date gap auto-merge."""

    def test_default_window_spans_a_weekend(self) -> None:
        """Card transactions post after the weekend; 3 days could not reach Tuesday.

        A Friday purchase posting the next business day is 4 days out, and a
        Monday holiday makes it 5. The old 3-day window could not admit that
        pair at all, so the OFX and PDF copies of one transaction were never
        even offered to the matcher as candidates.
        """
        assert MatchingSettings().date_window_days >= 5

    def test_shipped_defaults_keep_every_date_gap_in_review(self) -> None:
        """At the shipped defaults, only same-day pairs can auto-merge.

        Auto-merge belongs to the exact-key path — same account, exact amount,
        same day — because a wrong silent merge is the hardest inference to
        notice and undo (design-principles.md). The weighted branch peaks one
        day apart, so checking that peak against the auto-merge threshold covers
        every date gap at once.

        This is pinned at the DEFAULTS only. The margin narrows as the window
        widens, so it is a property of the shipped configuration rather than a
        law of the scoring function.
        """
        from moneybin.matching.scoring import _WEIGHT_DATE, _WEIGHT_DESCRIPTION

        settings = MatchingSettings()
        peak = (
            _WEIGHT_DATE * (1.0 - 1.0 / settings.date_window_days) + _WEIGHT_DESCRIPTION
        )
        assert peak < settings.high_confidence_threshold, (
            f"a one-day-apart pair with identical descriptions scores {peak:.4f}, "
            f"at or above the {settings.high_confidence_threshold} auto-merge "
            "threshold — date-gap pairs would merge without review"
        )
