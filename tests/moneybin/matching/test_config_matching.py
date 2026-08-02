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
    """The window may not widen far enough for the date alone to auto-merge."""

    def test_default_window_spans_a_weekend(self) -> None:
        """Card transactions post after the weekend; 3 days could not reach Tuesday.

        A Friday purchase posting the next business day is 4 days out, and a
        Monday holiday makes it 5. The old 3-day window could not admit that
        pair at all, so the OFX and PDF copies of one transaction were never
        even offered to the matcher as candidates.
        """
        assert MatchingSettings().date_window_days >= 5

    def test_closeness_alone_never_reaches_auto_merge(self) -> None:
        """Proximity is never enough to merge silently — only agreement is.

        This is the safety property the description-agreement gate rests on.
        Pairs whose descriptions agree are lifted to auto-merge by the floor;
        everything else falls to the weighted formula, and that path must stay
        below the auto-merge threshold no matter how close the dates are. If it
        could reach the threshold on its own, a disagreeing pair would merge
        without review purely for landing on a nearby day.

        Checking the weighted branch's peak covers every gap at once: it is
        highest one day apart with identical descriptions.

        Pinned at the DEFAULTS only. The margin narrows as the window widens, so
        this is a property of the shipped configuration, not a law of the
        scoring function.
        """
        from moneybin.matching.scoring import (
            _WEIGHT_DATE,  # pyright: ignore[reportPrivateUsage]  # derive, not literal
            _WEIGHT_DESCRIPTION,  # pyright: ignore[reportPrivateUsage]  # same
        )

        settings = MatchingSettings()
        # Derived from the live weights, never a literal, so retuning them here
        # cannot silently reopen the door this test is holding shut.
        peak = (
            _WEIGHT_DATE * (1.0 - 1.0 / settings.date_window_days) + _WEIGHT_DESCRIPTION
        )
        assert peak < settings.high_confidence_threshold, (
            f"a one-day-apart pair with identical descriptions scores {peak:.4f}, "
            f"at or above the {settings.high_confidence_threshold} auto-merge "
            "threshold — proximity alone would merge a pair without review"
        )
