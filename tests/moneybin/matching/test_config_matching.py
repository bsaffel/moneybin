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
        everything else must land in review (Tier 3) or go unmerged (Tier 2b),
        however close the dates are.

        The weighted formula does not hold that line by itself. Its peak is at
        *zero* days apart, where the date term is 1.0 — an earlier version of
        this test evaluated the score one day apart and called that the maximum,
        which is why the gap went unnoticed. The line is held at classification
        instead, and this pins it against the *shipped* settings rather than a
        fixture's, feeding in the formula's real peak.
        """
        from unittest.mock import MagicMock

        from moneybin.matching.engine import TransactionMatcher
        from moneybin.matching.scoring import CandidatePair, compute_confidence

        settings = MatchingSettings()
        # Derived from the live weights and window, never a literal, so retuning
        # them cannot silently reopen the door this test is holding shut.
        peak = compute_confidence(
            date_distance_days=0,
            description_similarity=1.0,
            date_window_days=settings.date_window_days,
        )
        assert peak >= settings.high_confidence_threshold, (
            f"the weighted peak is {peak:.4f}, below the auto-merge threshold "
            f"{settings.high_confidence_threshold} — the formula now holds this "
            "line on its own and this test should be rewritten deliberately"
        )
        pair = CandidatePair(
            source_transaction_id_a="a",
            source_type_a="csv",
            source_origin_a="chase",
            source_transaction_id_b="b",
            source_type_b="ofx",
            source_origin_b="chase_ofx",
            account_id="acct1",
            date_distance_days=0,
            description_similarity=1.0,
            confidence_score=peak,
            description_a="SHELL 1234",
            description_b="SHELL 1235",
            descriptions_agree=False,
        )
        matcher = TransactionMatcher(MagicMock(), settings)
        assert matcher._classify_pair(pair, "3") == ("pending", "auto"), (  # pyright: ignore[reportPrivateUsage]
            "a disagreeing cross-source pair at the weighted peak was accepted "
            "— proximity alone merged it without review"
        )
        assert matcher._classify_pair(pair, "2b") is None, (  # pyright: ignore[reportPrivateUsage]
            "a disagreeing within-source pair at the weighted peak was accepted "
            "— and Tier 2b has no review queue to catch it"
        )
