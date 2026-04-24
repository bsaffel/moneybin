"""Tests for transfer-specific fields in MatchingSettings."""

import pytest
from pydantic import ValidationError

from moneybin.config import MatchingSettings


class TestTransferSettings:
    """Tests for transfer-specific matching configuration."""

    def test_transfer_review_threshold_default(self) -> None:
        settings = MatchingSettings()
        assert settings.transfer_review_threshold == 0.70

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
            "date_distance": 0.4,
            "keyword": 0.3,
            "roundness": 0.15,
            "pair_frequency": 0.15,
        }

    def test_transfer_signal_weights_custom(self) -> None:
        custom = {
            "date_distance": 0.5,
            "keyword": 0.25,
            "roundness": 0.15,
            "pair_frequency": 0.1,
        }
        settings = MatchingSettings(transfer_signal_weights=custom)
        assert settings.transfer_signal_weights == custom

    def test_transfer_signal_weights_missing_key(self) -> None:
        with pytest.raises(ValidationError, match="missing keys"):
            MatchingSettings(
                transfer_signal_weights={"date_distance": 0.5, "keyword": 0.5}
            )

    def test_transfer_signal_weights_bad_sum(self) -> None:
        with pytest.raises(ValidationError, match="must sum to 1.0"):
            MatchingSettings(
                transfer_signal_weights={
                    "date_distance": 0.5,
                    "keyword": 0.4,
                    "roundness": 0.2,
                    "pair_frequency": 0.2,
                }
            )
