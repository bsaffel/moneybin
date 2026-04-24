"""Tests for MatchingSettings configuration."""

from moneybin.config import MatchingSettings, MoneyBinSettings


class TestMatchingSettings:
    """Tests for MatchingSettings defaults and validation."""

    def test_defaults(self) -> None:
        settings = MatchingSettings()
        assert settings.high_confidence_threshold == 0.95
        assert settings.review_threshold == 0.70
        assert settings.date_window_days == 3
        assert settings.source_priority == [
            "plaid",
            "csv",
            "excel",
            "tsv",
            "parquet",
            "feather",
            "pipe",
            "ofx",
        ]

    def test_source_priority_must_not_be_empty(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="source_priority"):
            MatchingSettings(source_priority=[])

    def test_thresholds_must_be_ordered(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="review_threshold.*high_confidence"):
            MatchingSettings(high_confidence_threshold=0.50, review_threshold=0.80)

    def test_available_on_root_settings(self) -> None:
        settings = MoneyBinSettings(profile="test")
        assert settings.matching.high_confidence_threshold == 0.95
        assert settings.matching.date_window_days == 3
