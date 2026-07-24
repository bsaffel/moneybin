"""Tests for InvestmentsSettings configuration."""

import pytest

from moneybin.config import InvestmentsSettings, MoneyBinSettings
from moneybin.staleness import SECURITY_TYPE_STALENESS_DAYS


class TestInvestmentsSettings:
    """Tests for InvestmentsSettings defaults, validation, and env override."""

    def test_defaults(self) -> None:
        settings = InvestmentsSettings()
        assert settings.price_staleness_default_days == 4

    def test_global_default_matches_the_exchange_traded_threshold(self) -> None:
        """The fallback absorbs a weekend, like the types it stands in for.

        A tighter global default would fire on most days for the unnamed types
        and train the reader to ignore every staleness warning.
        """
        assert (
            InvestmentsSettings().price_staleness_default_days
            == SECURITY_TYPE_STALENESS_DAYS["equity"]
        )

    def test_staleness_days_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="price_staleness_default_days"):
            InvestmentsSettings(price_staleness_default_days=0)

    def test_available_on_root_settings(self) -> None:
        settings = MoneyBinSettings(profile="test")
        assert settings.investments.price_staleness_default_days == 4

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The nested env-var path is the public contract this block locks in."""
        monkeypatch.setenv("MONEYBIN_INVESTMENTS__PRICE_STALENESS_DEFAULT_DAYS", "10")
        settings = MoneyBinSettings(profile="test")
        assert settings.investments.price_staleness_default_days == 10
