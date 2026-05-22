"""Tests for MoneyBinSettings.packages."""

from moneybin.config import MoneyBinSettings, PackagesSettings


def test_packages_settings_defaults() -> None:
    """Default PackagesSettings is empty but instantiable."""
    settings = PackagesSettings()
    assert settings is not None


def test_money_bin_settings_exposes_packages() -> None:
    """MoneyBinSettings.packages exists and is a PackagesSettings instance."""
    settings = MoneyBinSettings()
    assert isinstance(settings.packages, PackagesSettings)
