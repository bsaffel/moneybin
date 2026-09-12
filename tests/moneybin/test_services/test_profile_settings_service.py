"""Tests for profile-level managed settings (multi-currency Requirement 4)."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services.profile_settings_service import (
    MANAGED_SETTING_KEYS,
    ProfileSettingsService,
)


@pytest.fixture()
def service(db: Database) -> ProfileSettingsService:
    return ProfileSettingsService(db)


def test_home_currency_starts_unset(service: ProfileSettingsService) -> None:
    """A profile reports no home currency until one is chosen — never 'USD'."""
    assert service.get_settings().home_currency is None


def test_set_setting_persists_the_home_currency(
    service: ProfileSettingsService, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The managed key round-trips through the database."""
    restated: list[Database] = []
    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
        restated.append,
    )

    service.set_setting("home_currency", "EUR", actor="cli")

    assert service.get_settings().home_currency == "EUR"
    assert len(restated) == 1


def test_display_currency_targets_are_normalized_without_restatement(
    service: ProfileSettingsService, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Changing read targets plans future rates but never restates accounting."""
    restated: list[Database] = []
    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
        restated.append,
    )

    service.set_setting("display_currency_targets", " eur, GBP,EUR ", actor="cli")

    assert service.get_settings().display_currency_targets == ("EUR", "GBP")
    assert restated == []


def test_set_setting_classifies_a_malformed_display_target_list(
    service: ProfileSettingsService,
) -> None:
    """An empty comma-delimited target is mutation-invalid input, not a server bug."""
    service.set_setting("display_currency_targets", "EUR", actor="cli")

    with pytest.raises(UserError) as excinfo:
        service.set_setting("display_currency_targets", "EUR,,GBP", actor="cli")

    assert excinfo.value.code == "mutation_invalid_input"
    assert service.get_settings().display_currency_targets == ("EUR",)


def test_set_setting_rejects_an_unknown_key(service: ProfileSettingsService) -> None:
    """An unrecognized managed key is refused, and the error names the real ones.

    Catches a typo silently becoming a no-op write the user believes succeeded.
    """
    with pytest.raises(UserError) as excinfo:
        service.set_setting("home_currncy", "EUR", actor="cli")

    assert "home_currency" in str(excinfo.value)


def test_set_setting_rejects_a_malformed_currency(
    service: ProfileSettingsService,
) -> None:
    """A value that is not three uppercase letters never reaches the table."""
    with pytest.raises(UserError):
        service.set_setting("home_currency", "euro", actor="cli")

    assert service.get_settings().home_currency is None


def test_home_currency_is_the_registered_managed_key() -> None:
    """The CLI dispatches on this registry, so it must carry the currency key.

    If `home_currency` fell out of the registry, `profile set home_currency EUR`
    would silently route to the config.yaml path and write a key nothing reads.
    """
    assert "home_currency" in MANAGED_SETTING_KEYS
