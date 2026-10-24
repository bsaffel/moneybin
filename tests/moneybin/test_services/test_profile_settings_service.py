"""Tests for profile-level managed settings (multi-currency Requirement 4)."""

from __future__ import annotations

import pytest
from pytest_mock import MockerFixture

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services.profile_settings_service import (
    MANAGED_SETTING_KEYS,
    ProfileSettingsService,
)
from moneybin.tables import PROFILE_SETTINGS


@pytest.fixture()
def service(db: Database) -> ProfileSettingsService:
    return ProfileSettingsService(db)


def test_home_currency_starts_unset(service: ProfileSettingsService) -> None:
    """A profile reports no home currency until one is chosen — never 'USD'."""
    assert service.get_settings().home_currency is None


def test_set_setting_persists_the_home_currency(
    service: ProfileSettingsService, db: Database, mocker: MockerFixture
) -> None:
    """The managed key round-trips and restates what reads the settings row."""
    restate = mocker.patch("moneybin.services.restate.restate_after_write")

    service.set_setting("home_currency", "EUR", actor="cli")

    assert service.get_settings().home_currency == "EUR"
    restate.assert_called_once_with(
        db,
        [PROFILE_SETTINGS],
        committed_change="setting",
    )


def test_display_currency_targets_are_normalized_and_restate(
    service: ProfileSettingsService, db: Database, mocker: MockerFixture
) -> None:
    """Changing read targets also restates: the lots fold this row's audit history."""
    restate = mocker.patch("moneybin.services.restate.restate_after_write")

    service.set_setting("display_currency_targets", " eur, GBP,EUR ", actor="cli")

    assert service.get_settings().display_currency_targets == ("EUR", "GBP")
    restate.assert_called_once_with(
        db,
        [PROFILE_SETTINGS],
        committed_change="setting",
    )


def test_set_setting_rejects_an_oversized_display_target_list(
    service: ProfileSettingsService,
) -> None:
    """The CLI's comma-parsed string reaches the same repo-level count bound.

    An unbounded target set would make ``plan_rate_backfill`` build a
    Cartesian product against every held currency (MB-148 CONSIDER).
    """
    from moneybin.limits import DISPLAY_CURRENCY_TARGETS_MAX_COUNT

    codes = ",".join(
        f"{chr(65 + i % 26)}{chr(65 + (i // 26) % 26)}{chr(65 + i // 676)}"
        for i in range(DISPLAY_CURRENCY_TARGETS_MAX_COUNT + 1)
    )

    with pytest.raises(UserError) as excinfo:
        service.set_setting("display_currency_targets", codes, actor="cli")

    assert excinfo.value.code == "mutation_invalid_input"
    assert service.get_settings().display_currency_targets == ()


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


def test_reporting_timezone_changes_restate_profile_settings(
    service: ProfileSettingsService, db: Database, mocker: MockerFixture
) -> None:
    restate = mocker.patch("moneybin.services.restate.restate_after_write")
    service.set_setting("reporting_timezone", "America/New_York", actor="cli")
    assert service.get_settings().reporting_timezone == "America/New_York"
    restate.assert_called_once_with(db, [PROFILE_SETTINGS], committed_change="setting")
    restate.reset_mock()
    with pytest.raises(UserError):
        service.set_setting("reporting_timezone", "Unknown/Zone", actor="cli")
    assert service.get_settings().reporting_timezone == "America/New_York"
    restate.assert_not_called()
