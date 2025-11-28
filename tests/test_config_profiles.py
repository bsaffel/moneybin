"""Tests for profile-based configuration system.

This module tests the profile-based configuration loading, validation,
and environment file handling for dev and prod profiles.
"""

import os
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from moneybin.config import (
    MoneyBinSettings,
    get_current_profile,
    get_settings,
    get_settings_for_profile,
    reload_settings,
    set_current_profile,
)


class TestProfileConfiguration:
    """Test suite for profile-based configuration."""

    def test_default_profile_is_default(self):
        """Test that default profile is 'default'."""
        # Reset to ensure clean state
        set_current_profile("default")
        assert get_current_profile() == "default"

    def test_set_current_profile_alice(self):
        """Test setting current profile to alice."""
        set_current_profile("alice")
        assert get_current_profile() == "alice"

    def test_set_current_profile_bob(self):
        """Test setting current profile to bob."""
        set_current_profile("bob")
        assert get_current_profile() == "bob"
        # Reset to default for other tests
        set_current_profile("default")

    def test_set_invalid_profile_with_slash_raises_error(self):
        """Test that setting invalid profile with slash raises ValueError."""
        with pytest.raises(ValueError, match="Invalid profile"):
            set_current_profile("invalid/profile")

    def test_set_invalid_profile_with_space_raises_error(self):
        """Test that setting invalid profile with space raises ValueError."""
        with pytest.raises(ValueError, match="Invalid profile"):
            set_current_profile("bad profile")

    def test_get_settings_for_profile_dev(self, mocker: MockerFixture):
        """Test getting settings for dev profile explicitly."""
        # Mock environment variables
        mocker.patch.dict(
            os.environ,
            {
                "PLAID_CLIENT_ID": "test_dev_client_id",
                "PLAID_SECRET": "test_dev_secret",
                "PLAID_ENV": "sandbox",
            },
        )

        # Mock file existence to avoid actual file reads
        mocker.patch.object(Path, "exists", return_value=False)

        settings = get_settings_for_profile("dev")
        assert settings.profile == "dev"
        assert settings.plaid.environment == "sandbox"

    def test_get_settings_for_profile_prod(self, mocker: MockerFixture):
        """Test getting settings for prod profile explicitly."""
        # Mock environment variables
        mocker.patch.dict(
            os.environ,
            {
                "PLAID_CLIENT_ID": "test_prod_client_id",
                "PLAID_SECRET": "test_prod_secret",
                "PLAID_ENV": "production",
            },
        )

        # Mock file existence to avoid actual file reads
        mocker.patch.object(Path, "exists", return_value=False)

        settings = get_settings_for_profile("prod")
        assert settings.profile == "prod"

    def test_get_settings_for_invalid_profile_raises_error(self):
        """Test that getting settings for invalid profile raises ValueError."""
        with pytest.raises(ValueError, match="Invalid profile"):
            get_settings_for_profile("invalid/profile")

    def test_valid_profile_names_work(self):
        """Test that various valid profile names are accepted."""
        # These should all be valid
        valid_profiles = ["alice", "bob", "dev", "prod", "alice-personal", "bob_work"]

        for profile_name in valid_profiles:
            # Should not raise
            set_current_profile(profile_name)
            assert get_current_profile() == profile_name

    def test_settings_cache_per_profile(self, mocker: MockerFixture):
        """Test that settings are cached independently per profile."""
        # Mock environment variables
        mocker.patch.dict(
            os.environ,
            {
                "PLAID_CLIENT_ID": "test_client_id",
                "PLAID_SECRET": "test_secret",
            },
        )

        # Mock file existence
        mocker.patch.object(Path, "exists", return_value=False)

        # Get settings for dev
        settings_dev_1 = get_settings_for_profile("dev")
        settings_dev_2 = get_settings_for_profile("dev")

        # Should be the same cached instance
        assert settings_dev_1 is settings_dev_2

        # Get settings for prod
        settings_prod = get_settings_for_profile("prod")

        # Should be different instances
        assert settings_dev_1 is not settings_prod

    def test_reload_settings_clears_cache(self, mocker: MockerFixture):
        """Test that reload_settings clears the cache for a profile."""
        # Mock environment variables
        mocker.patch.dict(
            os.environ,
            {
                "PLAID_CLIENT_ID": "test_client_id",
                "PLAID_SECRET": "test_secret",
            },
        )

        # Mock file existence
        mocker.patch.object(Path, "exists", return_value=False)

        # Get initial settings
        settings_1 = get_settings_for_profile("dev")

        # Reload settings
        settings_2 = reload_settings("dev")

        # Should be different instances (cache was cleared)
        assert settings_1 is not settings_2

    def test_profile_env_file_loading_uses_profile(self, mocker: MockerFixture):
        """Test that profile name is included in settings."""
        # Mock file existence to test the file selection logic
        mocker.patch.dict(
            os.environ,
            {
                "PLAID_CLIENT_ID": "test_client",
                "PLAID_SECRET": "test_secret",
            },
        )
        mocker.patch.object(Path, "exists", return_value=False)

        # Create settings for alice profile
        settings = MoneyBinSettings(profile="alice")
        assert settings.profile == "alice"

    def test_legacy_environment_variables(self, mocker: MockerFixture):
        """Test that legacy environment variables still work."""
        # Mock legacy environment variables
        mocker.patch.dict(
            os.environ,
            {
                "PLAID_CLIENT_ID": "legacy_client_id",
                "PLAID_SECRET": "legacy_secret",
                "PLAID_ENV": "sandbox",
                "DUCKDB_PATH": "custom/path/db.duckdb",
            },
            clear=True,
        )

        # Mock file existence
        mocker.patch.object(Path, "exists", return_value=False)

        settings = MoneyBinSettings(profile="dev")

        assert settings.plaid.client_id == "legacy_client_id"
        assert settings.plaid.secret == "legacy_secret"  # noqa: S105  # Test fixture value, not a real secret
        assert settings.plaid.environment == "sandbox"
        assert settings.database.path == Path("custom/path/db.duckdb")

    def test_profile_field_in_settings(self, mocker: MockerFixture):
        """Test that profile field is properly set in settings."""
        mocker.patch.dict(
            os.environ,
            {
                "PLAID_CLIENT_ID": "test_client",
                "PLAID_SECRET": "test_secret",
            },
        )
        mocker.patch.object(Path, "exists", return_value=False)

        dev_settings = MoneyBinSettings(profile="dev")
        assert dev_settings.profile == "dev"

        prod_settings = MoneyBinSettings(profile="prod")
        assert prod_settings.profile == "prod"

    def test_get_settings_uses_current_profile(self, mocker: MockerFixture):
        """Test that get_settings() uses the current profile."""
        mocker.patch.dict(
            os.environ,
            {
                "PLAID_CLIENT_ID": "test_client",
                "PLAID_SECRET": "test_secret",
            },
        )
        mocker.patch.object(Path, "exists", return_value=False)

        # Set current profile to alice
        set_current_profile("alice")

        # get_settings() should use alice profile
        settings = get_settings()
        assert settings.profile == "alice"

        # Reset to default
        set_current_profile("default")
