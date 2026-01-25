"""Tests for profile-based configuration system.

This module tests the profile-based configuration loading, validation,
and environment file handling for dev and prod profiles.
"""

from __future__ import annotations

import os
from pathlib import Path

from conftest import temp_profile
from pytest_mock import MockerFixture

from moneybin.config import (
    MoneyBinSettings,
    clear_settings_cache,
    get_current_profile,
    get_settings,
    get_settings_for_profile,
    reload_settings,
    set_current_profile,
)


class TestProfileConfiguration:
    """Test suite for profile-based configuration."""

    def test_default_profile_is_test(self):
        """Test that test profile is 'test' in test environment."""
        # Profile should be 'test' from fixture
        assert get_current_profile() == "test"

    def test_set_current_profile_alice(self) -> None:
        """Test setting current profile to alice."""
        with temp_profile("alice"):
            set_current_profile("alice")
            assert get_current_profile() == "alice"

    def test_set_current_profile_bob(self) -> None:
        """Test setting current profile to bob."""
        with temp_profile("bob"):
            set_current_profile("bob")
            assert get_current_profile() == "bob"

    def test_set_profile_with_slash_normalizes(self) -> None:
        """Test that profile with slash has slash removed during normalization."""
        with temp_profile("invalid/profile"):
            set_current_profile("invalid/profile")
            # Slash gets removed during normalization
            assert get_current_profile() == "invalidprofile"

    def test_profile_with_space_is_normalized(self) -> None:
        """Test that profile with space is normalized to hyphen."""
        with temp_profile("bad profile"):
            set_current_profile("bad profile")
            assert get_current_profile() == "bad-profile"

    def test_get_settings_for_profile_dev(self, mocker: MockerFixture) -> None:
        """Test getting settings for dev profile explicitly."""
        with temp_profile("dev"):
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
            # Plaid credentials are now validated server-side through sync connector
            assert settings.sync is not None

    def test_get_settings_for_profile_prod(self, mocker: MockerFixture) -> None:
        """Test getting settings for prod profile explicitly."""
        with temp_profile("prod"):
            # Mock environment variables
            mocker.patch.dict(
                os.environ,
                {},
            )

            # Mock file existence to avoid actual file reads
            mocker.patch.object(Path, "exists", return_value=False)

            settings = get_settings_for_profile("prod")
            assert settings.profile == "prod"
            # Plaid credentials are now validated server-side through sync connector
            assert settings.sync is not None

    def test_get_settings_for_profile_with_special_chars(
        self, mocker: MockerFixture
    ) -> None:
        """Test that getting settings for profile with special chars normalizes them."""
        with temp_profile("test/profile"):
            mocker.patch.dict(os.environ, {})
            mocker.patch.object(Path, "exists", return_value=False)

            # Profile with slash gets normalized
            settings = get_settings_for_profile("test/profile")
            assert settings.profile == "testprofile"

    def test_valid_profile_names_work_and_normalize(self) -> None:
        """Test that various valid profile names are accepted and normalized."""
        # Test cases: (input, expected_normalized)
        test_cases = [
            ("alice", "alice"),
            ("bob", "bob"),
            ("dev", "dev"),
            ("prod", "prod"),
            ("alice-personal", "alice-personal"),
            ("bob_work", "bob-work"),  # underscore -> hyphen
            ("John Smith", "john-smith"),  # space -> hyphen, uppercase -> lowercase
            ("ALICE", "alice"),  # uppercase -> lowercase
        ]

        for input_name, expected in test_cases:
            with temp_profile(input_name):
                set_current_profile(input_name)
                assert get_current_profile() == expected

    def test_settings_cache_for_current_profile(self, mocker: MockerFixture) -> None:
        """Test that settings are cached for the current profile."""
        with temp_profile("dev"), temp_profile("prod"):
            # Mock environment variables
            mocker.patch.dict(
                os.environ,
                {},
            )

            # Mock file existence
            mocker.patch.object(Path, "exists", return_value=False)

            # Set to dev profile and get settings multiple times
            set_current_profile("dev")
            settings_dev_1 = get_settings()
            settings_dev_2 = get_settings()

            # Should be the same cached instance (no profile switch)
            assert settings_dev_1 is settings_dev_2
            assert settings_dev_1.profile == "dev"

            # Switch to prod profile
            set_current_profile("prod")
            settings_prod = get_settings()

            # Should be different instance (different profile)
            assert settings_dev_1 is not settings_prod
            assert settings_prod.profile == "prod"

            # Switch back to dev
            set_current_profile("dev")
            settings_dev_3 = get_settings()

            # Should be NEW instance (cache was invalidated on profile switch)
            assert settings_dev_1 is not settings_dev_3
            assert settings_dev_3.profile == "dev"

    def test_reload_settings_clears_cache(self, mocker: MockerFixture) -> None:
        """Test that reload_settings clears the cache for a profile."""
        with temp_profile("dev"):
            # Mock environment variables
            mocker.patch.dict(
                os.environ,
                {},
            )

            # Mock file existence
            mocker.patch.object(Path, "exists", return_value=False)

            # Get initial settings
            settings_1 = get_settings_for_profile("dev")

            # Reload settings
            settings_2 = reload_settings("dev")

            # Should be different instances (cache was cleared)
            assert settings_1 is not settings_2

    def test_profile_env_file_loading_uses_profile(self, mocker: MockerFixture) -> None:
        """Test that profile name is included in settings."""
        with temp_profile("alice"):
            # Mock file existence to test the file selection logic
            mocker.patch.dict(
                os.environ,
                {},
            )
            mocker.patch.object(Path, "exists", return_value=False)

            # Create settings for alice profile
            settings = MoneyBinSettings(profile="alice")
            assert settings.profile == "alice"

    def test_legacy_environment_variables(self, mocker: MockerFixture) -> None:
        """Test that legacy DUCKDB_PATH environment variable still works."""
        with temp_profile("dev"):
            # Mock legacy environment variables
            mocker.patch.dict(
                os.environ,
                {
                    "DUCKDB_PATH": "custom/path/db.duckdb",
                },
                clear=True,
            )

            # Mock file existence
            mocker.patch.object(Path, "exists", return_value=False)

            settings = MoneyBinSettings(profile="dev")

            # Legacy DUCKDB_PATH should still work
            assert settings.database.path == Path("custom/path/db.duckdb")

    def test_profile_field_in_settings(self, mocker: MockerFixture) -> None:
        """Test that profile field is properly set and normalized in settings."""
        with temp_profile("dev"), temp_profile("prod"), temp_profile("Alice_Work"):
            mocker.patch.dict(
                os.environ,
                {},
            )
            mocker.patch.object(Path, "exists", return_value=False)

            dev_settings = MoneyBinSettings(profile="dev")
            assert dev_settings.profile == "dev"

            prod_settings = MoneyBinSettings(profile="prod")
            assert prod_settings.profile == "prod"

            # Test normalization
            alice_settings = MoneyBinSettings(profile="Alice_Work")
            assert alice_settings.profile == "alice-work"

    def test_get_settings_uses_current_profile(self, mocker: MockerFixture) -> None:
        """Test that get_settings() uses the current profile."""
        with temp_profile("alice"):
            mocker.patch.dict(
                os.environ,
                {},
            )
            mocker.patch.object(Path, "exists", return_value=False)

            # Set current profile to alice
            set_current_profile("alice")

            # get_settings() should use alice profile
            settings = get_settings()
            assert settings.profile == "alice"

    def test_profile_aware_paths(self, mocker: MockerFixture) -> None:
        """Test that paths are profile-aware by default."""
        with temp_profile("alice"), temp_profile("bob"):
            # Clear environment variables that would override profile paths
            mocker.patch.dict(
                os.environ, {"DUCKDB_PATH": "", "LOG_FILE_PATH": ""}, clear=True
            )
            mocker.patch.object(Path, "exists", return_value=False)

            # Create settings for alice profile
            alice_settings = MoneyBinSettings(profile="alice")

            # Check that paths include the profile name
            assert alice_settings.database.path == Path("data/alice/moneybin.duckdb")
            assert alice_settings.data.raw_data_path == Path("data/alice/raw")
            assert alice_settings.data.temp_data_path == Path("data/alice/temp")
            assert alice_settings.logging.log_file_path == Path(
                "logs/alice/moneybin.log"
            )

            # Create settings for bob profile
            bob_settings = MoneyBinSettings(profile="bob")

            # Check that bob has different paths
            assert bob_settings.database.path == Path("data/bob/moneybin.duckdb")
            assert bob_settings.data.raw_data_path == Path("data/bob/raw")
            assert bob_settings.logging.log_file_path == Path("logs/bob/moneybin.log")

    def test_profile_isolation(self, mocker: MockerFixture) -> None:
        """Test that different profiles have completely isolated paths."""
        with temp_profile("alice"), temp_profile("bob"):
            # Clear environment variables that would override profile paths
            mocker.patch.dict(
                os.environ, {"DUCKDB_PATH": "", "LOG_FILE_PATH": ""}, clear=True
            )
            mocker.patch.object(Path, "exists", return_value=False)

            alice_settings = MoneyBinSettings(profile="alice")
            bob_settings = MoneyBinSettings(profile="bob")

            # Ensure database paths are different
            assert alice_settings.database.path != bob_settings.database.path
            assert "alice" in str(alice_settings.database.path)
            assert "bob" in str(bob_settings.database.path)

            # Ensure data paths are different
            assert alice_settings.data.raw_data_path != bob_settings.data.raw_data_path
            assert "alice" in str(alice_settings.data.raw_data_path)
            assert "bob" in str(bob_settings.data.raw_data_path)

    def test_clear_settings_cache_removes_cached_settings(
        self, mocker: MockerFixture
    ) -> None:
        """Test that clearing cache removes cached settings for current profile."""
        with temp_profile("alice"):
            mocker.patch.dict(os.environ, {})
            mocker.patch.object(Path, "exists", return_value=False)

            # Set profile to alice and get settings
            set_current_profile("alice")
            alice_settings_1 = get_settings()

            # Verify it's cached (same instance returned without profile switch)
            alice_settings_2 = get_settings()
            assert alice_settings_1 is alice_settings_2

            # Clear the cache
            clear_settings_cache()

            # Current profile should be reset to 'test'
            assert get_current_profile() == "test"

            # Get settings again for test profile - should be new instance
            test_settings = get_settings()
            assert test_settings is not alice_settings_1
            assert test_settings.profile == "test"

            # Switch back to alice - should create new instance (cache was cleared)
            set_current_profile("alice")
            alice_settings_3 = get_settings()
            assert alice_settings_1 is not alice_settings_3
            assert alice_settings_3.profile == "alice"
