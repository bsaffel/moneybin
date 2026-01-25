"""Tests for user configuration management.

This module tests the user-level configuration stored in ~/.moneybin/config.yaml,
including profile normalization and default profile management.
"""

from pathlib import Path
from unittest.mock import mock_open

import pytest
from pytest_mock import MockerFixture

from moneybin.utils.user_config import (
    UserConfig,
    get_default_profile,
    get_user_config_path,
    load_user_config,
    normalize_profile_name,
    reset_user_config,
    save_user_config,
    set_default_profile,
)


class TestProfileNormalization:
    """Test suite for profile name normalization."""

    def test_normalize_lowercase_simple(self):
        """Test normalizing simple lowercase name."""
        assert normalize_profile_name("alice") == "alice"

    def test_normalize_uppercase_to_lowercase(self):
        """Test normalizing uppercase to lowercase."""
        assert normalize_profile_name("ALICE") == "alice"
        assert normalize_profile_name("BOB") == "bob"

    def test_normalize_mixed_case(self):
        """Test normalizing mixed case to lowercase."""
        assert normalize_profile_name("Alice") == "alice"
        assert normalize_profile_name("JohnDoe") == "johndoe"

    def test_normalize_spaces_to_hyphens(self):
        """Test normalizing spaces to hyphens."""
        assert normalize_profile_name("John Smith") == "john-smith"
        assert normalize_profile_name("Alice Work") == "alice-work"

    def test_normalize_underscores_to_hyphens(self):
        """Test normalizing underscores to hyphens."""
        assert normalize_profile_name("alice_work") == "alice-work"
        assert normalize_profile_name("bob_personal") == "bob-personal"

    def test_normalize_removes_special_characters(self):
        """Test that special characters are removed."""
        assert normalize_profile_name("alice@work") == "alicework"
        assert normalize_profile_name("bob!personal") == "bobpersonal"
        assert normalize_profile_name("john.smith") == "johnsmith"

    def test_normalize_consecutive_hyphens(self):
        """Test that consecutive hyphens are collapsed."""
        assert normalize_profile_name("alice--work") == "alice-work"
        assert normalize_profile_name("bob___personal") == "bob-personal"

    def test_normalize_leading_trailing_hyphens(self):
        """Test that leading/trailing hyphens are removed."""
        assert normalize_profile_name("-alice-") == "alice"
        assert normalize_profile_name("_bob_") == "bob"

    def test_normalize_empty_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            normalize_profile_name("")

    def test_normalize_whitespace_only_raises_error(self):
        """Test that whitespace-only name raises ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            normalize_profile_name("   ")

    def test_normalize_invalid_chars_only_raises_error(self):
        """Test that name with only invalid characters raises ValueError."""
        with pytest.raises(ValueError, match="no valid characters"):
            normalize_profile_name("@#$%")

    def test_normalize_complex_name(self):
        """Test normalizing a complex name with multiple transformations."""
        assert normalize_profile_name("John O'Brien-Smith") == "john-obrien-smith"
        assert normalize_profile_name("alice_work@HOME") == "alice-workhome"


class TestUserConfig:
    """Test suite for UserConfig model."""

    def test_user_config_default(self):
        """Test default UserConfig initialization."""
        config = UserConfig()
        assert config.default_profile is None

    def test_user_config_with_profile(self):
        """Test UserConfig with profile set."""
        config = UserConfig(default_profile="alice")
        assert config.default_profile == "alice"

    def test_user_config_normalizes_profile(self):
        """Test that UserConfig normalizes profile name."""
        config = UserConfig(default_profile="Alice Work")
        assert config.default_profile == "alice-work"


class TestUserConfigFileOperations:
    """Test suite for user config file operations."""

    def test_get_user_config_path(self):
        """Test getting user config path."""
        config_path = get_user_config_path()
        assert config_path == Path.home() / ".moneybin" / "config.yaml"

    def test_load_user_config_no_file(self, mocker: MockerFixture):
        """Test loading config when file doesn't exist."""
        mocker.patch.object(Path, "exists", return_value=False)

        config = load_user_config()
        assert isinstance(config, UserConfig)
        assert config.default_profile is None

    def test_load_user_config_with_file(self, mocker: MockerFixture):
        """Test loading config from existing file."""
        yaml_content = "default_profile: alice\n"

        mocker.patch.object(Path, "exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=yaml_content))

        config = load_user_config()
        assert config.default_profile == "alice"

    def test_save_user_config(self, tmp_path: Path, mocker: MockerFixture):
        """Test saving user config to file."""
        # Use temporary directory for testing
        test_config_path = tmp_path / ".moneybin" / "config.yaml"
        mocker.patch(
            "moneybin.utils.user_config.get_user_config_path",
            return_value=test_config_path,
        )

        config = UserConfig(default_profile="alice")
        save_user_config(config)

        assert test_config_path.exists()

        # Read back and verify
        with open(test_config_path) as f:
            content = f.read()
            assert "alice" in content

    def test_get_default_profile_not_set(self, mocker: MockerFixture):
        """Test getting default profile when not set."""
        mocker.patch.object(Path, "exists", return_value=False)

        assert get_default_profile() is None

    def test_get_default_profile_set(self, mocker: MockerFixture):
        """Test getting default profile when set."""
        yaml_content = "default_profile: alice\n"

        mocker.patch.object(Path, "exists", return_value=True)
        mocker.patch("builtins.open", mock_open(read_data=yaml_content))

        assert get_default_profile() == "alice"

    def test_set_default_profile(self, tmp_path: Path, mocker: MockerFixture):
        """Test setting default profile."""
        test_config_path = tmp_path / ".moneybin" / "config.yaml"
        mocker.patch(
            "moneybin.utils.user_config.get_user_config_path",
            return_value=test_config_path,
        )

        set_default_profile("Alice Work")

        # Verify file was created and profile was normalized
        assert test_config_path.exists()

        # Read back and verify
        profile = get_default_profile()
        assert profile == "alice-work"

    def test_reset_user_config(self, tmp_path: Path, mocker: MockerFixture):
        """Test resetting user config."""
        test_config_path = tmp_path / ".moneybin" / "config.yaml"
        mocker.patch(
            "moneybin.utils.user_config.get_user_config_path",
            return_value=test_config_path,
        )

        # Create a config file
        test_config_path.parent.mkdir(parents=True, exist_ok=True)
        test_config_path.write_text("default_profile: alice\n")

        assert test_config_path.exists()

        # Reset config
        reset_user_config()

        # Verify file was deleted
        assert not test_config_path.exists()

    def test_reset_user_config_no_file(self, mocker: MockerFixture):
        """Test resetting config when file doesn't exist."""
        mocker.patch.object(Path, "exists", return_value=False)

        # Should not raise error
        reset_user_config()
