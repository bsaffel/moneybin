"""Tests for CLI profile handling.

This module tests the CLI's profile flag parsing, validation,
and integration with the configuration system.

These tests focus on profile mechanics (parsing, validation, propagation)
without requiring specific data source configurations like Plaid.
"""

from __future__ import annotations

import os

from conftest import temp_profile
from pytest_mock import MockerFixture
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.config import get_current_profile

runner = CliRunner()


class TestCLIProfileHandling:
    """Test suite for CLI profile handling."""

    def test_default_profile_from_config(self, mocker: MockerFixture) -> None:
        """Test that CLI uses saved default profile when no flag is provided."""
        # Mock the user config to return 'test' as the default profile
        mocker.patch("moneybin.cli.main.ensure_default_profile", return_value="test")

        # Run a command without --profile flag (credentials list-services doesn't need Plaid)
        result = runner.invoke(app, ["credentials", "list-services"])

        # Should succeed and use test profile from config
        assert result.exit_code == 0
        assert get_current_profile() == "test"

    def test_explicit_profile_alice(self) -> None:
        """Test explicitly setting a user profile via CLI flag."""
        with temp_profile("alice"):
            # Run with explicit --profile=alice
            result = runner.invoke(
                app, ["--profile=alice", "credentials", "list-services"]
            )

            assert result.exit_code == 0
            assert get_current_profile() == "alice"

    def test_explicit_profile_bob(self) -> None:
        """Test setting a different user profile."""
        with temp_profile("bob"):
            # Run with explicit --profile=bob
            result = runner.invoke(
                app, ["--profile=bob", "credentials", "list-services"]
            )

            assert result.exit_code == 0
            assert get_current_profile() == "bob"

    def test_short_profile_flag(self) -> None:
        """Test using short -p flag for profile."""
        with temp_profile("alice"):
            # Run with short flag -p
            result = runner.invoke(app, ["-p", "alice", "credentials", "list-services"])

            assert result.exit_code == 0
            assert get_current_profile() == "alice"

    def test_profile_name_with_slash_gets_normalized(self) -> None:
        """Test that profile name with slash gets normalized (slash removed)."""
        with temp_profile("invalid/profile"):
            # Run with profile containing slash - slash gets removed during normalization
            result = runner.invoke(
                app, ["--profile=invalid/profile", "credentials", "list-services"]
            )

            # Should succeed after normalization
            assert result.exit_code == 0
            # Slash is removed: "invalid/profile" -> "invalidprofile"
            assert get_current_profile() == "invalidprofile"

    def test_profile_name_with_space_gets_normalized(self) -> None:
        """Test that profile name with space gets normalized (space -> hyphen)."""
        with temp_profile("bad profile"):
            # Run with profile containing space - space gets converted to hyphen
            result = runner.invoke(
                app, ["--profile=bad profile", "credentials", "list-services"]
            )

            # Should succeed after normalization
            assert result.exit_code == 0
            # Space is converted to hyphen: "bad profile" -> "bad-profile"
            assert get_current_profile() == "bad-profile"

    def test_profile_environment_variable(self, mocker: MockerFixture) -> None:
        """Test setting profile via MONEYBIN_PROFILE environment variable."""
        with temp_profile("alice"):
            mocker.patch.dict(
                os.environ,
                {
                    "MONEYBIN_PROFILE": "alice",
                },
            )

            # Run without explicit flag (should use env var)
            result = runner.invoke(app, ["credentials", "list-services"])

            # Should use alice profile from environment variable
            assert result.exit_code == 0
            assert get_current_profile() == "alice"

    def test_cli_flag_overrides_environment_variable(
        self, mocker: MockerFixture
    ) -> None:
        """Test that CLI flag takes precedence over environment variable."""
        with temp_profile("alice"), temp_profile("bob"):
            mocker.patch.dict(
                os.environ,
                {
                    "MONEYBIN_PROFILE": "alice",
                },
            )

            # Run with explicit --profile=bob (should override env var)
            result = runner.invoke(
                app, ["--profile=bob", "credentials", "list-services"]
            )

            assert result.exit_code == 0
            # Should use bob profile from CLI flag, not alice from env var
            assert get_current_profile() == "bob"

    def test_profile_propagates_correctly(self) -> None:
        """Test that profile is set correctly in the config system."""
        with temp_profile("alice"):
            result = runner.invoke(
                app, ["--profile=alice", "credentials", "list-services"]
            )

            assert result.exit_code == 0
            # Verify profile was set correctly
            assert get_current_profile() == "alice"

    def test_help_shows_profile_option(self) -> None:
        """Test that help text shows the profile option."""
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        # Should show profile flag in help
        assert "--profile" in result.stdout or "-p" in result.stdout

    def test_valid_profile_with_dash(self) -> None:
        """Test that profile names with dashes are valid."""
        with temp_profile("alice-personal"):
            result = runner.invoke(
                app, ["--profile=alice-personal", "credentials", "list-services"]
            )

            assert result.exit_code == 0
            assert get_current_profile() == "alice-personal"

    def test_profile_with_underscore_gets_normalized(self) -> None:
        """Test that profile names with underscores get normalized to hyphens."""
        with temp_profile("alice_work"):
            result = runner.invoke(
                app, ["--profile=alice_work", "credentials", "list-services"]
            )

            assert result.exit_code == 0
            # Underscore is converted to hyphen: "alice_work" -> "alice-work"
            assert get_current_profile() == "alice-work"
