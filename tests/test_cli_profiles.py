"""Tests for CLI profile handling.

This module tests the CLI's profile flag parsing, validation,
and integration with the configuration system.

These tests focus on profile mechanics (parsing, validation, propagation)
without requiring specific data source configurations like Plaid.
"""

import os

from pytest_mock import MockerFixture
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.config import get_current_profile, set_current_profile

runner = CliRunner()


class TestCLIProfileHandling:
    """Test suite for CLI profile handling."""

    def setup_method(self) -> None:
        """Reset profile to default before each test."""
        set_current_profile("default")

    def test_default_profile_is_default(self) -> None:
        """Test that CLI defaults to 'default' profile when no flag is provided."""
        # Run a command without --profile flag (credentials list-services doesn't need Plaid)
        result = runner.invoke(app, ["credentials", "list-services"])

        # Should succeed and use default profile
        assert result.exit_code == 0
        assert get_current_profile() == "default"

    def test_explicit_profile_alice(self) -> None:
        """Test explicitly setting a user profile via CLI flag."""
        # Run with explicit --profile=alice
        result = runner.invoke(app, ["--profile=alice", "credentials", "list-services"])

        assert result.exit_code == 0
        assert get_current_profile() == "alice"

    def test_explicit_profile_bob(self) -> None:
        """Test setting a different user profile."""
        # Run with explicit --profile=bob
        result = runner.invoke(app, ["--profile=bob", "credentials", "list-services"])

        assert result.exit_code == 0
        assert get_current_profile() == "bob"

    def test_short_profile_flag(self) -> None:
        """Test using short -p flag for profile."""
        # Run with short flag -p
        result = runner.invoke(app, ["-p", "alice", "credentials", "list-services"])

        assert result.exit_code == 0
        assert get_current_profile() == "alice"

    def test_invalid_profile_name_with_slash(self) -> None:
        """Test that invalid profile name (with slash) raises error."""
        # Run with invalid profile containing slash
        result = runner.invoke(
            app, ["--profile=invalid/profile", "credentials", "list-services"]
        )

        # Should fail with non-zero exit code
        assert result.exit_code != 0

    def test_invalid_profile_name_with_space(self) -> None:
        """Test that invalid profile name (with space) raises error."""
        # Run with invalid profile containing space
        result = runner.invoke(
            app, ["--profile=bad profile", "credentials", "list-services"]
        )

        # Should fail with non-zero exit code
        assert result.exit_code != 0

    def test_profile_environment_variable(self, mocker: MockerFixture) -> None:
        """Test setting profile via MONEYBIN_PROFILE environment variable."""
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
        mocker.patch.dict(
            os.environ,
            {
                "MONEYBIN_PROFILE": "alice",
            },
        )

        # Run with explicit --profile=bob (should override env var)
        result = runner.invoke(app, ["--profile=bob", "credentials", "list-services"])

        assert result.exit_code == 0
        # Should use bob profile from CLI flag, not alice from env var
        assert get_current_profile() == "bob"

    def test_profile_propagates_correctly(self) -> None:
        """Test that profile is set correctly in the config system."""
        result = runner.invoke(app, ["--profile=alice", "credentials", "list-services"])

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
        result = runner.invoke(
            app, ["--profile=alice-personal", "credentials", "list-services"]
        )

        assert result.exit_code == 0
        assert get_current_profile() == "alice-personal"

    def test_valid_profile_with_underscore(self) -> None:
        """Test that profile names with underscores are valid."""
        result = runner.invoke(
            app, ["--profile=alice_work", "credentials", "list-services"]
        )

        assert result.exit_code == 0
        assert get_current_profile() == "alice_work"
