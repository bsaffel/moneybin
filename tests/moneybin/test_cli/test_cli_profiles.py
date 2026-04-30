"""Tests for CLI profile handling.

This module tests the CLI's profile flag parsing, validation,
and integration with the configuration system.

These tests focus on profile mechanics (parsing, validation, propagation)
without requiring specific data source configurations like Plaid.
"""

from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager

from pytest_mock import MockerFixture
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.config import get_base_dir, get_current_profile
from moneybin.utils.user_config import normalize_profile_name
from tests.moneybin.conftest import temp_profile

runner = CliRunner()


@contextmanager
def _create_profile(name: str) -> Generator[str, None, None]:
    """Create a profile directory and clean up after."""
    normalized = normalize_profile_name(name)
    profile_dir = get_base_dir() / "profiles" / normalized
    profile_dir.mkdir(parents=True, exist_ok=True)
    (profile_dir / "config.yaml").touch()
    with temp_profile(name):
        yield normalized


class TestCLIProfileHandling:
    """Test suite for CLI profile handling."""

    def test_default_profile_from_config(self, mocker: MockerFixture) -> None:
        """Test that CLI uses saved default profile when no flag is provided."""
        mocker.patch.dict(os.environ, {})
        os.environ.pop("MONEYBIN_PROFILE", None)

        mocker.patch("moneybin.cli.utils.ensure_default_profile", return_value="test")

        with _create_profile("test"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(app, ["logs", "--print-path"])

            assert result.exit_code == 0
            assert get_current_profile() == "test"

    def test_explicit_profile_bob(self, mocker: MockerFixture) -> None:
        """Test setting a different user profile."""
        with _create_profile("bob"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(app, ["--profile=bob", "logs", "--print-path"])

            assert result.exit_code == 0
            assert get_current_profile() == "bob"

    def test_short_profile_flag(self, mocker: MockerFixture) -> None:
        """Test using short -p flag for profile."""
        with _create_profile("alice"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(app, ["-p", "alice", "logs", "--print-path"])

            assert result.exit_code == 0
            assert get_current_profile() == "alice"

    def test_profile_name_with_slash_gets_normalized(
        self, mocker: MockerFixture
    ) -> None:
        """Test that profile name with slash gets normalized (slash removed)."""
        with _create_profile("invalid/profile"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(
                app,
                ["--profile=invalid/profile", "logs", "--print-path"],
            )

            assert result.exit_code == 0
            assert get_current_profile() == "invalidprofile"

    def test_profile_name_with_space_gets_normalized(
        self, mocker: MockerFixture
    ) -> None:
        """Test that profile name with space gets normalized (space -> hyphen)."""
        with _create_profile("bad profile"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(
                app, ["--profile=bad profile", "logs", "--print-path"]
            )

            assert result.exit_code == 0
            assert get_current_profile() == "bad-profile"

    def test_profile_environment_variable(self, mocker: MockerFixture) -> None:
        """Test setting profile via MONEYBIN_PROFILE environment variable."""
        with _create_profile("alice"):
            mocker.patch.dict(os.environ, {"MONEYBIN_PROFILE": "alice"})
            mocker.patch("moneybin.cli.commands.logs.get_settings")

            result = runner.invoke(app, ["logs", "--print-path"])

            assert result.exit_code == 0
            assert get_current_profile() == "alice"

    def test_cli_flag_overrides_environment_variable(
        self, mocker: MockerFixture
    ) -> None:
        """Test that CLI flag takes precedence over environment variable."""
        with _create_profile("alice"), _create_profile("bob"):
            mocker.patch.dict(os.environ, {"MONEYBIN_PROFILE": "alice"})
            mocker.patch("moneybin.cli.commands.logs.get_settings")

            result = runner.invoke(app, ["--profile=bob", "logs", "--print-path"])

            assert result.exit_code == 0
            assert get_current_profile() == "bob"

    def test_profile_propagates_correctly(self, mocker: MockerFixture) -> None:
        """Test that profile is set correctly in the config system."""
        with _create_profile("alice"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(app, ["--profile=alice", "logs", "--print-path"])

            assert result.exit_code == 0
            assert get_current_profile() == "alice"

    def test_help_shows_profile_option(self) -> None:
        """Test that help text shows the profile option."""
        result = runner.invoke(app, ["--help"])

        assert result.exit_code == 0
        assert "--profile" in result.stdout or "-p" in result.stdout

    def test_valid_profile_with_dash(self, mocker: MockerFixture) -> None:
        """Test that profile names with dashes are valid."""
        with _create_profile("alice-personal"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(
                app,
                ["--profile=alice-personal", "logs", "--print-path"],
            )

            assert result.exit_code == 0
            assert get_current_profile() == "alice-personal"

    def test_profile_with_underscore_gets_normalized(
        self, mocker: MockerFixture
    ) -> None:
        """Test that profile names with underscores get normalized to hyphens."""
        with _create_profile("alice_work"):
            mocker.patch("moneybin.cli.commands.logs.get_settings")
            result = runner.invoke(
                app, ["--profile=alice_work", "logs", "--print-path"]
            )

            assert result.exit_code == 0
            assert get_current_profile() == "alice-work"


class TestProfileCommandsResolveProfileButSkipWizard:
    """Profile subcommands honor --profile/env but never trigger first-run wizard."""

    def test_profile_list_skips_ensure_default(self, mocker: MockerFixture) -> None:
        """Profile commands must never trigger the first-run wizard."""
        mocker.patch.dict(os.environ, {})
        os.environ.pop("MONEYBIN_PROFILE", None)

        mock_ensure = mocker.patch("moneybin.cli.utils.ensure_default_profile")

        result = runner.invoke(app, ["profile", "list"])

        assert result.exit_code == 0
        mock_ensure.assert_not_called()

    def test_profile_list_honors_profile_flag(self) -> None:
        """--profile flag should resolve current profile for profile commands too."""
        with _create_profile("alice"):
            result = runner.invoke(app, ["--profile=alice", "profile", "list"])

            assert result.exit_code == 0
            assert get_current_profile() == "alice"

    def test_profile_list_honors_env_var(self, mocker: MockerFixture) -> None:
        """MONEYBIN_PROFILE env var should resolve for profile commands too."""
        mocker.patch.dict(os.environ, {"MONEYBIN_PROFILE": "alice"})

        with _create_profile("alice"):
            result = runner.invoke(app, ["profile", "list"])

            assert result.exit_code == 0
            assert get_current_profile() == "alice"
