"""Tests for profile CLI commands."""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.profile import app

runner = CliRunner()


class TestProfileCreate:
    """Tests for 'profile create' command."""

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_create_success(self, mock_cls: MagicMock) -> None:
        mock_svc = mock_cls.return_value
        mock_svc.create.return_value = Path("/fake/profiles/alice")
        result = runner.invoke(app, ["create", "alice"])
        assert result.exit_code == 0
        mock_svc.create.assert_called_once_with("alice")

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_create_duplicate_fails(self, mock_cls: MagicMock) -> None:
        from moneybin.services.profile_service import ProfileExistsError

        mock_svc = mock_cls.return_value
        mock_svc.create.side_effect = ProfileExistsError("exists")
        result = runner.invoke(app, ["create", "alice"])
        assert result.exit_code == 1


class TestProfileList:
    """Tests for 'profile list' command."""

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_list_profiles(
        self, mock_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_svc = mock_cls.return_value
        mock_svc.list.return_value = [
            {"name": "alice", "active": True, "path": "/fake"},
            {"name": "bob", "active": False, "path": "/fake"},
        ]
        with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.profile"):
            result = runner.invoke(app, ["list"])
        assert result.exit_code == 0
        assert "alice" in caplog.text

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_list_empty(
        self, mock_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_svc = mock_cls.return_value
        mock_svc.list.return_value = []
        with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.profile"):
            result = runner.invoke(app, ["list"])
        assert result.exit_code == 0
        assert "No profiles found" in caplog.text

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_list_marks_active(
        self, mock_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_svc = mock_cls.return_value
        mock_svc.list.return_value = [
            {"name": "alice", "active": True, "path": "/fake"},
            {"name": "bob", "active": False, "path": "/fake"},
        ]
        with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.profile"):
            result = runner.invoke(app, ["list"])
        assert result.exit_code == 0
        assert "(active)" in caplog.text


class TestProfileSwitch:
    """Tests for 'profile switch' command."""

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_switch_success(self, mock_cls: MagicMock) -> None:
        mock_svc = mock_cls.return_value
        result = runner.invoke(app, ["switch", "bob"])
        assert result.exit_code == 0
        mock_svc.switch.assert_called_once_with("bob")

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_switch_not_found_fails(self, mock_cls: MagicMock) -> None:
        from moneybin.services.profile_service import ProfileNotFoundError

        mock_svc = mock_cls.return_value
        mock_svc.switch.side_effect = ProfileNotFoundError("not found")
        result = runner.invoke(app, ["switch", "ghost"])
        assert result.exit_code == 1


class TestProfileDelete:
    """Tests for 'profile delete' command."""

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_delete_requires_confirmation(self, mock_cls: MagicMock) -> None:
        mock_svc = mock_cls.return_value
        result = runner.invoke(app, ["delete", "alice"], input="n\n")
        assert result.exit_code == 0
        mock_svc.delete.assert_not_called()

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_delete_with_yes_flag(self, mock_cls: MagicMock) -> None:
        mock_svc = mock_cls.return_value
        result = runner.invoke(app, ["delete", "alice", "--yes"])
        assert result.exit_code == 0
        mock_svc.delete.assert_called_once_with("alice")

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_delete_confirmed_interactively(self, mock_cls: MagicMock) -> None:
        mock_svc = mock_cls.return_value
        result = runner.invoke(app, ["delete", "alice"], input="y\n")
        assert result.exit_code == 0
        mock_svc.delete.assert_called_once_with("alice")

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_delete_not_found_fails(self, mock_cls: MagicMock) -> None:
        from moneybin.services.profile_service import ProfileNotFoundError

        mock_svc = mock_cls.return_value
        mock_svc.delete.side_effect = ProfileNotFoundError("not found")
        result = runner.invoke(app, ["delete", "ghost", "--yes"])
        assert result.exit_code == 1


class TestProfileShow:
    """Tests for 'profile show' command."""

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_show_active_profile(
        self, mock_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_svc = mock_cls.return_value
        mock_svc.show.return_value = {
            "name": "alice",
            "active": True,
            "path": "/fake/profiles/alice",
            "database_path": "/fake/profiles/alice/moneybin.duckdb",
            "database_exists": True,
            "config": {"logging": {"level": "INFO"}},
        }
        with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.profile"):
            result = runner.invoke(app, ["show"])
        assert result.exit_code == 0
        assert "alice" in caplog.text
        # CLI resolves the current profile before delegating to the service
        mock_svc.show.assert_called_once()

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_show_named_profile(self, mock_cls: MagicMock) -> None:
        mock_svc = mock_cls.return_value
        mock_svc.show.return_value = {
            "name": "bob",
            "active": False,
            "path": "/fake/profiles/bob",
            "database_path": "/fake/profiles/bob/moneybin.duckdb",
            "database_exists": False,
            "config": {},
        }
        result = runner.invoke(app, ["show", "bob"])
        assert result.exit_code == 0
        mock_svc.show.assert_called_once_with("bob")

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_show_not_found_fails(self, mock_cls: MagicMock) -> None:
        from moneybin.services.profile_service import ProfileNotFoundError

        mock_svc = mock_cls.return_value
        mock_svc.show.side_effect = ProfileNotFoundError("not found")
        result = runner.invoke(app, ["show", "ghost"])
        assert result.exit_code == 1

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_show_displays_db_status(
        self, mock_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_svc = mock_cls.return_value
        mock_svc.show.return_value = {
            "name": "alice",
            "active": True,
            "path": "/fake/profiles/alice",
            "database_path": "/fake/profiles/alice/moneybin.duckdb",
            "database_exists": False,
            "config": {},
        }
        with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.profile"):
            result = runner.invoke(app, ["show"])
        assert result.exit_code == 0
        assert "not created" in caplog.text


class TestProfileSet:
    """Tests for 'profile set' command."""

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_set_value(self, mock_cls: MagicMock) -> None:
        mock_svc = mock_cls.return_value
        mock_svc.list.return_value = [{"name": "alice", "active": True, "path": "/f"}]
        result = runner.invoke(app, ["set", "logging.level", "DEBUG"])
        assert result.exit_code == 0
        # CLI resolves the current profile (set to "test" by autouse fixture)
        # before delegating to the service.
        mock_svc.set.assert_called_once()

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_set_with_explicit_profile(self, mock_cls: MagicMock) -> None:
        mock_svc = mock_cls.return_value
        result = runner.invoke(
            app, ["set", "logging.level", "DEBUG", "--profile", "bob"]
        )
        assert result.exit_code == 0
        mock_svc.set.assert_called_once_with("bob", "logging.level", "DEBUG")

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_set_invalid_key_fails(self, mock_cls: MagicMock) -> None:
        mock_svc = mock_cls.return_value
        mock_svc.list.return_value = [{"name": "alice", "active": True, "path": "/f"}]
        mock_svc.set.side_effect = ValueError("Key must be section.field")
        result = runner.invoke(app, ["set", "badkey", "value"])
        assert result.exit_code == 1

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_set_profile_not_found_fails(self, mock_cls: MagicMock) -> None:
        from moneybin.services.profile_service import ProfileNotFoundError

        mock_svc = mock_cls.return_value
        mock_svc.list.return_value = []
        mock_svc.set.side_effect = ProfileNotFoundError("not found")
        result = runner.invoke(
            app, ["set", "logging.level", "DEBUG", "--profile", "ghost"]
        )
        assert result.exit_code == 1
