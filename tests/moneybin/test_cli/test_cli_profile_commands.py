"""Tests for profile CLI commands."""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.profile import app
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols

runner = CliRunner()


@pytest.fixture(autouse=True)
def _interactive_profile_terminal(  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Profile tests default to the runner's noninteractive text contract."""
    policy = TerminalPolicy(
        output="text",
        interactive=False,
        page=False,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=24,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )

    def get_policy(**_kwargs: object) -> TerminalPolicy:
        return policy

    monkeypatch.setattr("moneybin.cli.commands.profile.get_terminal_policy", get_policy)


def _interactive_policy() -> TerminalPolicy:
    return TerminalPolicy(
        output="text",
        interactive=True,
        page=False,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=80,
        height=24,
        symbols=TerminalSymbols("OK", "!", "X", ">"),
        minus="-",
    )


class TestProfileCreate:
    """Tests for 'profile create' command."""

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_create_success(self, mock_cls: MagicMock) -> None:
        """Non-TTY default: init_inbox=False (safe for scripts)."""
        mock_svc = mock_cls.return_value
        mock_svc.create.return_value = Path("/fake/profiles/alice")
        result = runner.invoke(app, ["create", "alice"])
        assert result.exit_code == 0
        mock_svc.create.assert_called_once_with("alice", init_inbox=False)

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_create_with_init_inbox_flag(self, mock_cls: MagicMock) -> None:
        """--init-inbox forwards init_inbox=True to the service."""
        mock_svc = mock_cls.return_value
        mock_svc.create.return_value = Path("/fake/profiles/alice")
        result = runner.invoke(app, ["create", "alice", "--init-inbox"])
        assert result.exit_code == 0
        mock_svc.create.assert_called_once_with("alice", init_inbox=True)

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_create_with_no_init_inbox_flag(self, mock_cls: MagicMock) -> None:
        """--no-init-inbox forwards init_inbox=False explicitly."""
        mock_svc = mock_cls.return_value
        mock_svc.create.return_value = Path("/fake/profiles/alice")
        result = runner.invoke(app, ["create", "alice", "--no-init-inbox"])
        assert result.exit_code == 0
        mock_svc.create.assert_called_once_with("alice", init_inbox=False)

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_create_duplicate_fails(self, mock_cls: MagicMock) -> None:
        from moneybin.services.profile_service import ProfileExistsError

        mock_svc = mock_cls.return_value
        mock_svc.create.side_effect = ProfileExistsError("exists")
        result = runner.invoke(app, ["create", "alice"])
        assert result.exit_code == 1

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_create_says_completed_when_it_adopts_an_existing_directory(
        self, mock_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Adopting a directory that may already hold a database is not "Created".

        `create()` completes an unregistered directory in place, so the command can
        land on a `db init`'d profile holding real data. Reporting that as a fresh
        create would hide the adoption — the user needs to know which of the two
        happened to their data.
        """
        mock_svc = mock_cls.return_value
        mock_svc.exists.return_value = True  # bare, unregistered directory
        mock_svc.create.return_value = Path("/fake/profiles/alice")

        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["create", "alice"])

        assert result.exit_code == 0
        assert "Profile setup completed" in result.stdout
        assert "Profile created" not in result.stdout

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_create_says_created_for_a_fresh_profile(
        self, mock_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_svc = mock_cls.return_value
        mock_svc.exists.return_value = False
        mock_svc.create.return_value = Path("/fake/profiles/alice")

        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["create", "alice"])

        assert result.exit_code == 0
        assert "Profile created" in result.stdout

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_create_does_not_claim_to_preserve_a_database_that_never_existed(
        self, mock_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Adopting an empty directory initializes a NEW database — say nothing else.

        This is the one message whose job is telling the user what happened to their
        data; asserting a data-safety property about a database that never existed
        would be exactly backwards.
        """
        mock_svc = mock_cls.return_value
        mock_svc.exists.return_value = True  # bare directory...
        mock_svc.has_database.return_value = False  # ...with nothing in it
        mock_svc.create.return_value = Path("/fake/profiles/alice")

        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["create", "alice"])

        assert result.exit_code == 0
        assert "Profile setup completed" in result.stdout
        assert "preserved" not in result.stdout

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_create_reports_a_preserved_database_when_one_was_adopted(
        self, mock_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_svc = mock_cls.return_value
        mock_svc.exists.return_value = True
        mock_svc.has_database.return_value = True
        mock_svc.create.return_value = Path("/fake/profiles/alice")

        with caplog.at_level(logging.INFO):
            result = runner.invoke(app, ["create", "alice"])

        assert result.exit_code == 0
        assert "preserved" in result.stdout


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
        assert "alice" in result.stdout

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_list_empty(
        self, mock_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_svc = mock_cls.return_value
        mock_svc.list.return_value = []
        with caplog.at_level(logging.INFO, logger="moneybin.cli.commands.profile"):
            result = runner.invoke(app, ["list"])
        assert result.exit_code == 0
        assert "No profiles found" in result.stdout

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
        assert "(active)" in result.stdout


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
    def test_delete_requires_confirmation(
        self, mock_cls: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_svc = mock_cls.return_value
        monkeypatch.setattr(
            "moneybin.cli.commands.profile.get_terminal_policy", _interactive_policy
        )
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
    def test_delete_confirmed_interactively(
        self, mock_cls: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_svc = mock_cls.return_value
        monkeypatch.setattr(
            "moneybin.cli.commands.profile.get_terminal_policy", _interactive_policy
        )
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

    @patch("moneybin.cli.commands.profile.ProfileService")
    def test_delete_denied_cleanup_uses_error_boundary(
        self, mock_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        from moneybin.secrets import SecretStorageUnavailableError

        mock_cls.return_value.delete.side_effect = SecretStorageUnavailableError(
            "Unlock the OS keychain and retry."
        )
        with caplog.at_level(logging.ERROR):
            result = runner.invoke(app, ["delete", "alice", "--yes"])
        assert result.exit_code != 0
        assert not isinstance(result.exception, SecretStorageUnavailableError)
        assert "keychain" in (result.output + caplog.text).lower()
        assert "Profile deleted" not in result.stdout


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
        assert "alice" in result.stdout
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
    def test_show_not_found_fails(
        self, mock_cls: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A missing profile exits cleanly instead of raising a traceback.

        `exit_code == 1` alone cannot assert this: CliRunner reports 1 for an
        unhandled exception too. The discriminating checks are that nothing
        propagated out of the command and that the user was actually told why.
        """
        from moneybin.services.profile_service import ProfileNotFoundError

        mock_svc = mock_cls.return_value
        mock_svc.show.side_effect = ProfileNotFoundError("not found")
        with caplog.at_level(logging.ERROR, logger="moneybin.cli.utils"):
            result = runner.invoke(app, ["show", "ghost"])
        assert result.exit_code == 1
        assert not isinstance(result.exception, ProfileNotFoundError)
        assert "not found" in caplog.text

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
        assert "not created" in result.stdout


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
