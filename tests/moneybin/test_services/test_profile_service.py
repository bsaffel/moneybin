"""Tests for profile lifecycle service."""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from moneybin.services.profile_service import (
    ProfileExistsError,
    ProfileNotFoundError,
    ProfileService,
)


@pytest.fixture(autouse=True)
def _skip_db_init() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
    """Prevent profile creation from hitting the real keychain."""
    with patch.object(ProfileService, "_init_database"):
        yield


class TestProfileCreate:
    """Test profile creation."""

    def test_create_profile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Creating a profile creates directory structure and config."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        profile_dir = tmp_path / "profiles" / "alice"
        assert profile_dir.exists()
        assert (profile_dir / "config.yaml").exists()
        assert (profile_dir / "logs").exists()
        assert (profile_dir / "temp").exists()

    def test_create_duplicate_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Creating an existing profile raises ProfileExistsError."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        with pytest.raises(ProfileExistsError):
            svc.create("alice")

    def test_create_sets_restrictive_dir_perms(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Profile dir + logs/ + temp/ are created at 0o700, not the umask default."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        profile_dir = tmp_path / "profiles" / "alice"
        # Mode 0o700 — owner rwx only; group/other have no access. Asserts
        # the umask-around-mkdir path in profile_service applied atomically.
        assert (profile_dir.stat().st_mode & 0o777) == 0o700
        assert ((profile_dir / "logs").stat().st_mode & 0o777) == 0o700
        assert ((profile_dir / "temp").stat().st_mode & 0o777) == 0o700

    def test_create_normalizes_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Profile name is normalized (lowercase, hyphens)."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("Alice Work")
        assert (tmp_path / "profiles" / "alice-work").exists()

    def test_create_with_init_inbox_creates_layout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """init_inbox=True provisions <inbox_root>/<profile>/{inbox,...}."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        inbox_root = tmp_path / "MoneyBin"
        monkeypatch.setenv("MONEYBIN_IMPORT___INBOX_ROOT", str(inbox_root))
        svc = ProfileService()
        svc.create("alice", init_inbox=True)
        profile_inbox = inbox_root / "alice"
        assert (profile_inbox / "inbox").is_dir()
        assert (profile_inbox / "processed").is_dir()
        assert (profile_inbox / "failed").is_dir()

    def test_create_without_init_inbox_skips_layout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Default (init_inbox=False) does not create the inbox tree."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        inbox_root = tmp_path / "MoneyBin"
        monkeypatch.setenv("MONEYBIN_IMPORT___INBOX_ROOT", str(inbox_root))
        svc = ProfileService()
        svc.create("alice")
        assert not (inbox_root / "alice").exists()

    def test_create_rolls_back_on_db_init_failure(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Profile directory is cleaned up when _init_database fails."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))

        with patch.object(
            ProfileService, "_init_database", side_effect=RuntimeError("keychain fail")
        ):
            svc = ProfileService()
            with pytest.raises(RuntimeError, match="keychain fail"):
                svc.create("rollback-test")

        # Directory must not exist after rollback
        assert not (tmp_path / "profiles" / "rollback-test").exists()


class TestProfileCreateRepairsBareDirectory:
    """`create()` completes an unregistered directory instead of dead-ending on it.

    A bare `moneybin db init`, a hand `mkdir`, or a partial delete leaves a profile
    directory with no `config.yaml`. Before this contract, `create()` raised off the
    *directory* (`mkdir(exist_ok=False)`), so such a profile could never be
    completed: `profile create` refused, `profile list` hid it, and it had no inbox.
    `ProfileExistsError` now means "a *registered* profile exists".
    """

    def test_create_completes_an_unregistered_bare_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A directory with no config.yaml is registered in place, not refused."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        profile_dir = tmp_path / "profiles" / "alice"
        profile_dir.mkdir(parents=True)

        svc = ProfileService()
        svc.create("alice")

        assert svc.is_registered("alice") is True
        assert (profile_dir / "config.yaml").exists()
        assert (profile_dir / "logs").is_dir()
        assert (profile_dir / "temp").is_dir()

    def test_create_registered_profile_still_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The exception survives — it just means "registered" now, not "directory"."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")

        with pytest.raises(ProfileExistsError):
            svc.create("alice")

    def test_create_tightens_permissions_on_an_adopted_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An adopted directory gets the same 0o700 the fresh path guarantees.

        Whatever made it — `db init`, a hand `mkdir` — did so at the ambient umask,
        typically 0o755. Once we register it, the encrypted database and privacy log
        live behind it, so it cannot keep permissions we never chose.
        """
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        profile_dir = tmp_path / "profiles" / "alice"
        profile_dir.mkdir(parents=True)
        profile_dir.chmod(0o755)  # world-readable, as an ambient-umask mkdir leaves it

        ProfileService().create("alice")

        assert (profile_dir.stat().st_mode & 0o777) == 0o700

    def test_create_never_clobbers_an_existing_database(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A `db init`'d database in a bare directory survives the repair untouched.

        This is the whole risk of the repair path: the directory we adopt may already
        hold the user's encrypted data.
        """
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        profile_dir = tmp_path / "profiles" / "alice"
        profile_dir.mkdir(parents=True)
        db_file = profile_dir / "moneybin.duckdb"
        db_file.write_bytes(b"pretend-encrypted-duckdb")

        svc = ProfileService()
        with patch.object(ProfileService, "_init_database") as init_db:
            svc.create("alice")

        init_db.assert_not_called()
        assert db_file.read_bytes() == b"pretend-encrypted-duckdb"

    def test_create_initializes_a_database_when_the_bare_directory_has_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A directory with no database still gets one — repair completes the profile."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        (tmp_path / "profiles" / "alice").mkdir(parents=True)

        svc = ProfileService()
        with patch.object(ProfileService, "_init_database") as init_db:
            svc.create("alice")

        init_db.assert_called_once()

    def test_failed_create_leaves_the_profile_retryable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A create() that dies part-way must not leave the profile half-registered.

        `config.yaml` is the commit marker: `list()` shows a profile once it exists,
        and `create()` refuses once it does. So a failed create that had already
        written it would strand the user — the command's own "run `profile create`
        to retry" hint would then hit `ProfileExistsError`, rebuilding the very dead
        end this contract removes. Registration must be the last step.

        Real trigger: `_init_database` fails whenever the OS keychain refuses the
        write (locked keychain, headless box, denied prompt).
        """
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        profile_dir = tmp_path / "profiles" / "alice"
        profile_dir.mkdir(parents=True)  # bare directory, no database

        svc = ProfileService()
        with (
            patch.object(
                ProfileService, "_init_database", side_effect=RuntimeError("keychain")
            ),
            pytest.raises(RuntimeError, match="keychain"),
        ):
            svc.create("alice")

        assert profile_dir.exists()  # adopted directory is never rolled back
        assert svc.is_registered("alice") is False  # ...but `create` can retry it
        assert "alice" not in [p["name"] for p in svc.list()]

    def test_failed_repair_does_not_delete_the_adopted_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Rollback deletes only a directory `create()` made itself.

        The rollback in the fresh-create path is `shutil.rmtree`. Applying it to an
        adopted directory would destroy the very database the user was trying to
        recover.
        """
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        profile_dir = tmp_path / "profiles" / "alice"
        profile_dir.mkdir(parents=True)
        db_file = profile_dir / "moneybin.duckdb"
        db_file.write_bytes(b"pretend-encrypted-duckdb")

        svc = ProfileService()
        with (
            patch.object(
                ProfileService, "_init_inbox", side_effect=RuntimeError("inbox fail")
            ),
            pytest.raises(RuntimeError, match="inbox fail"),
        ):
            svc.create("alice", init_inbox=True)

        assert profile_dir.exists()
        assert db_file.read_bytes() == b"pretend-encrypted-duckdb"


class TestProfileList:
    """Test profile listing."""

    def test_list_profiles(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Lists all profiles with active marker."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        config_path = tmp_path / "config.yaml"
        monkeypatch.setattr(
            "moneybin.utils.user_config.get_user_config_path",
            lambda: config_path,
        )
        svc = ProfileService()
        svc.create("alice")
        svc.create("bob")
        svc.switch("alice")
        profiles = svc.list()
        names = [p["name"] for p in profiles]
        assert "alice" in names
        assert "bob" in names
        alice = next(p for p in profiles if p["name"] == "alice")
        assert alice["active"] is True

    def test_list_empty(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty list when no profiles exist."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        assert svc.list() == []


class TestProfileSwitch:
    """Test profile switching."""

    def test_switch_profile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Switching updates global config active_profile."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        config_path = tmp_path / "config.yaml"
        monkeypatch.setattr(
            "moneybin.utils.user_config.get_user_config_path",
            lambda: config_path,
        )
        svc = ProfileService()
        svc.create("alice")
        svc.create("bob")
        svc.switch("bob")
        from moneybin.utils.user_config import load_user_config

        assert load_user_config().active_profile == "bob"

    def test_switch_nonexistent_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Switching to nonexistent profile raises ProfileNotFoundError."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        with pytest.raises(ProfileNotFoundError):
            svc.switch("nonexistent")


class TestProfileDelete:
    """Test profile deletion."""

    def test_delete_profile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Deleting removes profile directory."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        svc.delete("alice")
        assert not (tmp_path / "profiles" / "alice").exists()

    def test_delete_nonexistent_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Deleting nonexistent profile raises ProfileNotFoundError."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        with pytest.raises(ProfileNotFoundError):
            svc.delete("nonexistent")

    def test_delete_active_profile_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Deleting the active profile raises ValueError."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        config_path = tmp_path / "config.yaml"
        monkeypatch.setattr(
            "moneybin.utils.user_config.get_user_config_path",
            lambda: config_path,
        )
        svc = ProfileService()
        svc.create("alice")
        svc.switch("alice")
        with pytest.raises(ValueError, match="Cannot delete the active profile"):
            svc.delete("alice")

    def test_delete_clears_scoped_sync_tokens(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Deleting a profile clears its scoped broker sync tokens by profile_id."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        # The sync id is normally written lazily on first sync; seed it here.
        (tmp_path / "profiles" / "alice" / "profile_id").write_text("ab12cd34ef56")

        with patch(
            "moneybin.connectors.sync_client.SyncClient.clear_tokens_for_profile"
        ) as mock_clear:
            svc.delete("alice")

        mock_clear.assert_called_once_with("ab12cd34ef56")

    def test_delete_without_sync_id_skips_token_cleanup(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A profile with no profile_id file deletes without attempting token cleanup."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")  # create() does not write a profile_id file

        with patch(
            "moneybin.connectors.sync_client.SyncClient.clear_tokens_for_profile"
        ) as mock_clear:
            svc.delete("alice")

        mock_clear.assert_not_called()


class TestProfileShow:
    """Test profile show (resolved settings)."""

    def test_show_profile(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Show returns resolved settings for a profile."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        info = svc.show("alice")
        assert info["name"] == "alice"
        assert "database_path" in info
        assert "alice" in str(info["database_path"])

    def test_show_nonexistent_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Show raises ProfileNotFoundError for missing profile."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        with pytest.raises(ProfileNotFoundError):
            svc.show("ghost")

    def test_show_defaults_to_active(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Show with no name argument uses the active profile."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        config_path = tmp_path / "config.yaml"
        monkeypatch.setattr(
            "moneybin.utils.user_config.get_user_config_path",
            lambda: config_path,
        )
        svc = ProfileService()
        svc.create("alice")
        svc.switch("alice")
        info = svc.show()
        assert info["name"] == "alice"


class TestProfileSet:
    """Test setting config values on a profile."""

    def test_set_logging_level(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Set a string config value in profile config.yaml."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        svc.set("alice", "logging.level", "DEBUG")
        config_path = tmp_path / "profiles" / "alice" / "config.yaml"
        data = yaml.safe_load(config_path.read_text())
        assert data["logging"]["level"] == "DEBUG"

    def test_set_boolean_value(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Boolean string values are coerced to native bool."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        svc.set("alice", "logging.log_to_file", "false")
        config_path = tmp_path / "profiles" / "alice" / "config.yaml"
        data = yaml.safe_load(config_path.read_text())
        assert data["logging"]["log_to_file"] is False

    def test_set_integer_value(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Digit-only string values are coerced to int."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        svc.set("alice", "mcp.max_rows", "500")
        config_path = tmp_path / "profiles" / "alice" / "config.yaml"
        data = yaml.safe_load(config_path.read_text())
        assert data["mcp"]["max_rows"] == 500

    def test_set_invalid_key_format_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Keys not in section.field format raise ValueError."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        with pytest.raises(ValueError, match="section.field"):
            svc.set("alice", "badkey", "value")

    def test_set_unsafe_key_identifier_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Keys with uppercase or special chars in section/field raise ValueError."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("alice")
        with pytest.raises(ValueError, match="lowercase identifiers"):
            svc.set("alice", "Section.field", "value")
        with pytest.raises(ValueError, match="lowercase identifiers"):
            svc.set("alice", "section.__proto__", "value")

    def test_set_nonexistent_profile_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Setting on a nonexistent profile raises ProfileNotFoundError."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        with pytest.raises(ProfileNotFoundError):
            svc.set("ghost", "logging.level", "DEBUG")


class TestIsRegistered:
    """Test the registered-vs-unregistered signal used by onboarding guidance."""

    def test_unknown_profile_is_not_registered(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        assert ProfileService().is_registered("ghost") is False

    def test_created_profile_is_registered(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("real")  # writes config.yaml (_init_database stubbed by autouse)
        assert svc.is_registered("real") is True
