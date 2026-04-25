"""Tests for profile lifecycle service."""

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
def _skip_db_init():  # pyright: ignore[reportUnusedFunction]  # pytest autouse fixture
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

    def test_create_normalizes_name(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Profile name is normalized (lowercase, hyphens)."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        svc.create("Alice Work")
        assert (tmp_path / "profiles" / "alice-work").exists()


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
        svc.set("alice", "sync.enabled", "true")
        config_path = tmp_path / "profiles" / "alice" / "config.yaml"
        data = yaml.safe_load(config_path.read_text())
        assert data["sync"]["enabled"] is True

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
