"""Tests for old config format migration."""

from pathlib import Path

import pytest
import yaml

from moneybin.services.profile_service import ProfileService


class TestMigrateOldLayout:
    """Test migration from data/<name>/ + logs/<name>/ to profiles/<name>/."""

    def test_migrates_database(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Migrates moneybin.duckdb from data/<name>/ to profiles/<name>/."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        old_data = tmp_path / "data" / "alice"
        old_data.mkdir(parents=True)
        (old_data / "moneybin.duckdb").write_text("fake-db")
        old_logs = tmp_path / "logs" / "alice"
        old_logs.mkdir(parents=True)
        (old_logs / "moneybin.log").write_text("fake-log")

        svc = ProfileService()
        migrated = svc.migrate_old_layout()
        assert migrated == ["alice"]

        new_dir = tmp_path / "profiles" / "alice"
        assert (new_dir / "moneybin.duckdb").exists()
        assert (new_dir / "logs" / "moneybin.log").exists()
        assert (new_dir / "config.yaml").exists()

    def test_migrates_global_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Migrates default_profile to active_profile in global config."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        config_path = tmp_path / "config.yaml"
        config_path.write_text("default_profile: alice\n")
        monkeypatch.setattr(
            "moneybin.utils.user_config.get_user_config_path",
            lambda: config_path,
        )
        old_data = tmp_path / "data" / "alice"
        old_data.mkdir(parents=True)
        (old_data / "moneybin.duckdb").write_text("fake-db")

        svc = ProfileService()
        svc.migrate_old_layout()

        data = yaml.safe_load(config_path.read_text())
        assert "active_profile" in data
        assert "default_profile" not in data

    def test_skip_if_no_old_layout(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No-op when no old layout exists."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        svc = ProfileService()
        assert svc.migrate_old_layout() == []

    def test_skip_if_already_migrated(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No-op when profiles/ already exists with content."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        (tmp_path / "profiles" / "alice" / "config.yaml").parent.mkdir(parents=True)
        (tmp_path / "profiles" / "alice" / "config.yaml").write_text(
            "logging:\n  level: INFO\n"
        )
        svc = ProfileService()
        assert svc.migrate_old_layout() == []
