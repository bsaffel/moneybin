"""Tests for old config format migration."""

from pathlib import Path
from unittest.mock import patch

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

    def test_migrates_backups(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Migrates backups/ directory from data/<name>/ to profiles/<name>/."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        old_data = tmp_path / "data" / "alice"
        old_data.mkdir(parents=True)
        (old_data / "moneybin.duckdb").write_text("fake-db")
        old_backups = old_data / "backups"
        old_backups.mkdir()
        (old_backups / "moneybin.duckdb.bak").write_text("fake-backup")

        svc = ProfileService()
        migrated = svc.migrate_old_layout()
        assert migrated == ["alice"]

        new_dir = tmp_path / "profiles" / "alice"
        assert (new_dir / "backups" / "moneybin.duckdb.bak").exists()
        assert not old_backups.exists()

    def test_migrates_backups_partial_retry(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Backup migration merges files when target dir already exists."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        old_data = tmp_path / "data" / "alice"
        old_data.mkdir(parents=True)
        (old_data / "moneybin.duckdb").write_text("fake-db")
        old_backups = old_data / "backups"
        old_backups.mkdir()
        (old_backups / "new-backup.bak").write_text("new-backup")

        # Pre-create target with an existing file (simulating partial retry)
        new_backups = tmp_path / "profiles" / "alice" / "backups"
        new_backups.mkdir(parents=True)
        (new_backups / "existing-backup.bak").write_text("existing-backup")

        svc = ProfileService()
        svc.migrate_old_layout()

        # Both files present after merge
        assert (new_backups / "existing-backup.bak").read_text() == "existing-backup"
        assert (new_backups / "new-backup.bak").read_text() == "new-backup"
        assert not old_backups.exists()

    def test_partial_failure_logs_warning_and_continues(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Continues migrating other profiles when one fails mid-move."""
        monkeypatch.setenv("MONEYBIN_HOME", str(tmp_path))
        # Create two old-layout profiles (sorted: alice before bob)
        for name in ("alice", "bob"):
            old_data = tmp_path / "data" / name
            old_data.mkdir(parents=True)
            (old_data / "moneybin.duckdb").write_text("fake-db")

        import shutil as _shutil

        original_move = _shutil.move
        call_count = 0

        def failing_move(src: str, dst: str) -> str:
            nonlocal call_count
            call_count += 1
            # Fail on the first shutil.move (alice's db file)
            if call_count == 1:
                raise OSError("disk full")
            return original_move(src, dst)

        with patch("shutil.move", side_effect=failing_move):
            svc = ProfileService()
            migrated = svc.migrate_old_layout()

        # alice failed, bob succeeded
        assert "bob" in migrated
        assert "alice" not in migrated
        assert "Partial migration" in caplog.text
