# ruff: noqa: S101
"""E2E smoke tests — commands that mutate state.

Each test creates its own isolated profile so mutations don't affect
other tests. Tests that need a fresh MONEYBIN_HOME use tmp_path
(function-scoped) instead of e2e_home (session-scoped).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.conftest import (
    FIXTURES_DIR,
    TEST_ENCRYPTION_KEY,
    TEST_PASSPHRASE,
    make_workflow_env,
    run_cli,
)

pytestmark = pytest.mark.e2e


class TestProfileLifecycle:
    """Profile create, switch, set, and delete."""

    def test_profile_create_switch_delete(
        self,
        tmp_path: Path,
    ) -> None:
        base_env = {"MONEYBIN_HOME": str(tmp_path)}

        # Create two profiles so we can switch away before deleting
        run_cli("profile", "create", "keeper", env=base_env)
        result = run_cli("profile", "create", "doomed", env=base_env)
        result.assert_success()

        # Switch to doomed
        result = run_cli("profile", "switch", "doomed", env=base_env)
        result.assert_success()

        # Switch back so doomed is not active
        result = run_cli("profile", "switch", "keeper", env=base_env)
        result.assert_success()

        # Delete doomed (--yes to skip confirmation)
        result = run_cli("profile", "delete", "doomed", "--yes", env=base_env)
        result.assert_success()

    def test_profile_set(self, tmp_path: Path) -> None:
        env = {"MONEYBIN_HOME": str(tmp_path), "MONEYBIN_PROFILE": "settest"}
        run_cli("profile", "create", "settest", env=env)
        result = run_cli(
            "profile",
            "set",
            "logging.level",
            "DEBUG",
            "--profile",
            "settest",
            env=env,
        )
        result.assert_success()


class TestDBInit:
    """Database initialization with different key modes.

    With null keyring (E2E default), db init stores the generated key
    as a no-op. The Database constructor falls through to the env var
    MONEYBIN_DATABASE__ENCRYPTION_KEY which we set to a fixed test key.
    This tests the init workflow end-to-end without touching the real
    system keychain.
    """

    def test_db_init_auto_key(self, tmp_path: Path) -> None:
        env = {
            "MONEYBIN_HOME": str(tmp_path),
            "MONEYBIN_PROFILE": "initauto",
            "MONEYBIN_DATABASE__ENCRYPTION_KEY": TEST_ENCRYPTION_KEY,
        }
        run_cli("profile", "create", "initauto", env=env)
        result = run_cli("db", "init", "--yes", env=env)
        result.assert_success()

    def test_db_init_passphrase(self, tmp_path: Path) -> None:
        env = {
            "MONEYBIN_HOME": str(tmp_path),
            "MONEYBIN_PROFILE": "initpp",
            "MONEYBIN_DATABASE__ENCRYPTION_KEY": TEST_ENCRYPTION_KEY,
        }
        run_cli("profile", "create", "initpp", env=env)
        passphrase_input = f"{TEST_PASSPHRASE}\n{TEST_PASSPHRASE}\n"
        result = run_cli(
            "db",
            "init",
            "--passphrase",
            "--yes",
            env=env,
            input_text=passphrase_input,
        )
        result.assert_success()


class TestDBOperations:
    """Backup, restore, lock/unlock, and key rotation."""

    def test_db_backup_and_restore(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "backup")
        backup_dir = tmp_path / "backups"
        backup_dir.mkdir()

        # Backup
        result = run_cli(
            "db", "backup", "--output", str(backup_dir / "test.duckdb"), env=env
        )
        result.assert_success()

        # Restore
        backup_file = next(backup_dir.glob("*.duckdb"), None)
        assert backup_file is not None, "Backup file was not created"
        result = run_cli("db", "restore", "--from", str(backup_file), "--yes", env=env)
        result.assert_success()

    def test_db_lock(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "locktest")
        result = run_cli("db", "lock", env=env)
        result.assert_success()

    def test_db_unlock_no_salt(self, tmp_path: Path) -> None:
        """Unlock without a passphrase salt should fail gracefully."""
        env = make_workflow_env(tmp_path, "unlocktest")
        result = run_cli("db", "unlock", env=env, input_text=f"{TEST_PASSPHRASE}\n")
        # With null keyring, no passphrase salt is stored → exit 1
        assert result.exit_code == 1
        assert "Traceback (most recent call last)" not in result.output

    def test_db_rotate_key(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "rotatetest")
        result = run_cli("db", "rotate-key", "--yes", env=env)
        result.assert_success()

    def test_db_migrate_apply(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "migrateapply")
        result = run_cli("db", "migrate", "apply", env=env)
        result.assert_success()

    def test_db_kill_no_processes(self, tmp_path: Path) -> None:
        """Db kill with no matching processes exits cleanly."""
        env = make_workflow_env(tmp_path, "killtest")
        result = run_cli("db", "kill", env=env)
        result.assert_success()


class TestTransformMutating:
    """Transform commands that modify the database."""

    def test_transform_apply(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "xformapply")
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

    def test_transform_audit(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "xformaudit")
        result = run_cli(
            "transform",
            "audit",
            "--start",
            "2020-01-01",
            "--end",
            "2020-12-31",
            env=env,
            timeout=180,
        )
        # May exit non-zero if no models have audits — no Python crash is the bar
        assert "Traceback (most recent call last)" not in result.output

    def test_transform_restate(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "xformrestate")
        result = run_cli(
            "transform",
            "restate",
            "--model",
            "core.fct_transactions",
            "--start",
            "2020-01-01",
            "--yes",
            env=env,
            timeout=180,
        )
        # May exit non-zero if model not materialized — no Python crash is the bar
        assert "Traceback (most recent call last)" not in result.output


class TestCategorizeMutating:
    """Categorization commands that write to the database."""

    def test_categorize_seed(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "catseed")
        result = run_cli("categorize", "seed", env=env)
        result.assert_success()

    def test_categorize_apply_rules(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "catrules")
        result = run_cli("categorize", "apply-rules", env=env)
        result.assert_success()


class TestMatchesMutating:
    """Matching commands that modify match state."""

    def test_matches_run(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "matchrun")
        result = run_cli("matches", "run", env=env)
        # May exit non-zero if no transforms have been run — no Python crash is the bar
        assert "Traceback (most recent call last)" not in result.output

    def test_matches_review_accept_all(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "matchreview")
        result = run_cli("matches", "review", "--accept-all", env=env)
        result.assert_success()

    def test_matches_backfill(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "matchbf")
        result = run_cli("matches", "backfill", env=env)
        # May exit non-zero if no transforms have been run — no Python crash is the bar
        assert "Traceback (most recent call last)" not in result.output

    def test_matches_undo_nonexistent(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "matchundo")
        result = run_cli("matches", "undo", "nonexistent-id", "--yes", env=env)
        # Should fail gracefully with "not found", not crash
        assert "Traceback (most recent call last)" not in result.output


class TestImportMutating:
    """Import commands that write data or modify formats."""

    def test_import_file_and_revert(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "importrev")
        fixture = FIXTURES_DIR / "tabular" / "standard.csv"

        # Import
        result = run_cli(
            "import",
            "file",
            str(fixture),
            "--account-id",
            "smoke-acct",
            "--skip-transform",
            env=env,
        )
        result.assert_success()

        # Revert with a fake ID — should fail gracefully, not crash
        result = run_cli("import", "revert", "nonexistent-id", "--yes", env=env)
        assert "Traceback (most recent call last)" not in result.output

    def test_import_delete_format(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "delfmt")
        result = run_cli(
            "import", "delete-format", "nonexistent-format", "--yes", env=env
        )
        assert "Traceback (most recent call last)" not in result.output


class TestSyntheticMutating:
    """Synthetic data generation commands."""

    def test_synthetic_generate(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "synthgen")
        result = run_cli(
            "synthetic",
            "generate",
            "--persona",
            "basic",
            "--profile",
            "synthgen",
            "--skip-transform",
            "--seed",
            "42",
            env=env,
            timeout=120,
        )
        result.assert_success()

    def test_synthetic_reset(self, tmp_path: Path) -> None:
        env = make_workflow_env(tmp_path, "synthreset")
        result = run_cli(
            "synthetic",
            "reset",
            "--persona",
            "basic",
            "--profile",
            "synthreset",
            "--skip-transform",
            "--seed",
            "42",
            "--yes",
            env=env,
            timeout=120,
        )
        # May exit non-zero if profile was not created by the generator
        assert "Traceback (most recent call last)" not in result.output


class TestLogsMutating:
    """Log management commands that delete files."""

    def test_logs_clean(self, tmp_path: Path) -> None:
        env = {"MONEYBIN_HOME": str(tmp_path), "MONEYBIN_PROFILE": "logstest"}
        run_cli("profile", "create", "logstest", env=env)
        result = run_cli("logs", "clean", "--older-than", "0d", env=env)
        result.assert_success()
