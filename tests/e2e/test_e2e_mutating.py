# ruff: noqa: S101
"""E2E smoke tests — commands that mutate state.

Each test creates its own isolated profile so mutations don't affect
other tests. Tests that need a fresh MONEYBIN_HOME use tmp_path
(function-scoped) instead of e2e_home (session-scoped).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from tests.e2e.conftest import (
    FIXTURES_DIR,
    TEST_ENCRYPTION_KEY,
    TEST_PASSPHRASE,
    base_env,
    make_workflow_env,
    make_workflow_env_fast,
    run_cli,
)

_has_duckdb_cli = shutil.which("duckdb") is not None

pytestmark = pytest.mark.e2e


class TestProfileLifecycle:
    """Profile create, switch, set, and delete."""

    def test_profile_create_initializes_database(
        self,
        tmp_path: Path,
    ) -> None:
        """Profile create must produce a usable encrypted database."""
        env = base_env(tmp_path, "dbcheck")
        result = run_cli("profile", "create", "dbcheck", env=env)
        result.assert_success()

        # Database file must exist after create
        db_path = tmp_path / "profiles" / "dbcheck" / "moneybin.duckdb"
        assert db_path.exists(), "profile create did not create database file"

        # Database must be usable — db info should succeed without db init
        result = run_cli("db", "info", env=env)
        result.assert_success()

    def test_profile_create_runs_migrations(
        self,
        tmp_path: Path,
    ) -> None:
        """Profile create runs all migrations so the DB is fully ready.

        After create, the version is recorded. A subsequent command must
        NOT re-run migrations — verified by checking that the migration
        summary line does not appear in db info output.
        """
        env = base_env(tmp_path, "migcheck")

        # Step 1: profile create should run migrations
        result = run_cli("profile", "create", "migcheck", env=env)
        result.assert_success()

        # Step 2: db info must succeed — DB is fully initialized
        result = run_cli("db", "info", env=env)
        result.assert_success()

        # Step 3: migrations must NOT re-run — version already matches
        assert "migration(s) applied" not in result.output, (
            "Migrations re-ran on db info — version was not recorded during "
            f"profile create.\nOutput: {result.output}"
        )

    def test_profile_create_switch_delete(
        self,
        tmp_path: Path,
    ) -> None:
        env = {"MONEYBIN_HOME": str(tmp_path)}

        # Create two profiles so we can switch away before deleting
        run_cli("profile", "create", "keeper", env=env)
        result = run_cli("profile", "create", "doomed", env=env)
        result.assert_success()

        # Switch to doomed
        result = run_cli("profile", "switch", "doomed", env=env)
        result.assert_success()

        # Switch back so doomed is not active
        result = run_cli("profile", "switch", "keeper", env=env)
        result.assert_success()

        # Delete doomed (--yes to skip confirmation)
        result = run_cli("profile", "delete", "doomed", "--yes", env=env)
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
        # Remove the auto-created DB so db init can create a new one
        # with a passphrase-derived key
        db_path = tmp_path / "profiles" / "initpp" / "moneybin.duckdb"
        db_path.unlink(missing_ok=True)
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

    def test_db_backup_and_restore(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "backup", _mutating_profile_template)
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

    def test_db_lock(self, _mutating_profile_template: Path, tmp_path: Path) -> None:
        env = make_workflow_env_fast(tmp_path, "locktest", _mutating_profile_template)
        result = run_cli("db", "lock", env=env)
        result.assert_success()

    def test_db_unlock_no_salt(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Unlock without a passphrase salt should fail gracefully."""
        env = make_workflow_env_fast(tmp_path, "unlocktest", _mutating_profile_template)
        result = run_cli("db", "unlock", env=env, input_text=f"{TEST_PASSPHRASE}\n")
        # With null keyring, no passphrase salt is stored → exit 1
        assert result.exit_code == 1
        assert "Traceback (most recent call last)" not in result.output

    def test_db_rotate_key(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "rotatetest", _mutating_profile_template)
        result = run_cli("db", "key", "rotate", "--yes", env=env)
        result.assert_success()

    def test_db_migrate_apply(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(
            tmp_path, "migrateapply", _mutating_profile_template
        )
        result = run_cli("db", "migrate", "apply", env=env)
        result.assert_success()

    def test_db_kill_no_processes(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Db kill with no matching processes exits cleanly."""
        env = make_workflow_env_fast(tmp_path, "killtest", _mutating_profile_template)
        result = run_cli("db", "kill", env=env)
        result.assert_success()


class TestTransformMutating:
    """Transform commands that modify the database."""

    def test_transform_apply(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "xformapply", _mutating_profile_template)
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

    def test_transform_state_persists_across_processes(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """SQLMesh state must land in the encrypted moneybin catalog, not memory.

        Regression: DuckDB cursors default to the `memory` catalog regardless
        of the parent connection's USE. Without cursor_init pinning the cursor
        to `moneybin`, SQLMesh writes _environments/_snapshots/_versions into
        memory.sqlmesh.* and they evaporate on process exit — leaving `status`
        to report "No SQLMesh environment initialized" right after `apply`.
        """
        env = make_workflow_env_fast(tmp_path, "xformstate", _mutating_profile_template)
        run_cli("transform", "apply", env=env, timeout=180).assert_success()
        result = run_cli("transform", "status", env=env, timeout=60)
        result.assert_success()
        assert "Environment: prod" in result.output
        assert "No SQLMesh environment initialized" not in result.output

    def test_transform_audit(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "xformaudit", _mutating_profile_template)
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

    def test_transform_restate(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(
            tmp_path, "xformrestate", _mutating_profile_template
        )
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

    def test_transform_seed(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Seeds materialize via the transform command, not categorize."""
        env = make_workflow_env_fast(
            tmp_path, "transformseed", _mutating_profile_template
        )
        result = run_cli("transform", "seed", env=env, timeout=180)
        result.assert_success()

    def test_categorize_apply_rules(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "catrules", _mutating_profile_template)
        result = run_cli("categorize", "apply-rules", env=env)
        result.assert_success()

    @pytest.mark.skipif(not _has_duckdb_cli, reason="DuckDB CLI not installed")
    def test_categorize_auto_review_and_confirm(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """auto-review surfaces a pending proposal; auto-confirm promotes it."""
        env = make_workflow_env_fast(tmp_path, "catauto", _mutating_profile_template)

        # auto-confirm's promotion path joins core.fct_transactions to
        # backfill matches, so transforms must materialize the (empty)
        # core schema before we exercise approve-all.
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

        # Insert a pending proposal directly — bulk_categorize is MCP-only and
        # has no CLI surface, so seed app.proposed_rules via db query. The
        # CLI we exercise is auto-review / auto-confirm / auto-stats /
        # auto-rules; how the proposal got there is irrelevant.
        insert_sql = (
            "INSERT INTO app.proposed_rules "
            "(proposed_rule_id, merchant_pattern, match_type, category, "
            "subcategory, status, trigger_count, source, sample_txn_ids) "
            "VALUES ('autoe2e0001', 'COFFEE SHOP', 'contains', 'Food & Dining', "
            "'Coffee', 'pending', 1, 'pattern_detection', ['t1'])"
        )
        result = run_cli("db", "query", insert_sql, env=env)
        result.assert_success()

        # auto-review lists the pending proposal
        result = run_cli("categorize", "auto", "review", env=env)
        result.assert_success()
        assert "autoe2e0001" in result.output, (
            f"auto-review did not surface proposal: {result.output}"
        )

        # auto-stats reports the pending proposal
        result = run_cli("categorize", "auto", "stats", env=env)
        result.assert_success()

        # auto-confirm --approve-all promotes it
        result = run_cli("categorize", "auto", "confirm", "--approve-all", env=env)
        result.assert_success()
        assert "Approved" in result.output, (
            f"auto-confirm missing approval message: {result.output}"
        )

        # auto-rules now lists at least one active rule, and auto-stats
        # reflects the promotion
        result = run_cli("categorize", "auto", "rules", env=env)
        result.assert_success()
        assert "autoe2e0001" not in result.output  # listed by rule_id, not proposal_id

        result = run_cli("categorize", "auto", "stats", env=env)
        result.assert_success()
        assert "Active auto-rules" in result.output


class TestMatchesMutating:
    """Matching commands that modify match state."""

    def test_matches_run(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "matchrun", _mutating_profile_template)
        result = run_cli("matches", "run", env=env)
        # May exit non-zero if no transforms have been run — no Python crash is the bar
        assert "Traceback (most recent call last)" not in result.output

    def test_matches_review_accept_all(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(
            tmp_path, "matchreview", _mutating_profile_template
        )
        result = run_cli("matches", "review", "--accept-all", env=env)
        result.assert_success()

    def test_matches_backfill(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "matchbf", _mutating_profile_template)
        result = run_cli("matches", "backfill", env=env)
        # May exit non-zero if no transforms have been run — no Python crash is the bar
        assert "Traceback (most recent call last)" not in result.output

    def test_matches_undo_nonexistent(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "matchundo", _mutating_profile_template)
        result = run_cli("matches", "undo", "nonexistent-id", "--yes", env=env)
        # Should fail gracefully with "not found", not crash
        assert "Traceback (most recent call last)" not in result.output


class TestImportMutating:
    """Import commands that write data or modify formats."""

    def test_import_file_and_revert(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "importrev", _mutating_profile_template)
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

    def test_import_delete_format(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "delfmt", _mutating_profile_template)
        result = run_cli(
            "import", "formats", "delete", "nonexistent-format", "--yes", env=env
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
        result = run_cli("logs", "--prune", "--older-than", "0d", env=env)
        result.assert_success()
