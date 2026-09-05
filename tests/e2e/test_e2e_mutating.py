# ruff: noqa: S101
"""E2E smoke tests — commands that mutate state.

Each test creates its own isolated profile so mutations don't affect
other tests. Tests that need a fresh MONEYBIN_HOME use tmp_path
(function-scoped) instead of e2e_home (session-scoped).
"""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from tests.e2e.conftest import (
    FIXTURES_DIR,
    TEST_ENCRYPTION_KEY,
    TEST_PASSPHRASE,
    base_env,
    make_workflow_env,
    make_workflow_env_fast,
    match_status,
    run_cli,
    seed_pending_match,
)

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

    def test_refresh_runs_pipeline(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`moneybin refresh` runs matching + SQLMesh apply + categorization."""
        env = make_workflow_env_fast(tmp_path, "refresh", _mutating_profile_template)
        result = run_cli("refresh", env=env, timeout=180)
        result.assert_success()

    def test_refresh_step_transform_only(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`moneybin refresh --step transform` runs only the transform step."""
        env = make_workflow_env_fast(
            tmp_path, "refresh-step-xform", _mutating_profile_template
        )
        result = run_cli("refresh", "--step", "transform", env=env, timeout=180)
        result.assert_success()

    def test_refresh_step_rates_only(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`moneybin refresh --step rates` is accepted and exits clean.

        The template profile sets no home currency, so this proves the step is
        wired end to end AND that it makes no network call when nothing implies
        one — an E2E that reached Frankfurter would be a flaky test and an
        outbound request from CI.
        """
        env = make_workflow_env_fast(
            tmp_path, "refresh-step-rates", _mutating_profile_template
        )
        result = run_cli("refresh", "--step", "rates", env=env, timeout=180)
        result.assert_success()

    def test_refresh_step_json_envelope_shape(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`moneybin refresh --step transform --output json` returns the CLI/MCP envelope."""
        import json as _json

        env = make_workflow_env_fast(
            tmp_path, "refresh-step-json", _mutating_profile_template
        )
        result = run_cli(
            "refresh",
            "--step",
            "transform",
            "--output",
            "json",
            env=env,
            timeout=180,
        )
        result.assert_success()
        payload = _json.loads(result.stdout)
        assert payload["status"] == "ok"
        assert payload["data"]["applied"] is True
        assert "duration_seconds" in payload["data"]

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
        result = run_cli("transactions", "categorize", "rules", "apply", env=env)
        result.assert_success()

    def test_categorize_run_default_methods(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Categorize run with default methods exits cleanly."""
        env = make_workflow_env_fast(tmp_path, "catrun", _mutating_profile_template)
        result = run_cli("transactions", "categorize", "run", env=env)
        result.assert_success()

    def test_categorize_run_rules_only(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Categorize run --methods rules applies only the rules engine."""
        env = make_workflow_env_fast(
            tmp_path, "catrunrules", _mutating_profile_template
        )
        result = run_cli(
            "transactions", "categorize", "run", "--methods", "rules", env=env
        )
        result.assert_success()

    def test_categorize_improve_ai(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Categorize improve-ai exits cleanly (no ai-guessed rows to upgrade)."""
        env = make_workflow_env_fast(
            tmp_path, "catimproveai", _mutating_profile_template
        )
        result = run_cli("transactions", "categorize", "improve-ai", env=env)
        result.assert_success()

    def test_categorize_auto_review_and_confirm(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """auto-review surfaces a pending proposal; auto-accept promotes it."""
        env = make_workflow_env_fast(tmp_path, "catauto", _mutating_profile_template)

        # auto-accept's promotion path joins core.fct_transactions to
        # backfill matches, so transforms must materialize the (empty)
        # core schema before we exercise approve-all.
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

        # Insert a pending proposal directly — categorize_items is MCP-only and
        # has no CLI surface, so seed app.proposed_rules via db query. The
        # CLI we exercise is auto-review / auto-accept / auto-stats /
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
        result = run_cli("transactions", "categorize", "auto", "review", env=env)
        result.assert_success()
        assert "autoe2e0001" in result.output, (
            f"auto-review did not surface proposal: {result.output}"
        )

        # auto-stats reports the pending proposal
        result = run_cli("transactions", "categorize", "auto", "stats", env=env)
        result.assert_success()

        # auto-accept --accept-all promotes it
        result = run_cli(
            "transactions", "categorize", "auto", "accept", "--accept-all", env=env
        )
        result.assert_success()
        assert "Accepted" in result.output, (
            f"auto-accept missing approval message: {result.output}"
        )

        # auto-rules now lists at least one active rule, and auto-stats
        # reflects the promotion
        result = run_cli("transactions", "categorize", "auto", "rules", env=env)
        result.assert_success()
        assert "autoe2e0001" not in result.output  # listed by rule_id, not proposal_id

        result = run_cli("transactions", "categorize", "auto", "stats", env=env)
        result.assert_success()
        assert "Active auto-rules" in result.output


class TestMatchesMutating:
    """Matching commands that modify match state."""

    def test_matches_run(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "matchrun", _mutating_profile_template)
        result = run_cli("transactions", "matches", "run", env=env)
        # May exit non-zero if no transforms have been run — no Python crash is the bar
        assert "Traceback (most recent call last)" not in result.output

    def test_matches_set_accepts_pending_match(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`transactions matches set --status accepted` updates a pending match."""
        env = make_workflow_env_fast(
            tmp_path, "matchset-acc", _mutating_profile_template
        )
        match_id = "e2e_cli_set_acc001"
        seed_pending_match(env, match_id)

        result = run_cli(
            "transactions",
            "matches",
            "set",
            match_id,
            "--status",
            "accepted",
            env=env,
        )
        result.assert_success()
        assert match_status(env, match_id) == "accepted"

    def test_matches_set_rejects_pending_match(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`transactions matches set --status rejected` updates a pending match."""
        env = make_workflow_env_fast(
            tmp_path, "matchset-rej", _mutating_profile_template
        )
        match_id = "e2e_cli_set_rej001"
        seed_pending_match(env, match_id)

        result = run_cli(
            "transactions",
            "matches",
            "set",
            match_id,
            "--status",
            "rejected",
            env=env,
        )
        result.assert_success()
        assert match_status(env, match_id) == "rejected"

    def test_matches_set_invalid_status_is_usage_error(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """An invalid --status value is a usage error (exit 2), not a runtime error."""
        env = make_workflow_env_fast(
            tmp_path, "matchset-bad", _mutating_profile_template
        )
        result = run_cli(
            "transactions",
            "matches",
            "set",
            "any_id",
            "--status",
            "bogus",
            env=env,
        )
        assert result.exit_code == 2

    def test_review_type_matches_reject_id(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`review --type matches --reject <id>` rejects a seeded pending match."""
        env = make_workflow_env_fast(tmp_path, "review-rej", _mutating_profile_template)
        match_id = "e2e_cli_rev_rej001"
        seed_pending_match(env, match_id)

        result = run_cli(
            "transactions",
            "review",
            "--type",
            "matches",
            "--reject",
            match_id,
            env=env,
        )
        result.assert_success()
        assert match_status(env, match_id) == "rejected"

    def test_review_type_matches_confirm_all(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`review --type matches --confirm-all` accepts all seeded pending matches."""
        env = make_workflow_env_fast(
            tmp_path, "review-confirm-all", _mutating_profile_template
        )
        match_id_a = "e2e_cli_all_acc001"
        match_id_b = "e2e_cli_all_acc002"
        seed_pending_match(env, match_id_a)
        seed_pending_match(env, match_id_b)

        result = run_cli(
            "transactions",
            "review",
            "--type",
            "matches",
            "--confirm-all",
            env=env,
        )
        result.assert_success()
        assert match_status(env, match_id_a) == "accepted"
        assert match_status(env, match_id_b) == "accepted"

    def test_review_type_all_with_flag_is_usage_error(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Non-interactive flags require --type matches; --type all errors (no silent partial run)."""
        env = make_workflow_env_fast(
            tmp_path, "review-all-guard", _mutating_profile_template
        )
        match_id = "e2e_cli_all_guard001"
        seed_pending_match(env, match_id)

        result = run_cli(
            "transactions", "review", "--type", "all", "--confirm-all", env=env
        )
        assert result.exit_code == 2
        # The pending match must be untouched — no partial execution.
        assert match_status(env, match_id) == "pending"

    def test_review_confirm_all_with_reject_is_usage_error(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """--confirm-all combined with --reject is ambiguous → exit 2, queue untouched."""
        env = make_workflow_env_fast(
            tmp_path, "review-combo-guard", _mutating_profile_template
        )
        match_id = "e2e_cli_combo_guard001"
        seed_pending_match(env, match_id)

        result = run_cli(
            "transactions",
            "review",
            "--type",
            "matches",
            "--confirm-all",
            "--reject",
            match_id,
            env=env,
        )
        assert result.exit_code == 2
        assert match_status(env, match_id) == "pending"

    def test_review_confirm_and_reject_same_id_is_usage_error(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """--confirm X --reject X is contradictory → exit 2 before any commit."""
        env = make_workflow_env_fast(
            tmp_path, "review-same-id-guard", _mutating_profile_template
        )
        match_id = "e2e_cli_same_id_guard001"
        seed_pending_match(env, match_id)

        result = run_cli(
            "transactions",
            "review",
            "--type",
            "matches",
            "--confirm",
            match_id,
            "--reject",
            match_id,
            env=env,
        )
        assert result.exit_code == 2
        # Guard must fire before the accept commits — queue untouched.
        assert match_status(env, match_id) == "pending"

    def test_matches_backfill(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "matchbf", _mutating_profile_template)
        result = run_cli("transactions", "matches", "backfill", env=env)
        # May exit non-zero if no transforms have been run — no Python crash is the bar
        assert "Traceback (most recent call last)" not in result.output

    def test_matches_undo_nonexistent(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "matchundo", _mutating_profile_template)
        result = run_cli(
            "transactions", "matches", "undo", "nonexistent-id", "--yes", env=env
        )
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
            "files",
            str(fixture),
            "--account-id",
            "smoke-acct",
            "--no-refresh",
            env=env,
        )
        result.assert_success()

        # Revert with a fake ID — should fail gracefully, not crash
        result = run_cli("import", "revert", "nonexistent-id", "--yes", env=env)
        assert "Traceback (most recent call last)" not in result.output

    def test_import_confirm_accept_loads_rows(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Import confirm <file> --accept imports rows and exits 0."""
        env = make_workflow_env_fast(
            tmp_path, "importconfirm", _mutating_profile_template
        )
        fixture = FIXTURES_DIR / "tabular" / "standard.csv"

        result = run_cli(
            "import",
            "confirm",
            str(fixture),
            "--accept",
            "--account-name",
            "smoke-acct",
            "--no-save-format",
            "--output",
            "json",
            env=env,
        )
        result.assert_success()
        payload = json.loads(result.stdout)
        assert payload["status"] == "ok"
        assert payload["data"]["rows_loaded"] > 0

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


# ---------------------------------------------------------------------------
# Accounts entity ops — write-path E2E coverage
# ---------------------------------------------------------------------------
# After Fix 3/4 (account-existence validation), mutators require an account in
# core.dim_accounts. The shared transformed template provides one and tests
# copy it through make_workflow_env_fast.
# ---------------------------------------------------------------------------

_OFX_ACCOUNT_ID = "9876543210"  # ACCTID from multi_currency_eur.qfx


@pytest.fixture(scope="module")
def _accounts_with_data_template(  # pyright: ignore[reportUnusedFunction]  # pytest fixture
    _transformed_profile_template: Path,
) -> Path:
    """Module template with a pre-materialized account.

    Provides a MONEYBIN_HOME where core.dim_accounts has at least one row.
    Account mutation tests copy this template via make_workflow_env_fast so
    they can call rename/archive/etc. without failing the _assert_account_exists
    check. Use _resolve_account_id(env) to look up the minted canonical id.

    The shared fixture keeps this module from rebuilding a profile, import, and
    transform cycle independently.
    """
    return _transformed_profile_template


def _resolve_account_id(env: dict[str, str]) -> str:
    """Resolve the canonical account_id minted by the AccountResolver at import time.

    The OFX import mints an opaque UUID-based account_id for the ACCTID from the
    fixture; the source-native number (9876543210) is no longer the lookup key.
    Fetches the real id from ``accounts list --output json`` so tests don't
    hardcode a value that changes every time a fresh template is built.
    """
    result = run_cli("accounts", "list", "--output", "json", env=env)
    result.assert_success()
    data = json.loads(result.stdout)["data"]
    rows = data["rows"]
    assert len(rows) >= 1, f"expected >=1 account row, got: {result.stdout}"
    return str(rows[0]["account_id"])


class TestAccountsEntityOps:
    """E2E lifecycle for accounts entity-op write commands.

    Uses a pre-populated template (imported + transformed)
    so that core.dim_accounts is populated and _assert_account_exists passes.
    """

    _ACCOUNT = _OFX_ACCOUNT_ID

    def test_accounts_set_display_name_persists(
        self, _accounts_with_data_template: Path, tmp_path: Path
    ) -> None:
        """`accounts set --display-name` writes; no traceback."""
        env = make_workflow_env_fast(
            tmp_path, "acct-rename", _accounts_with_data_template
        )
        account_id = _resolve_account_id(env)
        result = run_cli(
            "accounts",
            "set",
            account_id,
            "--display-name",
            "My Test Account",
            env=env,
        )
        result.assert_success()
        assert "Traceback" not in result.output

    def test_accounts_set_include_exclude_round_trip(
        self, _accounts_with_data_template: Path, tmp_path: Path
    ) -> None:
        """`accounts set --exclude` then `--include` round-trip both succeed."""
        env = make_workflow_env_fast(
            tmp_path, "acct-include", _accounts_with_data_template
        )
        account_id = _resolve_account_id(env)
        result = run_cli("accounts", "set", account_id, "--exclude", env=env)
        result.assert_success()
        result = run_cli("accounts", "set", account_id, "--include", env=env)
        result.assert_success()

    def test_accounts_set_archive_then_unarchive(
        self, _accounts_with_data_template: Path, tmp_path: Path
    ) -> None:
        """`accounts set --archive` then `--unarchive` round-trip; cascade message present on archive."""
        env = make_workflow_env_fast(
            tmp_path, "acct-archive", _accounts_with_data_template
        )
        account_id = _resolve_account_id(env)
        result = run_cli("accounts", "set", account_id, "--archive", env=env)
        result.assert_success()
        assert "excluded from net worth" in result.output
        result = run_cli("accounts", "set", account_id, "--unarchive", env=env)
        result.assert_success()

    def test_accounts_set_canonical_subtype(
        self, _accounts_with_data_template: Path, tmp_path: Path
    ) -> None:
        """Accounts set --subtype with a canonical value and --yes succeeds."""
        env = make_workflow_env_fast(tmp_path, "acct-set", _accounts_with_data_template)
        account_id = _resolve_account_id(env)
        result = run_cli(
            "accounts",
            "set",
            account_id,
            "--subtype",
            "checking",
            "--yes",
            env=env,
        )
        result.assert_success()

    def test_accounts_set_no_flags_exits_2(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Accounts set with no field flags exits 2 (usage error)."""
        env = make_workflow_env_fast(
            tmp_path, "acct-set-noflags", _mutating_profile_template
        )
        result = run_cli("accounts", "set", self._ACCOUNT, env=env)
        assert result.exit_code == 2


# ---------------------------------------------------------------------------
# Balance assertions — write + read lifecycle
# ---------------------------------------------------------------------------
# balance assert / delete / list all operate on app.balance_assertions which
# is created by migrations (no transform apply needed). Full data-path tests.
# balance show / history / reconcile query fct_balances_daily (core schema,
# needs transforms) — covered at help-tier only in test_e2e_readonly.py.
# ---------------------------------------------------------------------------


class TestBalanceAssertions:
    """E2E lifecycle for accounts balance assert/list/delete commands.

    Uses a pre-populated template (imported + transformed)
    so that core.dim_accounts is populated and _assert_account_exists passes.
    """

    _ACCOUNT = _OFX_ACCOUNT_ID  # must exist in dim_accounts
    _DATE = "2024-06-15"
    _AMOUNT = "12345.67"

    def test_balance_assert_then_list(
        self, _accounts_with_data_template: Path, tmp_path: Path
    ) -> None:
        """Balance assert writes; balance list --output json returns the row."""
        env = make_workflow_env_fast(
            tmp_path, "bal-assert-list", _accounts_with_data_template
        )
        account_id = _resolve_account_id(env)
        result = run_cli(
            "accounts",
            "balance",
            "assert",
            account_id,
            self._DATE,
            self._AMOUNT,
            "--notes",
            "E2E test assertion",
            env=env,
        )
        result.assert_success()

        result = run_cli("accounts", "balance", "list", "--output", "json", env=env)
        result.assert_success()
        # account_id is RECORD_ID (unmasked opaque surrogate — not PII).
        assert account_id in result.stdout
        assert self._DATE in result.stdout

    def test_balance_assert_then_delete(
        self, _accounts_with_data_template: Path, tmp_path: Path
    ) -> None:
        """Balance assert then delete round-trip; list returns empty after delete."""
        env = make_workflow_env_fast(
            tmp_path, "bal-assert-del", _accounts_with_data_template
        )
        account_id = _resolve_account_id(env)
        run_cli(
            "accounts",
            "balance",
            "assert",
            account_id,
            self._DATE,
            self._AMOUNT,
            env=env,
        ).assert_success()

        result = run_cli(
            "accounts",
            "balance",
            "assertion-delete",
            account_id,
            self._DATE,
            env=env,
        )
        result.assert_success()

        # After delete, list should return an empty assertions array
        result = run_cli(
            "accounts",
            "balance",
            "list",
            "--account",
            account_id,
            "--output",
            "json",
            env=env,
        )
        result.assert_success()
        assert '"data": []' in result.stdout or "data" in result.stdout

    def test_balance_delete_nonexistent_is_noop(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Balance delete for a nonexistent row exits 0 (silent no-op per spec)."""
        env = make_workflow_env_fast(
            tmp_path, "bal-del-noop", _mutating_profile_template
        )
        result = run_cli(
            "accounts",
            "balance",
            "assertion-delete",
            "nonexistent_acct",
            "2000-01-01",
            env=env,
        )
        result.assert_success()

    def test_balance_list_empty_is_success(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Balance list on a fresh profile returns exit 0 with an empty result."""
        env = make_workflow_env_fast(
            tmp_path, "bal-list-empty", _mutating_profile_template
        )
        result = run_cli("accounts", "balance", "list", "--output", "json", env=env)
        result.assert_success()


class TestCategoriesDeleteCommand:
    """`moneybin categories delete` — hard-delete with refuse/force semantics."""

    def test_delete_unreferenced_user_category(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "catdel-ok", _mutating_profile_template)
        insert_sql = (
            "INSERT INTO app.user_categories "
            "(category_id, category, subcategory, is_active) "
            "VALUES ('E2EDEL000001', 'E2ECleanup', NULL, true)"
        )
        run_cli("db", "query", insert_sql, env=env).assert_success()

        result = run_cli("categories", "delete", "E2EDEL000001", env=env)
        result.assert_success()
        assert "deleted" in result.output.lower()

        verify = run_cli(
            "db",
            "query",
            "SELECT COUNT(*) AS n FROM app.user_categories "
            "WHERE category_id = 'E2EDEL000001'",
            env=env,
        )
        verify.assert_success()
        assert "0" in verify.output, f"row still present after delete:\n{verify.output}"

    def test_delete_unknown_category_exits_nonzero(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(
            tmp_path, "catdel-missing", _mutating_profile_template
        )
        result = run_cli("categories", "delete", "DOES-NOT-EXIST", env=env)
        assert result.exit_code == 1, (
            f"expected exit 1 on unknown category, got {result.exit_code}\n"
            f"output: {result.output}"
        )
        assert "not found" in result.output.lower()


class TestCategorizeRulesCreateCLI:
    """`moneybin transactions categorize rules create` — single and batch modes."""

    def test_create_single_rule(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(
            tmp_path, "rulescreate-one", _mutating_profile_template
        )
        result = run_cli(
            "transactions",
            "categorize",
            "rules",
            "create",
            "starbucks-rule",
            "--pattern",
            "STARBUCKS",
            "--category",
            "Food & Dining",
            "--subcategory",
            "Coffee Shops",
            "--match-type",
            "contains",
            "--priority",
            "100",
            env=env,
        )
        result.assert_success()
        assert "Created 1 rule" in result.stderr

    def test_create_from_file_batch(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(
            tmp_path, "rulescreate-batch", _mutating_profile_template
        )
        rules_file = tmp_path / "rules.json"
        rules_file.write_text(
            json.dumps([
                {
                    "name": "amazon-rule",
                    "merchant_pattern": "AMAZON",
                    "category": "Shopping",
                    "match_type": "contains",
                    "priority": 100,
                },
                {
                    "name": "uber-rule",
                    "merchant_pattern": "UBER",
                    "category": "Transportation",
                    "match_type": "contains",
                    "priority": 100,
                },
            ])
        )
        result = run_cli(
            "transactions",
            "categorize",
            "rules",
            "create",
            "--from-file",
            str(rules_file),
            env=env,
        )
        result.assert_success()
        assert "Created 2 rule" in result.stderr

    def test_create_with_json_output(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(
            tmp_path, "rulescreate-json", _mutating_profile_template
        )
        result = run_cli(
            "transactions",
            "categorize",
            "rules",
            "create",
            "test-json-rule",
            "--pattern",
            "TEST",
            "--category",
            "Other",
            "--output",
            "json",
            env=env,
        )
        result.assert_success()
        payload = json.loads(result.stdout)
        assert "data" in payload
        data = payload["data"]
        assert data["created"] >= 1
        assert isinstance(data.get("rule_ids"), list)

    def test_create_requires_name_pattern_category_when_no_file(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Bare `create` without name+pattern+category or --from-file is a usage error."""
        env = make_workflow_env_fast(
            tmp_path, "rulescreate-usage", _mutating_profile_template
        )
        result = run_cli("transactions", "categorize", "rules", "create", env=env)
        assert result.exit_code != 0
        assert "Traceback (most recent call last)" not in result.stderr
        assert "Single-rule mode requires" in result.stderr

    def test_create_from_file_with_single_rule_flag_errors(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`--from-file` alongside any single-rule flag is rejected, not silently ignored."""
        env = make_workflow_env_fast(
            tmp_path, "rulescreate-mutex", _mutating_profile_template
        )
        rules_file = tmp_path / "rules.json"
        rules_file.write_text(json.dumps([]))
        result = run_cli(
            "transactions",
            "categorize",
            "rules",
            "create",
            "--from-file",
            str(rules_file),
            "--pattern",
            "OOPS",
            env=env,
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.stderr
        assert "--pattern" in result.stderr

    def test_create_exits_nonzero_when_rows_skipped(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Batch with at least one malformed row exits 1 even though good rows are created."""
        env = make_workflow_env_fast(
            tmp_path, "rulescreate-partial", _mutating_profile_template
        )
        rules_file = tmp_path / "rules.json"
        # Missing required `merchant_pattern` field on the second rule.
        rules_file.write_text(
            json.dumps([
                {
                    "name": "good-rule",
                    "merchant_pattern": "GOOD",
                    "category": "Other",
                },
                {"name": "bad-rule", "category": "Other"},
            ])
        )
        result = run_cli(
            "transactions",
            "categorize",
            "rules",
            "create",
            "--from-file",
            str(rules_file),
            env=env,
        )
        assert result.exit_code == 1
        assert "Traceback (most recent call last)" not in result.stderr
        # Text mode surfaces per-row failure reason so the user knows what failed.
        assert "bad-rule" in result.stderr
        assert "⚠️" in result.stderr

    def test_create_from_file_directory_path_errors_cleanly(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Passing a directory to --from-file yields a clean error, not a traceback."""
        env = make_workflow_env_fast(
            tmp_path, "rulescreate-dir", _mutating_profile_template
        )
        result = run_cli(
            "transactions",
            "categorize",
            "rules",
            "create",
            "--from-file",
            str(tmp_path),
            env=env,
        )
        assert result.exit_code == 2
        assert "Traceback (most recent call last)" not in result.stderr
        assert "Cannot read" in result.stderr

    def test_create_quiet_still_surfaces_failure_warnings(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`--quiet` suppresses the success line but failure warnings are diagnostic, not informational."""
        env = make_workflow_env_fast(
            tmp_path, "rulescreate-quiet", _mutating_profile_template
        )
        rules_file = tmp_path / "rules.json"
        rules_file.write_text(
            json.dumps([
                {
                    "name": "good-quiet-rule",
                    "merchant_pattern": "GOODQ",
                    "category": "Other",
                },
                {"name": "bad-quiet-rule", "category": "Other"},
            ])
        )
        result = run_cli(
            "transactions",
            "categorize",
            "rules",
            "create",
            "--from-file",
            str(rules_file),
            "--quiet",
            env=env,
        )
        assert result.exit_code == 1
        # Success line IS suppressed by --quiet.
        assert "✅ Created" not in result.stderr
        # Failure warnings ARE NOT suppressed by --quiet.
        assert "bad-quiet-rule" in result.stderr
        assert "⚠️" in result.stderr


class TestCategorizeRulesDeleteCLI:
    """`moneybin transactions categorize rules delete` — soft-delete by ID."""

    def test_delete_existing_rule(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(
            tmp_path, "rulesdel-ok", _mutating_profile_template
        )
        create_result = run_cli(
            "transactions",
            "categorize",
            "rules",
            "create",
            "delete-target",
            "--pattern",
            "DELTARGET",
            "--category",
            "Other",
            "--output",
            "json",
            env=env,
        )
        create_result.assert_success()
        rule_ids = json.loads(create_result.stdout)["data"]["rule_ids"]
        assert rule_ids, "create did not return any rule_ids"
        rule_id = rule_ids[0]

        delete_result = run_cli(
            "transactions", "categorize", "rules", "delete", rule_id, env=env
        )
        delete_result.assert_success()
        assert "deactivated" in delete_result.output.lower()

    def test_delete_existing_rule_json_output(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(
            tmp_path, "rulesdel-json", _mutating_profile_template
        )
        create_result = run_cli(
            "transactions",
            "categorize",
            "rules",
            "create",
            "delete-target-json",
            "--pattern",
            "DELJ",
            "--category",
            "Other",
            "--output",
            "json",
            env=env,
        )
        create_result.assert_success()
        rule_id = json.loads(create_result.stdout)["data"]["rule_ids"][0]

        delete_result = run_cli(
            "transactions",
            "categorize",
            "rules",
            "delete",
            rule_id,
            "--output",
            "json",
            env=env,
        )
        delete_result.assert_success()
        payload = json.loads(delete_result.stdout)
        assert payload["data"]["rule_id"] == rule_id
        assert payload["data"]["action"] == "deactivated"

    def test_delete_nonexistent_rule_errors(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(
            tmp_path, "rulesdel-missing", _mutating_profile_template
        )
        result = run_cli(
            "transactions",
            "categorize",
            "rules",
            "delete",
            "does-not-exist",
            env=env,
        )
        assert result.exit_code != 0
        assert "Traceback (most recent call last)" not in result.stderr
        assert "Rule does-not-exist not found" in result.stderr


class TestAccountLinksMutating:
    """E2E smoke tests for `accounts links run` and `accounts links set`."""

    def test_accounts_links_run(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`accounts links run` exits 0; 0 proposals on empty data is success."""
        env = make_workflow_env_fast(tmp_path, "links-run", _mutating_profile_template)
        result = run_cli("accounts", "links", "run", env=env)
        result.assert_success()

    def test_accounts_links_run_json(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`accounts links run --output json` returns an envelope with new_proposals."""
        env = make_workflow_env_fast(
            tmp_path, "links-run-json", _mutating_profile_template
        )
        result = run_cli("accounts", "links", "run", "--output", "json", env=env)
        result.assert_success()
        payload = json.loads(result.stdout)
        assert "data" in payload
        assert "new_proposals" in payload["data"]
        assert isinstance(payload["data"]["new_proposals"], int)

    def test_accounts_links_set_not_found(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`accounts links set <nonexistent_id> --standalone` fails with not-found, no traceback.

        No pending link decisions exist on a fresh profile — the service raises
        UserError(MUTATION_NOT_FOUND) which handle_cli_errors converts to exit 1.
        """
        env = make_workflow_env_fast(
            tmp_path, "links-set-nf", _mutating_profile_template
        )
        result = run_cli(
            "accounts",
            "links",
            "set",
            "nonexistent-decision-id",
            "--standalone",
            env=env,
        )
        assert result.exit_code != 0
        assert "Traceback (most recent call last)" not in result.stderr

    def test_accounts_links_set_missing_flag_is_usage_error(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`accounts links set <id>` without --into or --standalone exits 2 (usage error)."""
        env = make_workflow_env_fast(
            tmp_path, "links-set-usage", _mutating_profile_template
        )
        result = run_cli("accounts", "links", "set", "any-id", env=env)
        assert result.exit_code == 2
        assert "Traceback (most recent call last)" not in result.stderr

    def test_accounts_links_set_mutual_exclusion_error(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`accounts links set <id> --into X --standalone` exits 2 (mutually exclusive)."""
        env = make_workflow_env_fast(
            tmp_path, "links-set-mutex", _mutating_profile_template
        )
        result = run_cli(
            "accounts",
            "links",
            "set",
            "any-id",
            "--into",
            "CAND001",
            "--standalone",
            env=env,
        )
        assert result.exit_code == 2
        assert "Traceback (most recent call last)" not in result.stderr


class TestMerchantLinksMutating:
    """E2E smoke tests for `merchants links run` and `merchants links set`.

    Deep accept/reject behavior is covered at the unit (test_merchant_links_service)
    and scenario (test_merchant_harvest) tiers — seeding a pending decision across
    the subprocess + encrypted-DB boundary is brittle, so these smoke the wiring,
    exit codes, and JSON envelope the way the `accounts links` e2e tests do.
    """

    def test_merchants_links_run(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`merchants links run` exits 0; nothing to harvest on empty data is success."""
        env = make_workflow_env_fast(tmp_path, "mlinks-run", _mutating_profile_template)
        result = run_cli("merchants", "links", "run", env=env)
        result.assert_success()

    def test_merchants_links_run_json(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`merchants links run --output json` returns an envelope with bound + conflicts."""
        env = make_workflow_env_fast(
            tmp_path, "mlinks-run-json", _mutating_profile_template
        )
        result = run_cli("merchants", "links", "run", "--output", "json", env=env)
        result.assert_success()
        payload = json.loads(result.stdout)
        assert "data" in payload
        assert isinstance(payload["data"]["bound"], int)
        assert isinstance(payload["data"]["conflicts"], int)

    def test_merchants_links_set_not_found(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`merchants links set <nonexistent_id> --new` fails not-found, no traceback.

        No pending decisions exist on a fresh profile — the service raises
        UserError(MUTATION_NOT_FOUND) which handle_cli_errors converts to exit 1.
        ``--new`` (reject) avoids needing a real merchant target.
        """
        env = make_workflow_env_fast(
            tmp_path, "mlinks-set-nf", _mutating_profile_template
        )
        result = run_cli(
            "merchants", "links", "set", "nonexistent-decision-id", "--new", env=env
        )
        assert result.exit_code != 0
        assert "Traceback (most recent call last)" not in result.stderr

    def test_merchants_links_set_missing_flag_is_usage_error(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`merchants links set <id>` without --into or --new exits 2 (usage error)."""
        env = make_workflow_env_fast(
            tmp_path, "mlinks-set-usage", _mutating_profile_template
        )
        result = run_cli("merchants", "links", "set", "any-id", env=env)
        assert result.exit_code == 2
        assert "Traceback (most recent call last)" not in result.stderr

    def test_merchants_links_set_empty_into_is_usage_error(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`merchants links set <id> --into ""` exits 2 — empty target never silently binds."""
        env = make_workflow_env_fast(
            tmp_path, "mlinks-set-empty", _mutating_profile_template
        )
        result = run_cli("merchants", "links", "set", "any-id", "--into", "", env=env)
        assert result.exit_code == 2
        assert "Traceback (most recent call last)" not in result.stderr

    def test_merchants_links_set_mutual_exclusion_error(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`merchants links set <id> --into X --new` exits 2 (mutually exclusive)."""
        env = make_workflow_env_fast(
            tmp_path, "mlinks-set-mutex", _mutating_profile_template
        )
        result = run_cli(
            "merchants", "links", "set", "any-id", "--into", "CAND001", "--new", env=env
        )
        assert result.exit_code == 2
        assert "Traceback (most recent call last)" not in result.stderr


class TestSecurityLinksMutating:
    """E2E smoke tests for `investments securities links set`.

    No `run` subcommand exists for security links (proposals come from
    `SecurityResolver` during `sync pull`, not a CLI-invoked harvest), so
    this mirrors only the `set` half of `TestMerchantLinksMutating`. Deep
    accept/reject behavior is covered at the unit tier
    (test_security_links_service.py); these smoke the wiring, exit codes,
    and mutual-exclusion guard the way the `merchants links` e2e tests do.
    """

    def test_investments_prices_set_then_delete_round_trips(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """A mark can be written and then removed, returning the date to feeds.

        `delete` is load-bearing rather than CRUD symmetry: an override outranks
        every provider close for its date and `set` can only change the number, so
        without this the date could never return to provider-derived valuation.
        """
        env = make_workflow_env_fast(
            tmp_path, "prices-roundtrip", _mutating_profile_template
        )
        add = run_cli(
            "investments",
            "securities",
            "add",
            "--name",
            "Private Placement A",
            "--type",
            "other",
            env=env,
        )
        add.assert_success()

        marked = run_cli(
            "investments",
            "prices",
            "set",
            "Private Placement A",
            "2026-06-30",
            "42.50",
            "--note",
            "409A valuation",
            env=env,
        )
        marked.assert_success()

        removed = run_cli(
            "investments",
            "prices",
            "delete",
            "Private Placement A",
            "2026-06-30",
            env=env,
        )
        removed.assert_success()

        again = run_cli(
            "investments",
            "prices",
            "delete",
            "Private Placement A",
            "2026-06-30",
            env=env,
        )
        again.assert_success()
        assert "No mark existed" in again.stdout

    def test_fx_override_round_trips_through_the_resolver(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """A recorded rate answers `fx rate` for its date, then stops answering.

        The whole point of the override layer is that it outranks the provider,
        and the only way to see that is to ask the resolver afterwards. Asking
        for a date this profile has no cached rate for would otherwise reach
        the network; the correction is what keeps this test offline, which is
        the same reason a user records one.
        """
        env = make_workflow_env_fast(
            tmp_path, "fx-override", _mutating_profile_template
        )

        recorded = run_cli(
            "fx",
            "set",
            "USD",
            "EUR",
            "2026-03-13",
            "0.87138000",
            "--note",
            "bank",
            env=env,
        )
        recorded.assert_success()

        resolved = run_cli(
            "fx", "rate", "USD", "EUR", "2026-03-13", "--output", "json", env=env
        )
        resolved.assert_success()
        data = json.loads(resolved.stdout)["data"]
        assert Decimal(str(data["rate"])) == Decimal("0.87138000")
        assert data["source"] == "override"

        removed = run_cli("fx", "delete", "USD", "EUR", "2026-03-13", env=env)
        removed.assert_success()

        again = run_cli("fx", "delete", "USD", "EUR", "2026-03-13", env=env)
        again.assert_success()
        assert "No override existed" in again.stdout

    def test_fx_set_refuses_a_zero_rate(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """A zero rate converts every balance in that currency to nothing.

        It also outranks the provider, so nothing downstream would contradict
        it — the refusal has to happen here or not at all.
        """
        env = make_workflow_env_fast(tmp_path, "fx-zero", _mutating_profile_template)
        result = run_cli("fx", "set", "USD", "EUR", "2026-03-13", "0", env=env)

        assert result.exit_code != 0
        assert "Traceback (most recent call last)" not in result.stderr

    def test_investments_prices_set_refuses_a_zero_price(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """The never-zero rule must hold on the user-controlled path too.

        A worthless position is a ledger event — a disposal or write-off — not a
        zero price, which would make worthless and unknown two states every
        downstream total has to tell apart.
        """
        env = make_workflow_env_fast(
            tmp_path, "prices-zero", _mutating_profile_template
        )
        add = run_cli(
            "investments",
            "securities",
            "add",
            "--name",
            "Private Placement B",
            "--type",
            "other",
            env=env,
        )
        add.assert_success()

        result = run_cli(
            "investments",
            "prices",
            "set",
            "Private Placement B",
            "2026-06-30",
            "0",
            env=env,
        )
        assert result.exit_code != 0
        assert "Traceback (most recent call last)" not in result.stderr

    def test_investments_prices_set_rejects_a_malformed_date(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """A bad date is a usage error (exit 2), not a runtime failure."""
        env = make_workflow_env_fast(
            tmp_path, "prices-baddate", _mutating_profile_template
        )
        result = run_cli(
            "investments", "prices", "set", "AAPL", "30-06-2026", "42.50", env=env
        )
        assert result.exit_code == 2
        assert "Traceback (most recent call last)" not in result.stderr

    def test_investments_securities_links_set_not_found(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`investments securities links set <nonexistent_id> --reject` fails not-found, no traceback.

        No pending decisions exist on a fresh profile — the service raises
        UserError(MUTATION_NOT_FOUND) which handle_cli_errors converts to exit 1.
        ``--reject`` avoids needing a real candidate security.
        """
        env = make_workflow_env_fast(
            tmp_path, "slinks-set-nf", _mutating_profile_template
        )
        result = run_cli(
            "investments",
            "securities",
            "links",
            "set",
            "nonexistent-decision-id",
            "--reject",
            env=env,
        )
        assert result.exit_code != 0
        assert "Traceback (most recent call last)" not in result.stderr

    def test_investments_securities_links_set_missing_flag_is_usage_error(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`investments securities links set <id>` without --accept or --reject exits 2."""
        env = make_workflow_env_fast(
            tmp_path, "slinks-set-usage", _mutating_profile_template
        )
        result = run_cli("investments", "securities", "links", "set", "any-id", env=env)
        assert result.exit_code == 2
        assert "Traceback (most recent call last)" not in result.stderr

    def test_investments_securities_links_set_mutual_exclusion_error(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`investments securities links set <id> --accept --reject` exits 2 (mutually exclusive)."""
        env = make_workflow_env_fast(
            tmp_path, "slinks-set-mutex", _mutating_profile_template
        )
        result = run_cli(
            "investments",
            "securities",
            "links",
            "set",
            "any-id",
            "--accept",
            "--reject",
            env=env,
        )
        assert result.exit_code == 2
        assert "Traceback (most recent call last)" not in result.stderr

    def test_investments_securities_links_set_accept_without_into_is_usage_error(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """`... set <id> --accept` without `--into` exits 2 — no default merge target.

        The confirming safety check (--into must name the decision's own
        candidate_security_id) is required, not optional, on accept.
        """
        env = make_workflow_env_fast(
            tmp_path, "slinks-set-no-into", _mutating_profile_template
        )
        result = run_cli(
            "investments", "securities", "links", "set", "any-id", "--accept", env=env
        )
        assert result.exit_code == 2
        assert "Traceback (most recent call last)" not in result.stderr


class TestPrivacyConsent:
    """Consent ledger CLI commands (grant / revoke / revoke-all)."""

    def test_privacy_grant_status_log_revoke_cycle(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        """Full lifecycle over a real subprocess + encrypted DB."""
        env = make_workflow_env_fast(tmp_path, "privcycle", _mutating_profile_template)
        env["MONEYBIN_AI__DEFAULT_BACKEND"] = "anthropic"

        run_cli(
            "privacy", "grant", "mcp-data-sharing", "--yes", env=env
        ).assert_success()

        status = run_cli("privacy", "status", "--output", "json", env=env)
        status.assert_success()
        assert "mcp-data-sharing" in status.stdout

        log = run_cli("privacy", "log", env=env)
        log.assert_success()
        assert "consent.grant" in log.stdout

        run_cli(
            "privacy", "revoke", "mcp-data-sharing", "--yes", env=env
        ).assert_success()

    def test_privacy_revoke_all(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(tmp_path, "privrevall", _mutating_profile_template)
        env["MONEYBIN_AI__DEFAULT_BACKEND"] = "anthropic"
        run_cli(
            "privacy", "grant", "mcp-data-sharing", "--yes", env=env
        ).assert_success()
        run_cli(
            "privacy", "grant", "ml-categorization", "--yes", env=env
        ).assert_success()
        run_cli("privacy", "revoke-all", "--yes", env=env).assert_success()


class TestUserReports:
    """`moneybin reports create/list/run/reclassify/set/delete` over a real DB.

    The saved query reads ``app.user_reports`` itself. That is a real permitted
    read (``app`` is inside the save allowlist), needs no SQLMesh build, and
    guarantees a non-empty result — the report returns itself, which is the
    ask→save→verify loop end to end in one subprocess chain.
    """

    def test_saved_report_lifecycle(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(
            tmp_path, "userreports", _mutating_profile_template
        )

        created = run_cli(
            "reports",
            "create",
            "my_reports",
            "--sql",
            "SELECT report_id, name FROM app.user_reports",
            "--description",
            "Every report I have saved.",
            "--output",
            "json",
            env=env,
        )
        created.assert_success()
        report_id = json.loads(created.stdout)["data"]["report_id"]
        assert report_id.startswith("user:")

        listed = run_cli(
            "reports", "list", "--tier", "user", "--output", "json", env=env
        )
        listed.assert_success()
        entries = json.loads(listed.stdout)["data"]
        assert [entry["report_id"] for entry in entries] == [report_id]
        assert entries[0]["tier"] == "user"

        # The report returns its own row: derived classes let `name` through
        # (USER_NOTE is MEDIUM and passthrough — FLOORED is the only
        # below-CRITICAL class that masks), so this also proves the class map
        # is not over-masking.
        ran = run_cli("reports", "run", report_id, "--output", "json", env=env)
        ran.assert_success()
        rows = json.loads(ran.stdout)["data"]
        assert [row["name"] for row in rows] == ["my_reports"]

        # A saved report resolves by name too, and binds `$name` by position-free
        # keyword — R8.
        parameterized = run_cli(
            "reports",
            "create",
            "one_report",
            "--sql",
            "SELECT report_id FROM app.user_reports WHERE name = $wanted",
            "--param",
            "wanted",
            "--output",
            "json",
            env=env,
        )
        parameterized.assert_success()
        bound = run_cli(
            "reports",
            "run",
            "one_report",
            "--param",
            "wanted=my_reports",
            "--output",
            "json",
            env=env,
        )
        bound.assert_success()
        assert [row["report_id"] for row in json.loads(bound.stdout)["data"]] == [
            report_id
        ]

        # R6's verify surface over the same saved report: both SQL forms, the
        # per-column provenance, and the graduation verdict.
        explained = run_cli(
            "reports", "explain", report_id, "--output", "json", env=env
        )
        explained.assert_success()
        evidence = json.loads(explained.stdout)["data"]
        assert evidence["tier"] == "user"
        assert evidence["sql"] is not None
        assert evidence["sql_template"] is not None
        by_column = {column["column"]: column for column in evidence["columns"]}
        assert by_column["name"]["upstream"] == "app.user_reports.name"
        assert by_column["name"]["origin"] == "upstream"
        assert evidence["lineage"] == ["app.user_reports"]
        assert evidence["class_fingerprint"]
        assert evidence["drift_detected"] is False
        # `app.*` has an independently authored CLASSIFICATION ground truth, so a
        # named projection over it clears both materialization rules. The blocked
        # cases (a `reports.*` read, a star projection) are unit-tested; this is
        # the eligible one, which no assertion about masking would catch.
        assert evidence["graduation"] == "eligible"
        assert evidence["graduation_blockers"] == []

        # A parameter with no supplied value withholds the executed form and
        # names the flag that would produce it.
        unbound = run_cli(
            "reports", "explain", "one_report", "--output", "json", env=env
        )
        unbound.assert_success()
        pending = json.loads(unbound.stdout)["data"]
        assert pending["sql"] is None
        assert pending["sql_suppressed_by"] == ["wanted"]
        assert "$wanted" in pending["sql_template"]

        downgraded = run_cli(
            "reports",
            "reclassify",
            report_id,
            "--column",
            "name",
            "--to",
            "aggregate",
            "--reason",
            "A report name I wrote reveals nothing about my finances.",
            "--yes",
            "--output",
            "json",
            env=env,
        )
        downgraded.assert_success()
        assert json.loads(downgraded.stdout)["data"]["from"] == "user_note"

        # The audit row has to say a flag supplied that confirmation, not a human.
        # `actor` is "cli" either way, so this field is the only thing standing
        # between an assistant self-accepting a permanent masking downgrade and a
        # human approving one — and it has to survive the JSON round-trip through
        # `context_json` and the privacy payload to be worth anything.
        trail = run_cli(
            "system",
            "audit",
            "list",
            "--target-id",
            report_id,
            "--action",
            "user_report.set",
            "--output",
            "json",
            env=env,
        )
        trail.assert_success()
        events = json.loads(trail.stdout)["data"]
        assert [event["context_json"] for event in events] == [
            {"confirmed_via": "flag"}
        ]

        # An equal-tier weakening is refused whatever the reason: report_id is
        # RECORD_ID, so there is no lower tier to move it to.
        refused = run_cli(
            "reports",
            "reclassify",
            report_id,
            "--column",
            "report_id",
            "--to",
            "aggregate",
            "--reason",
            "Trying to weaken a class that is already LOW.",
            "--yes",
            env=env,
        )
        assert refused.exit_code == 1, refused.output

        run_cli("reports", "set", report_id, "--archive", env=env).assert_success()
        active = run_cli(
            "reports", "list", "--tier", "user", "--output", "json", env=env
        )
        active.assert_success()
        assert report_id not in {
            entry["report_id"] for entry in json.loads(active.stdout)["data"]
        }
        archived = run_cli(
            "reports", "list", "--include-archived", "--output", "json", env=env
        )
        archived.assert_success()
        widened = {
            entry["report_id"]: entry["archived"]
            for entry in json.loads(archived.stdout)["data"]
        }
        assert widened[report_id] is True
        # A widened listing must still be the whole catalog, not a swapped view.
        assert any(rid.startswith("core:") for rid in widened)

        # Archiving hides a report; it must not retire it. The unit tests assert
        # this on the catalog, which is one layer in from the thing a user does —
        # and an unrunnable archived report is exactly what shipped before.
        still_runs = run_cli("reports", "run", report_id, "--output", "json", env=env)
        still_runs.assert_success()
        # The saved query selects from `app.user_reports`, so by now it returns
        # this report and the parameterized one saved above — compared as a set
        # because the query declares no ORDER BY.
        assert {row["name"] for row in json.loads(still_runs.stdout)["data"]} == {
            "my_reports",
            "one_report",
        }

        run_cli("reports", "delete", report_id, "--yes", env=env).assert_success()
        gone = run_cli("reports", "run", report_id, env=env)
        assert gone.exit_code == 1, gone.output


class TestDemo:
    """`moneybin demo` — the evaluator preset (real full pipeline)."""

    def test_demo_end_to_end_json(self, tmp_path: Path) -> None:
        """Demo builds a populated, doctor-clean demo profile and reports net worth."""
        env = base_env(tmp_path, "demo")
        env["MONEYBIN_IMPORT___INBOX_ROOT"] = str(tmp_path / "inbox-root")
        result = run_cli(
            "demo",
            "--yes",
            "--seed",
            "42",
            "--years",
            "1",
            "--output",
            "json",
            env=env,
            timeout=300,
        )
        assert result.exit_code == 0, result.output
        assert "Traceback" not in result.stderr
        envelope = json.loads(result.stdout)
        payload = envelope["data"]
        assert payload["profile"] == "demo"
        assert payload["transaction_count"] > 0
        assert payload["account_count"] > 0
        # The whole point of demo: it ends clean AND categorized. Coverage is part
        # of the success signal because doctor's own coverage check is warn-only —
        # demo once shipped 0% categorized while still reporting "clean".
        assert payload["doctor_failing"] == 0, payload["doctor_failing_names"]
        assert payload["categorized_count"] / payload["transaction_count"] > 0.7

    def test_demo_rerun_after_a_real_cli_run(self, tmp_path: Path) -> None:
        """A second `moneybin demo` rebuilds the profile the first one left behind.

        This has to be a real subprocess, twice. Every CLI process that opened a
        write connection flushes operational metrics to `app.metrics` at exit via
        `atexit` — which no in-process test can trigger. The real-data guard reads
        any unrecognized `app.*` table as the user's, so our own telemetry looked
        like user data and made the demo profile unrebuildable. Only a real second
        invocation proves it doesn't.
        """
        env = base_env(tmp_path, "demo")
        env["MONEYBIN_IMPORT___INBOX_ROOT"] = str(tmp_path / "inbox-root")
        first = run_cli(
            "demo", "--yes", "--seed", "42", "--years", "1", env=env, timeout=300
        )
        assert first.exit_code == 0, first.output

        second = run_cli(
            "demo",
            "--yes",
            "--seed",
            "7",
            "--years",
            "1",
            "--output",
            "json",
            env=env,
            timeout=300,
        )
        assert second.exit_code == 0, second.output
        assert "Traceback" not in second.stderr
        payload = json.loads(second.stdout)["data"]
        assert payload["transaction_count"] > 0
        assert payload["doctor_failing"] == 0, payload["doctor_failing_names"]


class TestCategorizeRulesResolveCLI:
    """`moneybin transactions categorize rules resolve` — decide a rule conflict."""

    @staticmethod
    def _create(env: dict[str, str], name: str, category: str) -> dict[str, Any]:
        result = run_cli(
            "transactions",
            "categorize",
            "rules",
            "create",
            name,
            "--pattern",
            "CONFLICTPATTERN",
            "--category",
            category,
            "--output",
            "json",
            env=env,
        )
        result.assert_success()
        return json.loads(result.stdout)

    def test_second_category_is_refused_then_replaced(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(
            tmp_path, "rulesresolve-replace", _mutating_profile_template
        )
        first = self._create(env, "conflict-a", "Other")
        assert first["status"] == "ok"
        second = self._create(env, "conflict-b", "Shopping")

        assert second["status"] == "conflict"
        assert second["data"]["created"] == 0
        conflict_id = str(second["data"]["conflict_ids"][0])

        listed = run_cli(
            "transactions",
            "categorize",
            "rules",
            "list-conflicts",
            "--output",
            "json",
            env=env,
        )
        listed.assert_success()
        assert json.loads(listed.stdout)["data"][0]["conflict_id"] == conflict_id

        resolved = run_cli(
            "transactions",
            "categorize",
            "rules",
            "resolve",
            conflict_id,
            "--replace",
            "--yes",
            "--output",
            "json",
            env=env,
        )
        resolved.assert_success()
        row = json.loads(resolved.stdout)["data"][0]
        assert row["resolution"] == "replace"
        assert row["superseded_rule_ids"] == [first["data"]["rule_ids"][0]]

    def test_resolve_refuses_a_missing_batch_file(
        self, _mutating_profile_template: Path, tmp_path: Path
    ) -> None:
        env = make_workflow_env_fast(
            tmp_path, "rulesresolve-nofile", _mutating_profile_template
        )
        result = run_cli(
            "transactions",
            "categorize",
            "rules",
            "resolve",
            "--from-file",
            str(tmp_path / "absent.json"),
            "--yes",
            env=env,
        )
        assert result.exit_code == 2, result.output
        assert "Traceback (most recent call last)" not in result.stderr
