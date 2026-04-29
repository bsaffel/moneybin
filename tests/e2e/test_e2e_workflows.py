# ruff: noqa: S101
"""E2E workflow tests — multi-step user flows run as subprocesses."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from tests.e2e.conftest import FIXTURES_DIR, make_workflow_env, run_cli

_has_duckdb_cli = shutil.which("duckdb") is not None

pytestmark = pytest.mark.e2e


class TestSyntheticPipeline:
    """Workflow 1: profile create → db init → synthetic generate → transform → query."""

    def test_synthetic_generate_and_transform(self, e2e_home: Path) -> None:
        env = make_workflow_env(e2e_home, "wf-synthetic")

        # Generate synthetic data (skip transform — we'll run it separately)
        result = run_cli(
            "synthetic",
            "generate",
            "--persona",
            "basic",
            "--profile",
            "wf-synthetic",
            "--skip-transform",
            "--seed",
            "42",
            env=env,
            timeout=120,
        )
        result.assert_success()

        # Run transforms
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

        # Verify core tables have data (requires DuckDB CLI)
        if _has_duckdb_cli:
            result = run_cli(
                "db",
                "query",
                "SELECT COUNT(*) AS n FROM core.fct_transactions",
                "--format",
                "csv",
                env=env,
            )
            result.assert_success()
            count = int(result.stdout.strip().split("\n")[-1].strip())
            assert count > 0, f"Expected rows in core.fct_transactions, got {count}"


class TestCSVImportPipeline:
    """Workflow 2: profile create → db init → import CSV → transform → query."""

    def test_csv_import_and_transform(self, e2e_home: Path) -> None:
        env = make_workflow_env(e2e_home, "wf-csv")

        fixture = FIXTURES_DIR / "tabular" / "standard.csv"

        # Import CSV
        result = run_cli(
            "import",
            "file",
            str(fixture),
            "--account-id",
            "e2e-test-acct",
            "--skip-transform",
            env=env,
        )
        result.assert_success()

        # Run transforms
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

        # Verify core tables have data (requires DuckDB CLI)
        if _has_duckdb_cli:
            result = run_cli(
                "db",
                "query",
                "SELECT COUNT(*) AS n FROM core.fct_transactions",
                "--format",
                "csv",
                env=env,
            )
            result.assert_success()
            count = int(result.stdout.strip().split("\n")[-1].strip())
            assert count > 0, f"Expected rows after CSV import, got {count}"


class TestOFXImportPipeline:
    """Workflow 3: profile create → db init → import OFX → transform → query."""

    def test_ofx_import_and_transform(self, e2e_home: Path) -> None:
        env = make_workflow_env(e2e_home, "wf-ofx")

        fixture = FIXTURES_DIR / "sample_statement.qfx"

        # Import OFX
        result = run_cli(
            "import",
            "file",
            str(fixture),
            "--skip-transform",
            env=env,
        )
        result.assert_success()

        # Run transforms
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

        # Verify core tables have data (requires DuckDB CLI)
        if _has_duckdb_cli:
            result = run_cli(
                "db",
                "query",
                "SELECT COUNT(*) AS n FROM core.fct_transactions",
                "--format",
                "csv",
                env=env,
            )
            result.assert_success()
            count = int(result.stdout.strip().split("\n")[-1].strip())
            assert count > 0, f"Expected rows after OFX import, got {count}"


class TestLockUnlockCycle:
    """Workflow 4: lock exits cleanly, unlock fails gracefully without salt.

    The full passphrase round-trip (init --passphrase → lock → unlock →
    verify) is tested in ``tests/integration/test_integration_existing.py::
    TestPassphraseRoundTrip``.  E2E subprocess tests use a null keyring
    backend, so the passphrase salt is never persisted and unlock cannot
    succeed — but it must fail gracefully.
    """

    def test_lock_unlock_graceful(self, e2e_home: Path) -> None:
        from tests.e2e.conftest import TEST_PASSPHRASE

        env = make_workflow_env(e2e_home, "wf-lock")

        # Verify DB works before locking
        result = run_cli("db", "info", env=env)
        result.assert_success()

        # Lock — clears key from keychain (no-op with null keyring)
        result = run_cli("db", "lock", env=env)
        result.assert_success()

        # Unlock — fails because no passphrase salt in null keyring
        result = run_cli(
            "db",
            "unlock",
            env=env,
            input_text=f"{TEST_PASSPHRASE}\n",
        )
        assert result.exit_code == 1
        assert "Traceback (most recent call last)" not in result.output
        assert "passphrase" in result.stderr.lower()

        # DB still works via env var key (lock only clears keychain)
        result = run_cli("db", "info", env=env)
        result.assert_success()


class TestCategorizationPipeline:
    """Workflow 5: import → transform → seed categories → apply rules → stats."""

    def test_categorize_after_import(self, e2e_home: Path) -> None:
        env = make_workflow_env(e2e_home, "wf-categorize")

        fixture = FIXTURES_DIR / "tabular" / "standard.csv"

        # Import
        result = run_cli(
            "import",
            "file",
            str(fixture),
            "--account-id",
            "e2e-cat-acct",
            "--skip-transform",
            env=env,
        )
        result.assert_success()

        # Transform
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

        # Seed categories
        result = run_cli("categorize", "seed", env=env)
        result.assert_success()

        # Apply rules
        result = run_cli("categorize", "apply-rules", env=env)
        result.assert_success()

        # Stats should work
        result = run_cli("categorize", "stats", env=env)
        result.assert_success()


class TestAutoRulePipeline:
    """Workflow 6: import -> transform -> seed proposal -> auto-confirm -> verify rule promoted.

    bulk_categorize is MCP-only and has no CLI surface, so we seed a proposal
    directly via db query. The user-facing surface this test exercises is:
    import + transform + auto-review + auto-confirm. Promotion is verified by
    inspecting both the active categorization_rules table (created_by='auto_rule'
    after approval) and a re-import — the second import should not fail and the
    rule remains active for future categorization runs.
    """

    def test_import_then_promote_proposal(self, e2e_home: Path) -> None:
        if not _has_duckdb_cli:
            pytest.skip("DuckDB CLI required to verify rule promotion")

        env = make_workflow_env(e2e_home, "wf-autorule")
        fixture = FIXTURES_DIR / "tabular" / "standard.csv"

        # Import + transform so the full app/core schema is materialized.
        result = run_cli(
            "import",
            "file",
            str(fixture),
            "--account-id",
            "wf-autorule-acct",
            "--skip-transform",
            env=env,
        )
        result.assert_success()
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

        # Seed categories so app schema has reference data.
        result = run_cli("categorize", "seed", env=env)
        result.assert_success()

        # Seed a pending proposal directly — bulk_categorize is MCP-only, so
        # we drive the auto-* CLI surface against a known-good app row.
        insert_sql = (
            "INSERT INTO app.proposed_rules "
            "(proposed_rule_id, merchant_pattern, match_type, category, "
            "subcategory, status, trigger_count, source, sample_txn_ids) "
            "VALUES ('wfauto00001', 'COFFEE SHOP', 'contains', 'Food & Dining', "
            "'Coffee', 'pending', 1, 'pattern_detection', [])"
        )
        result = run_cli("db", "query", insert_sql, env=env)
        result.assert_success()

        # auto-review surfaces the seeded proposal
        result = run_cli("categorize", "auto-review", env=env)
        result.assert_success()
        assert "wfauto00001" in result.output

        # auto-confirm promotes the proposal to an active rule
        result = run_cli("categorize", "auto-confirm", "--approve-all", env=env)
        result.assert_success()
        assert "Approved 1" in result.output, (
            f"auto-confirm did not approve the proposal: {result.output}"
        )

        # Verify a rule with created_by='auto_rule' now exists in
        # app.categorization_rules — this is what future imports/runs will use
        # to auto-categorize matching transactions.
        result = run_cli(
            "db",
            "query",
            "SELECT COUNT(*) AS n FROM app.categorization_rules "
            "WHERE created_by = 'auto_rule' AND is_active = true",
            "--format",
            "csv",
            env=env,
        )
        result.assert_success()
        rule_count = int(result.stdout.strip().split("\n")[-1].strip())
        assert rule_count >= 1, (
            f"Expected >=1 active auto_rule, got {rule_count}.\n{result.output}"
        )

        # auto-stats reflects the promotion
        result = run_cli("categorize", "auto-stats", env=env)
        result.assert_success()
        assert "Active auto-rules" in result.output

        # Approval back-fills the matching tabular transaction with
        # categorized_by='auto_rule'. This guards against a regression where
        # tabular descriptions land NULL in core.fct_transactions and the
        # back-fill SELECT (`WHERE t.description IS NOT NULL`) silently
        # returns zero rows.
        result = run_cli(
            "db",
            "query",
            "SELECT COUNT(*) AS n FROM app.transaction_categories "
            "WHERE categorized_by = 'auto_rule'",
            "--format",
            "csv",
            env=env,
        )
        result.assert_success()
        backfilled = int(result.stdout.strip().split("\n")[-1].strip())
        assert backfilled >= 1, (
            f"Expected >=1 transaction back-filled by auto_rule, got {backfilled}.\n"
            f"{result.output}"
        )

        # Re-running apply-rules after promotion must succeed and not crash —
        # this exercises the path future imports will hit.
        result = run_cli("categorize", "apply-rules", env=env)
        result.assert_success()
