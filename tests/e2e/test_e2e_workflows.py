# ruff: noqa: S101
"""E2E workflow tests — multi-step user flows run as subprocesses."""

from __future__ import annotations

import json
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
                "--output",
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
                "--output",
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
                "--output",
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

        # Summary should work
        result = run_cli("categorize", "summary", env=env)
        result.assert_success()


class TestAutoRulePipeline:
    """Workflow 6: import → transform → categorize bulk → auto-confirm → verify rule promoted.

    This test drives the full auto-rule pipeline through the CLI:
    import → transform → categorize bulk → auto-review → auto-confirm → re-apply.

    Promotion is verified by inspecting both the active categorization_rules table
    (created_by='auto_rule' after approval) and re-running apply-rules — the rule
    remains active for future categorization runs.
    """

    def test_import_then_promote_proposal(self, e2e_home: Path) -> None:
        if not _has_duckdb_cli:
            pytest.skip("DuckDB CLI required to verify rule promotion")

        env = make_workflow_env(e2e_home, "wf-autorule")
        fixture = FIXTURES_DIR / "tabular" / "standard.csv"

        # Import the fixture twice under different account IDs so the same
        # merchant description appears in two separate transactions. The first
        # import's COFFEE SHOP row will be categorized via `categorize bulk`;
        # the second import's COFFEE SHOP row will remain uncategorized and be
        # picked up by the auto-rule back-fill on approval.
        result = run_cli(
            "import",
            "file",
            str(fixture),
            "--account-id",
            "wf-autorule-acct-a",
            "--skip-transform",
            env=env,
        )
        result.assert_success()
        result = run_cli(
            "import",
            "file",
            str(fixture),
            "--account-id",
            "wf-autorule-acct-b",
            "--skip-transform",
            env=env,
        )
        result.assert_success()
        result = run_cli("transform", "apply", env=env, timeout=180)
        result.assert_success()

        # Seed categories so the app schema has reference data.
        result = run_cli("categorize", "seed", env=env)
        result.assert_success()

        # Find the COFFEE SHOP transaction ID for account-a only — categorizing
        # just this one leaves account-b's COFFEE SHOP row uncategorized for
        # the back-fill step to exercise after approval.
        result = run_cli(
            "db",
            "query",
            "SELECT transaction_id FROM core.fct_transactions "
            "WHERE description ILIKE '%COFFEE SHOP%' AND account_id = 'wf-autorule-acct-a' "
            "LIMIT 1",
            "--output",
            "csv",
            env=env,
        )
        result.assert_success()
        lines = [ln.strip() for ln in result.stdout.strip().splitlines() if ln.strip()]
        # lines[0] is the CSV header; lines[1] is the first data row.
        assert len(lines) >= 2, (
            f"Expected at least one COFFEE SHOP transaction for acct-a, got: {result.stdout!r}"
        )
        txn_id = lines[1]

        # Write a JSON bulk-categorization payload to the workflow tmp dir.
        json_path = e2e_home / "wf-autorule-bulk.json"
        json_path.write_text(
            json.dumps([
                {
                    "transaction_id": txn_id,
                    "category": "Food & Dining",
                    "subcategory": "Coffee",
                }
            ]),
            encoding="utf-8",
        )

        # bulk categorize — this records a user categorization and triggers the
        # auto-rule pipeline to create a pending proposal (threshold default = 1).
        result = run_cli("categorize", "bulk", "--input", str(json_path), env=env)
        result.assert_success()

        # auto-review surfaces the seeded proposal
        result = run_cli("categorize", "auto", "review", env=env)
        result.assert_success()
        assert "COFFEE SHOP" in result.output, (
            f"Expected COFFEE SHOP pattern in auto-review output: {result.output}"
        )

        # auto-confirm promotes the proposal to an active rule
        result = run_cli("categorize", "auto", "confirm", "--approve-all", env=env)
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
            "--output",
            "csv",
            env=env,
        )
        result.assert_success()
        rule_count = int(result.stdout.strip().split("\n")[-1].strip())
        assert rule_count >= 1, (
            f"Expected >=1 active auto_rule, got {rule_count}.\n{result.output}"
        )

        # auto-stats reflects the promotion
        result = run_cli("categorize", "auto", "stats", env=env)
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
            "--output",
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
