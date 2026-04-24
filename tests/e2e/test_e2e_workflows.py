# ruff: noqa: S101
"""E2E workflow tests — multi-step user flows run as subprocesses."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.conftest import FIXTURES_DIR, make_workflow_env, run_cli

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

        # Verify core tables have data
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

        # Verify core tables have data
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

        # Verify core tables have data
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
        result = run_cli("db", "query", "SELECT 1 AS ok", env=env)
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
        result = run_cli("db", "query", "SELECT 1 AS ok", env=env)
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
