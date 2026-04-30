# ruff: noqa: S101
"""E2E read-only tests — commands that don't mutate state.

Covers three groups:
- No-DB commands: profile list/show, import preview, logs, mcp config, db ps
- Stub commands: sync/track/export stubs that print "not implemented"
- DB commands: read-only queries against the shared e2e_profile database
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from tests.e2e.conftest import FIXTURES_DIR, run_cli

_has_duckdb_cli = shutil.which("duckdb") is not None

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# No-DB commands — execute real logic without get_database()
# ---------------------------------------------------------------------------


class TestNoDBCommands:
    """Commands that run without an initialized database."""

    def test_profile_list(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("profile", "list", env=e2e_env)
        result.assert_success()

    def test_profile_show(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("profile", "show", env=e2e_env)
        # exit_code may be 0 or 1 depending on whether a profile is set
        assert "Traceback" not in result.stderr

    def test_import_list_formats(self) -> None:
        result = run_cli("import", "formats", "list")
        result.assert_success()

    def test_import_preview(self) -> None:
        fixture = FIXTURES_DIR / "tabular" / "standard.csv"
        result = run_cli("import", "preview", str(fixture))
        result.assert_success()

    def test_logs_path(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("logs", "--print-path", env=e2e_env)
        result.assert_success()

    def test_logs_tail(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("logs", "cli", "--lines", "5", env=e2e_env)
        # May exit 0 or 1 if no log files exist yet — no crash is the bar
        assert "Traceback" not in result.stderr

    def test_logs_clean_dry_run(self, e2e_env: dict[str, str]) -> None:
        result = run_cli(
            "logs", "--prune", "--older-than", "30d", "--dry-run", env=e2e_env
        )
        result.assert_success()

    def test_logs_bare_invocation_skips_wizard(self, tmp_path: Path) -> None:
        """Bare ``moneybin logs`` surfaces a usage error, not the wizard.

        Pointed at an empty MONEYBIN_HOME with no MONEYBIN_PROFILE: a normal
        command would invoke ``ensure_default_profile()`` and prompt for a
        profile name on stdin. The leaf's missing-arg check must fire first.
        """
        env = {"MONEYBIN_HOME": str(tmp_path)}
        result = run_cli("logs", env=env)
        assert result.exit_code == 2, result.output
        assert "Missing argument" in result.stderr
        assert "Welcome to MoneyBin" not in result.output

    def test_mcp_list_prompts(self) -> None:
        result = run_cli("mcp", "list-prompts")
        result.assert_success()

    def test_mcp_config_show(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("mcp", "config", env=e2e_env)
        result.assert_success()

    def test_mcp_config_generate(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("mcp", "config", "generate", env=e2e_env)
        result.assert_success()

    def test_db_ps(self) -> None:
        result = run_cli("db", "ps")
        result.assert_success()


# ---------------------------------------------------------------------------
# Stub commands — reserved CLI namespace, not yet implemented
# ---------------------------------------------------------------------------


class TestStubCommands:
    """Stubs should print a message and exit 0, not crash."""

    @pytest.mark.parametrize(
        "cmd",
        [
            ["sync", "login"],
            ["sync", "logout"],
            ["sync", "connect"],
            ["sync", "disconnect"],
            ["sync", "pull"],
            ["sync", "status"],
            ["sync", "key", "rotate"],
            ["sync", "schedule", "set"],
            ["sync", "schedule", "show"],
            ["sync", "schedule", "remove"],
            ["track", "balance", "show"],
            ["track", "networth", "show"],
            ["track", "budget", "show"],
            ["track", "recurring", "show"],
            ["track", "investments", "show"],
            ["export", "run"],
        ],
        ids=lambda c: " ".join(c),
    )
    def test_stub_exits_cleanly(self, cmd: list[str]) -> None:
        result = run_cli(*cmd)
        result.assert_success()
        assert "not yet implemented" in result.output.lower()


# ---------------------------------------------------------------------------
# DB commands — read-only queries on the shared e2e_profile database
# ---------------------------------------------------------------------------


class TestDBReadOnlyCommands:
    """Commands that query the database but don't modify it."""

    # ── db ──────────────────────────────────────────────────────────────

    def test_db_info(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "info", env=e2e_profile)
        result.assert_success()

    @pytest.mark.skipif(not _has_duckdb_cli, reason="DuckDB CLI not installed")
    def test_db_query(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "query", "SELECT 1 AS ok", env=e2e_profile)
        result.assert_success()

    def test_db_key(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "key", "show", env=e2e_profile)
        result.assert_success()

    def test_db_migrate_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "migrate", "status", env=e2e_profile)
        result.assert_success()

    # ── transform ───────────────────────────────────────────────────────

    def test_transform_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transform", "status", env=e2e_profile)
        result.assert_success()

    def test_transform_validate(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transform", "validate", env=e2e_profile)
        result.assert_success()

    def test_transform_plan(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transform", "plan", env=e2e_profile)
        result.assert_success()

    # ── import ──────────────────────────────────────────────────────────

    def test_import_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("import", "status", env=e2e_profile)
        result.assert_success()

    def test_import_history(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("import", "history", env=e2e_profile)
        result.assert_success()

    def test_import_show_format(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("import", "formats", "show", "chase_credit", env=e2e_profile)
        # May exit 1 if format not found — no crash is the bar
        assert "Traceback" not in result.stderr

    # ── categorize ──────────────────────────────────────────────────────

    def test_categorize_summary(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("categorize", "summary", env=e2e_profile)
        result.assert_success()

    def test_categorize_list_rules(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("categorize", "list-rules", env=e2e_profile)
        result.assert_success()

    # ── matches ─────────────────────────────────────────────────────────

    def test_matches_history(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("matches", "history", env=e2e_profile)
        result.assert_success()

    # ── mcp ─────────────────────────────────────────────────────────────

    def test_mcp_list_tools(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("mcp", "list-tools", env=e2e_profile)
        result.assert_success()

    # ── stats ───────────────────────────────────────────────────────────

    def test_stats_show(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("stats", env=e2e_profile)
        result.assert_success()
