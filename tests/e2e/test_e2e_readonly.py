# ruff: noqa: S101
"""E2E read-only tests — commands that don't mutate state.

Covers three groups:
- No-DB commands: profile list/show, import preview, logs, mcp config, db ps
- Stub commands: sync/track/export stubs that print "not implemented"
- DB commands: read-only queries against the shared e2e_profile database
"""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from tests.e2e.conftest import FIXTURES_DIR, run_cli

pytestmark = pytest.mark.e2e

# In-process runner for tests that exercise pure CLI argument parsing
# (--help output, stub messages). Subprocess invocation is reserved for
# tests that need real boot/wiring coverage — DB-touching read commands
# and env-dependent NoDB commands.
_runner = CliRunner()


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

    def test_mcp_install_print(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("mcp", "install", "--print", env=e2e_env)
        result.assert_success()

    def test_sync_status_unreachable_server_fails_cleanly(
        self, e2e_env: dict[str, str]
    ) -> None:
        """`moneybin sync status` with an unreachable server must exit cleanly.

        Exit non-zero without a Python traceback — handle_cli_errors should
        classify the connection failure into a user-facing message.

        Covers boot/wiring for the live sync commands removed from
        TestStubCommands: catches import-time regressions in sync.py,
        Typer subcommand registration, and SyncClient construction even
        though the actual HTTP request fails.
        """
        env = {
            **e2e_env,
            # 127.0.0.1:1 is reserved and refuses connections fast.
            "MONEYBIN_SYNC__SERVER_URL": "http://127.0.0.1:1",
        }
        result = run_cli("sync", "status", env=env, timeout=15)
        assert result.exit_code != 0
        assert "Traceback" not in result.stderr

    def test_db_ps(self) -> None:
        result = run_cli("db", "ps")
        result.assert_success()


# ---------------------------------------------------------------------------
# Stub commands — reserved CLI namespace, not yet implemented
# ---------------------------------------------------------------------------


class TestStubCommands:
    """Stubs should print a message and exit 0, not crash.

    Run in-process via CliRunner: stub bodies are pure log + return, with
    no env, DB, or filesystem access — subprocess isolation adds cost
    without coverage. Subprocess-level boot is exercised by the smoke in
    ``test_e2e_help.py`` and the mutating/workflow tiers.
    """

    @pytest.mark.parametrize(
        "cmd",
        [
            ["sync", "key", "rotate"],
            ["sync", "schedule", "set"],
            ["sync", "schedule", "show"],
            ["sync", "schedule", "remove"],
            ["accounts", "investments", "show"],
            ["reports", "budget"],
            ["reports", "health"],
            ["budget", "set", "Food", "500"],
            ["budget", "delete", "Food"],
            ["tax", "w2", "2024"],
            ["tax", "deductions", "2024"],
            ["export", "run"],
        ],
        ids=lambda c: " ".join(c),
    )
    def test_stub_exits_cleanly(self, cmd: list[str]) -> None:
        result = _runner.invoke(app, cmd)
        assert result.exit_code == 0, (
            f"stub {cmd} exited {result.exit_code}\noutput: {result.output}"
        )
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

    # ── transactions categorize ──────────────────────────────────────────

    def test_categorize_stats(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transactions", "categorize", "stats", env=e2e_profile)
        result.assert_success()

    def test_categorize_rules_list(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transactions", "categorize", "rules", "list", env=e2e_profile)
        result.assert_success()

    # ── matches ─────────────────────────────────────────────────────────

    def test_matches_history(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transactions", "matches", "history", env=e2e_profile)
        result.assert_success()

    # ── system ──────────────────────────────────────────────────────────

    def test_system_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("system", "status", env=e2e_profile)
        result.assert_success()

    def test_system_status_json(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("system", "status", "--output", "json", env=e2e_profile)
        result.assert_success()

    # ── mcp ─────────────────────────────────────────────────────────────

    def test_mcp_list_tools(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("mcp", "list-tools", env=e2e_profile)
        result.assert_success()

    # ── accounts / reports help-substring checks (in-process) ──────────
    # Verify each command registers and exposes its documented flags. The
    # shared e2e_profile has no transforms run yet so core.dim_accounts /
    # fct_balances_daily / reports.net_worth do not exist; read commands
    # that require those tables are covered at help-tier only. Write
    # commands (rename, include, archive, set, balance assert/delete/list)
    # are covered in test_e2e_mutating.py which uses isolated envs.
    #
    # Runs via CliRunner because --help is side-effect free per
    # ``.claude/rules/cli.md`` and doesn't need subprocess isolation.

    def test_accounts_list_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "list", "--help"])
        assert result.exit_code == 0, result.output
        assert "--output" in result.output

    def test_accounts_show_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "show", "--help"])
        assert result.exit_code == 0, result.output
        assert "account_id" in result.output.lower() or "ACCOUNT_ID" in result.output

    def test_accounts_resolve_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "resolve", "--help"])
        assert result.exit_code == 0, result.output
        assert "--limit" in result.output
        assert "--output" in result.output

    def test_accounts_balance_show_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "balance", "show", "--help"])
        assert result.exit_code == 0, result.output
        assert "--output" in result.output

    def test_accounts_balance_history_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "balance", "history", "--help"])
        assert result.exit_code == 0, result.output
        assert "--account" in result.output

    def test_accounts_balance_list_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "balance", "list", "--help"])
        assert result.exit_code == 0, result.output
        assert "--output" in result.output

    def test_accounts_balance_assert_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "balance", "assert", "--help"])
        assert result.exit_code == 0, result.output
        assert "--output" not in result.output or "account_id" in result.output.lower()

    def test_accounts_balance_delete_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "balance", "delete", "--help"])
        assert result.exit_code == 0, result.output
        assert "--yes" in result.output or "assertion_date" in result.output.lower()

    def test_accounts_balance_reconcile_help(self) -> None:
        result = _runner.invoke(app, ["accounts", "balance", "reconcile", "--help"])
        assert result.exit_code == 0, result.output
        assert "--threshold" in result.output

    def test_reports_networth_show_help(self) -> None:
        result = _runner.invoke(app, ["reports", "networth", "show", "--help"])
        assert result.exit_code == 0, result.output
        assert "--output" in result.output

    def test_reports_networth_history_help(self) -> None:
        result = _runner.invoke(app, ["reports", "networth", "history", "--help"])
        assert result.exit_code == 0, result.output
        assert "--from" in result.output

    # ── stats ───────────────────────────────────────────────────────────

    def test_stats_show(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("stats", env=e2e_profile)
        result.assert_success()
