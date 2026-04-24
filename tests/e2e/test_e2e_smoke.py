# ruff: noqa: S101
"""E2E smoke tests — verify every CLI command boots without errors."""

from __future__ import annotations

import pytest

from tests.e2e.conftest import FIXTURES_DIR, run_cli

pytestmark = pytest.mark.e2e


# ---------------------------------------------------------------------------
# Tier 1: --help commands (no DB, no profile needed)
# ---------------------------------------------------------------------------

_HELP_COMMANDS: list[list[str]] = [
    [],  # moneybin --help
    ["profile"],
    ["import"],
    ["sync"],
    ["categorize"],
    ["matches"],
    ["transform"],
    ["synthetic"],
    ["db"],
    ["db", "migrate"],
    ["logs"],
    ["mcp"],
    ["stats"],
    ["track"],
    ["export"],
]


class TestHelpCommands:
    """Tier 1: every command group responds to --help without errors."""

    @pytest.mark.parametrize(
        "cmd",
        _HELP_COMMANDS,
        ids=[" ".join(c) if c else "top-level" for c in _HELP_COMMANDS],
    )
    def test_help_exits_cleanly(self, cmd: list[str]) -> None:
        result = run_cli(*cmd, "--help")
        result.assert_success()
        assert "Usage" in result.stdout or "usage" in result.stdout.lower()


# ---------------------------------------------------------------------------
# Tier 2: commands that run without a database
# ---------------------------------------------------------------------------


class TestNoDBCommands:
    """Tier 2: commands that execute real logic but don't need get_database()."""

    def test_profile_list(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("profile", "list", env=e2e_env)
        result.assert_success()

    def test_profile_show(self, e2e_env: dict[str, str]) -> None:
        # May show "no active profile" — that's fine, just no crash
        result = run_cli("profile", "show", env=e2e_env)
        # exit_code may be 0 or 1 depending on whether a profile is set
        assert "Traceback" not in result.stderr

    def test_import_list_formats(self) -> None:
        result = run_cli("import", "list-formats")
        result.assert_success()

    def test_import_preview(self) -> None:
        fixture = FIXTURES_DIR / "tabular" / "standard.csv"
        result = run_cli("import", "preview", str(fixture))
        result.assert_success()

    def test_logs_path(self, e2e_env: dict[str, str]) -> None:
        result = run_cli("logs", "path", env=e2e_env)
        result.assert_success()

    def test_mcp_list_tools(self) -> None:
        result = run_cli("mcp", "list-tools")
        result.assert_success()

    def test_mcp_list_prompts(self) -> None:
        result = run_cli("mcp", "list-prompts")
        result.assert_success()

    def test_db_ps(self) -> None:
        result = run_cli("db", "ps")
        result.assert_success()


# ---------------------------------------------------------------------------
# Tier 3: commands that need an initialized database
# ---------------------------------------------------------------------------


class TestDBCommands:
    """Tier 3: commands that go through get_database() → init_schemas."""

    def test_db_info(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "info", env=e2e_profile)
        result.assert_success()

    def test_db_query(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "query", "SELECT 1 AS ok", env=e2e_profile)
        result.assert_success()

    def test_db_migrate_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("db", "migrate", "status", env=e2e_profile)
        result.assert_success()

    def test_transform_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transform", "status", env=e2e_profile)
        result.assert_success()

    def test_transform_validate(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("transform", "validate", env=e2e_profile)
        result.assert_success()

    def test_import_status(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("import", "status", env=e2e_profile)
        result.assert_success()

    def test_import_history(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("import", "history", env=e2e_profile)
        result.assert_success()

    def test_categorize_stats(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("categorize", "stats", env=e2e_profile)
        result.assert_success()

    def test_categorize_list_rules(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("categorize", "list-rules", env=e2e_profile)
        result.assert_success()

    def test_matches_log(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("matches", "log", env=e2e_profile)
        result.assert_success()

    def test_stats_show(self, e2e_profile: dict[str, str]) -> None:
        result = run_cli("stats", "show", env=e2e_profile)
        result.assert_success()
