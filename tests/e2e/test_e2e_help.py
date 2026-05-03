# ruff: noqa: S101
"""E2E help tests — every command group responds to --help without errors.

This is the cheapest E2E tier: no profile, no DB, just verify that Typer
wiring and imports don't crash when the app boots.
"""

from __future__ import annotations

import pytest

from tests.e2e.conftest import run_cli

pytestmark = pytest.mark.e2e

_HELP_COMMANDS: list[list[str]] = [
    [],  # moneybin --help
    ["profile"],
    ["import"],
    ["import", "inbox"],
    ["import", "inbox", "list"],
    ["import", "inbox", "path"],
    ["import", "formats"],
    ["sync"],
    ["sync", "schedule"],
    ["sync", "key"],
    ["accounts"],
    ["accounts", "balance"],
    ["accounts", "investments"],
    ["transactions"],
    ["transactions", "matches"],
    ["transactions", "review"],
    ["transactions", "categorize"],
    ["transactions", "categorize", "auto"],
    ["transactions", "categorize", "auto", "review"],
    ["transactions", "categorize", "auto", "confirm"],
    ["transactions", "categorize", "auto", "stats"],
    ["transactions", "categorize", "auto", "rules"],
    ["transactions", "categorize", "rules"],
    ["transactions", "categorize", "ml"],
    ["categories"],
    ["merchants"],
    ["assets"],
    ["reports"],
    ["reports", "networth"],
    ["budget"],
    ["tax"],
    ["system"],
    ["transform"],
    ["synthetic"],
    ["db"],
    ["db", "key"],
    ["db", "migrate"],
    ["logs"],
    ["mcp"],
    ["mcp", "config"],
    ["stats"],
    ["export"],
]


class TestHelpCommands:
    """Every command group responds to --help without errors."""

    @pytest.mark.parametrize(
        "cmd",
        _HELP_COMMANDS,
        ids=[" ".join(c) if c else "top-level" for c in _HELP_COMMANDS],
    )
    def test_help_exits_cleanly(self, cmd: list[str]) -> None:
        result = run_cli(*cmd, "--help")
        result.assert_success()
        assert "Usage" in result.stdout or "usage" in result.stdout.lower()
