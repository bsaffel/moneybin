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
    ["import", "formats"],
    ["sync"],
    ["categorize"],
    ["categorize", "auto"],
    ["categorize", "auto", "review"],
    ["categorize", "auto", "confirm"],
    ["categorize", "auto", "stats"],
    ["categorize", "auto", "rules"],
    ["matches"],
    ["transform"],
    ["synthetic"],
    ["db"],
    ["db", "migrate"],
    ["logs"],
    ["mcp"],
    ["mcp", "config"],
    ["stats"],
    ["track"],
    ["track", "balance"],
    ["track", "networth"],
    ["track", "budget"],
    ["track", "recurring"],
    ["track", "investments"],
    ["export"],
    ["sync", "schedule"],
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
