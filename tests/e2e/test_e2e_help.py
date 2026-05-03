# ruff: noqa: S101
"""E2E help tests — every command group responds to --help without errors.

Most cases run in-process via Typer's CliRunner since `--help` is pure
documentation rendering. One subprocess boot smoke (`moneybin --help`)
catches packaging/entry-point regressions. CLI cold-start fidelity for
real commands is exercised by the other E2E tiers (readonly, mutating,
workflows, mcp).
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
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
    ["accounts", "list"],
    ["accounts", "show"],
    ["accounts", "rename"],
    ["accounts", "include"],
    ["accounts", "archive"],
    ["accounts", "unarchive"],
    ["accounts", "set"],
    ["accounts", "balance", "show"],
    ["accounts", "balance", "history"],
    ["accounts", "balance", "assert"],
    ["accounts", "balance", "list"],
    ["accounts", "balance", "delete"],
    ["accounts", "balance", "reconcile"],
    ["reports", "networth", "show"],
    ["reports", "networth", "history"],
    ["export"],
]

_runner = CliRunner()


class TestHelpCommandsInProcess:
    """Every command group responds to --help without errors (in-process)."""

    @pytest.mark.parametrize(
        "cmd",
        _HELP_COMMANDS,
        ids=[" ".join(c) if c else "top-level" for c in _HELP_COMMANDS],
    )
    def test_help_exits_cleanly(self, cmd: list[str]) -> None:
        result = _runner.invoke(app, [*cmd, "--help"])
        assert result.exit_code == 0, (
            f"--help exited {result.exit_code} for {cmd}\noutput: {result.output}"
        )
        assert "Usage" in result.output or "usage" in result.output.lower()


class TestHelpCommandBootSmoke:
    """One subprocess invocation to catch packaging/entry-point regressions."""

    def test_top_level_help_via_subprocess(self) -> None:
        result = run_cli("--help")
        result.assert_success()
        assert "Usage" in result.stdout or "usage" in result.stdout.lower()
