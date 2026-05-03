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
    ["db", "key"],
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
    ["sync", "key"],
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
