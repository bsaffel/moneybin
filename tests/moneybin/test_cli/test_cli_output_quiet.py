"""Verify -o/--output and -q/--quiet are accepted by every read-only command."""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app as root_app


@pytest.fixture()
def runner() -> CliRunner:
    """Return a Typer CliRunner for invoking the root app."""
    return CliRunner()


_READ_ONLY_HELP_PATHS: list[list[str]] = [
    ["db", "info", "--help"],
    ["db", "query", "--help"],
    ["db", "ps", "--help"],
    ["db", "key", "show", "--help"],
    ["import", "status", "--help"],
    ["import", "history", "--help"],
    ["import", "formats", "list", "--help"],
    ["import", "formats", "show", "--help"],
    ["categorize", "summary", "--help"],
    ["categorize", "auto", "stats", "--help"],
    ["categorize", "list-rules", "--help"],
    ["sync", "status", "--help"],
    ["profile", "list", "--help"],
    ["profile", "show", "--help"],
    ["matches", "history", "--help"],
    ["mcp", "list-tools", "--help"],
    ["mcp", "list-prompts", "--help"],
    ["db", "migrate", "status", "--help"],
    ["stats", "--help"],
    ["logs", "--help"],
]


@pytest.mark.unit
@pytest.mark.parametrize("argv", _READ_ONLY_HELP_PATHS, ids=lambda a: " ".join(a))
def test_read_only_command_advertises_output_and_quiet(
    runner: CliRunner, argv: list[str]
) -> None:
    """Every read-only command's --help advertises -o/--output and -q/--quiet."""
    result = runner.invoke(root_app, argv)
    assert result.exit_code == 0, result.output
    out = result.stdout
    # Both flags must appear in the help text
    assert "--output" in out, f"missing --output in {' '.join(argv)}"
    assert "--quiet" in out, f"missing --quiet in {' '.join(argv)}"
    # Short forms must also be present
    assert "-o" in out, f"missing -o in {' '.join(argv)}"
    assert "-q" in out, f"missing -q in {' '.join(argv)}"
