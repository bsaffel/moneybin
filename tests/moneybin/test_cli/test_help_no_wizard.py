"""Verify --help is side-effect free across all command groups."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app


@pytest.fixture()
def runner() -> CliRunner:
    """CLI runner. Click 8.2+ separates stderr by default; mix_stderr was removed."""
    return CliRunner()


_GROUPS = [
    [],  # top-level
    ["profile"],
    ["import"],
    ["sync"],
    ["categorize"],
    ["matches"],
    ["transform"],
    ["synthetic"],
    ["track"],
    ["stats"],
    ["export"],
    ["mcp"],
    ["db"],
    ["logs"],
]


@pytest.mark.unit
@pytest.mark.parametrize("argv", _GROUPS, ids=lambda a: " ".join(a) or "root")
def test_help_does_not_trigger_first_run_wizard(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    argv: list[str],
) -> None:
    """`moneybin <group> --help` must not prompt for profile setup or write files."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    monkeypatch.delenv("MONEYBIN_PROFILE", raising=False)
    # CliRunner replaces sys.argv with [''], so the production --help
    # short-circuit (which inspects sys.argv) wouldn't fire under tests.
    # Set sys.argv to mirror the real invocation.
    monkeypatch.setattr(sys, "argv", ["moneybin", *argv, "--help"])

    result = runner.invoke(app, [*argv, "--help"])

    assert result.exit_code == 0, f"--help failed: {result.output}"
    assert "First name" not in result.stdout
    assert "First name" not in (result.stderr or "")
    assert not list((tmp_path / ".config").rglob("profiles")), (
        "wizard wrote profile data during --help"
    )
