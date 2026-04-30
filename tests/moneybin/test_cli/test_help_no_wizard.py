"""Verify --help is side-effect free across all command groups."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app  # noqa: F401  # ensures module is imported

# `from moneybin.cli import main` resolves to the re-exported function rather
# than the submodule (the CLI package re-exports `main` in its __init__).
# Look the module up via sys.modules to monkeypatch its attributes.
cli_main = sys.modules["moneybin.cli.main"]
cli_utils = sys.modules["moneybin.cli.utils"]


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
    """`moneybin <group> --help` must not call the first-run profile wizard."""
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / ".config"))
    monkeypatch.delenv("MONEYBIN_PROFILE", raising=False)

    # Behavioral assertion: replace ensure_default_profile with a mock and
    # verify the parent callback never reaches it on --help. This decouples
    # the test from the wizard's prompt copy, so reword-the-prompt won't
    # silently break the regression net.
    wizard_mock = MagicMock(name="ensure_default_profile")
    monkeypatch.setattr(cli_utils, "ensure_default_profile", wizard_mock)

    result = runner.invoke(app, [*argv, "--help"])

    assert result.exit_code == 0, f"--help failed: {result.output}"
    wizard_mock.assert_not_called()
    # Defense in depth: even if some other code path tried to provision a
    # profile, the filesystem must remain untouched on --help.
    assert not list((tmp_path / ".config").rglob("profiles")), (
        "wizard wrote profile data during --help"
    )
