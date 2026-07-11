"""Tests for the top-level `moneybin` CLI callback: --version flag."""

import importlib.metadata

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app, get_version

runner = CliRunner()


def test_version_flag_prints_installed_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert get_version() in result.stdout


def test_get_version_matches_distribution_metadata() -> None:
    assert get_version() == importlib.metadata.version("moneybin")


def test_version_preempts_subcommand_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--version stats` must print and exit before `stats` runs.

    Guards the ``raise typer.Exit()`` in ``_version_callback``: without it,
    Typer's eager-option handling would print the version and then fall
    through to execute the subcommand in full (cli.md Help Surface
    Contract). A bare ``--version`` invocation can't detect this — with no
    subcommand, ``main_callback``'s body never runs regardless of whether
    the eager exit fires, so the guard has to be exercised through a real
    subcommand that would otherwise touch the database.
    """

    def _boom(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("stats subcommand must not run when --version is set")

    monkeypatch.setattr("moneybin.cli.commands.stats.get_database", _boom)
    result = runner.invoke(app, ["--version", "stats"])
    assert result.exit_code == 0
    assert get_version() in result.stdout
