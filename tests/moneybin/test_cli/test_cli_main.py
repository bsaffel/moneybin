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


def test_version_flag_does_not_touch_the_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """--version must be side-effect free (cli.md Help Surface Contract)."""

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("--version must not open a database")

    monkeypatch.setattr("moneybin.database.get_database", _boom)
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
