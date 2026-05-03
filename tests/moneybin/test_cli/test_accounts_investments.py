"""Smoke tests for accounts investments sub-group."""

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


def test_investments_help() -> None:
    result = runner.invoke(app, ["accounts", "investments", "--help"])
    assert result.exit_code == 0
