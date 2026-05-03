"""Tests for budget mutation stubs."""

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


def test_budget_set() -> None:
    result = runner.invoke(app, ["budget", "set", "groceries", "500"])
    assert result.exit_code == 0


def test_budget_delete() -> None:
    result = runner.invoke(app, ["budget", "delete", "groceries"])
    assert result.exit_code == 0
