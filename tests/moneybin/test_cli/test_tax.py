"""Tests for tax stubs."""

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


def test_tax_w2() -> None:
    result = runner.invoke(app, ["tax", "w2", "2025"])
    assert result.exit_code == 0


def test_tax_deductions() -> None:
    result = runner.invoke(app, ["tax", "deductions", "2025"])
    assert result.exit_code == 0
