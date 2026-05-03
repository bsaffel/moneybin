"""Tests for system status stub."""

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


def test_system_status() -> None:
    result = runner.invoke(app, ["system", "status"])
    assert result.exit_code == 0
