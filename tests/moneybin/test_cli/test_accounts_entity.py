"""Smoke tests for accounts entity ops (list, show, rename, include).

These verify that the commands are wired and their help text is accessible.
Full behavioral coverage is in test_accounts.py (which mocks AccountService).
"""

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


def test_accounts_list_help() -> None:
    """Accounts list command wires and shows --help."""
    result = runner.invoke(app, ["accounts", "list", "--help"])
    assert result.exit_code == 0
    assert "--output" in result.output


def test_accounts_show_help() -> None:
    """Accounts show command wires and shows --help."""
    result = runner.invoke(app, ["accounts", "show", "--help"])
    assert result.exit_code == 0


def test_accounts_rename_help() -> None:
    """Accounts rename command wires and shows --help."""
    result = runner.invoke(app, ["accounts", "rename", "--help"])
    assert result.exit_code == 0


def test_accounts_include_help() -> None:
    """Accounts include command wires and shows --help."""
    result = runner.invoke(app, ["accounts", "include", "--help"])
    assert result.exit_code == 0
