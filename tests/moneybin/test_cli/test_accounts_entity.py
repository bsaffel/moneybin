"""Smoke tests for accounts entity ops (list, get, set, resolve).

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


def test_accounts_get_help() -> None:
    """Accounts get command wires and shows --help."""
    result = runner.invoke(app, ["accounts", "get", "--help"])
    assert result.exit_code == 0


def test_accounts_set_help_lists_new_flags() -> None:
    """`accounts set --help` advertises the folded-in behavioral flags."""
    result = runner.invoke(app, ["accounts", "set", "--help"])
    assert result.exit_code == 0
    # Behavioral flags folded in from the removed narrow commands
    assert "--display-name" in result.output
    assert "--include" in result.output and "--exclude" in result.output
    assert "--archive" in result.output and "--unarchive" in result.output


def test_accounts_rename_command_removed() -> None:
    """`accounts rename` was folded into `accounts set --display-name`."""
    result = runner.invoke(app, ["accounts", "rename", "x", "y"])
    assert result.exit_code != 0


def test_accounts_archive_command_removed() -> None:
    """`accounts archive` was folded into `accounts set --archive`."""
    result = runner.invoke(app, ["accounts", "archive", "x"])
    assert result.exit_code != 0
