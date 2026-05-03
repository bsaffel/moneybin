"""Smoke tests for accounts entity ops (list, show, rename, include)."""

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


def test_accounts_list_stub() -> None:
    result = runner.invoke(app, ["accounts", "list"])
    assert result.exit_code == 0
    assert (
        "not yet implemented" in result.output.lower()
        or "account-management" in result.output
    )


def test_accounts_show_stub() -> None:
    result = runner.invoke(app, ["accounts", "show", "fake-id"])
    assert result.exit_code == 0
    assert (
        "not yet implemented" in result.output.lower()
        or "account-management" in result.output
    )


def test_accounts_rename_stub() -> None:
    result = runner.invoke(app, ["accounts", "rename", "fake-id", "new-name"])
    assert result.exit_code == 0


def test_accounts_include_stub() -> None:
    result = runner.invoke(app, ["accounts", "include", "fake-id"])
    assert result.exit_code == 0
