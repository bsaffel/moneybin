"""Smoke tests for accounts balance sub-group stubs."""

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


def test_balance_show_stub() -> None:
    result = runner.invoke(app, ["accounts", "balance", "show"])
    assert result.exit_code == 0
    assert "net-worth" in result.output


def test_balance_assert_stub() -> None:
    result = runner.invoke(
        app, ["accounts", "balance", "assert", "acct-1", "2026-05-01", "100.00"]
    )
    assert result.exit_code == 0


def test_balance_list_stub() -> None:
    result = runner.invoke(app, ["accounts", "balance", "list"])
    assert result.exit_code == 0


def test_balance_delete_stub() -> None:
    result = runner.invoke(
        app, ["accounts", "balance", "delete", "acct-1", "2026-05-01"]
    )
    assert result.exit_code == 0


def test_balance_reconcile_stub() -> None:
    result = runner.invoke(app, ["accounts", "balance", "reconcile"])
    assert result.exit_code == 0


def test_balance_history_stub() -> None:
    result = runner.invoke(app, ["accounts", "balance", "history"])
    assert result.exit_code == 0
