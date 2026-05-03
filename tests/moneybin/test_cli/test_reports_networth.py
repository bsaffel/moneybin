"""Tests for reports networth + verifying track group is dissolved."""

from typer.testing import CliRunner

from moneybin.cli.main import app

runner = CliRunner()


def test_networth_show_stub() -> None:
    result = runner.invoke(app, ["reports", "networth", "show"])
    assert result.exit_code == 0
    assert "net-worth" in result.output


def test_networth_history_stub() -> None:
    result = runner.invoke(app, ["reports", "networth", "history"])
    assert result.exit_code == 0


def test_reports_health_stub() -> None:
    result = runner.invoke(app, ["reports", "health"])
    assert result.exit_code == 0


def test_reports_spending_stub() -> None:
    result = runner.invoke(app, ["reports", "spending"])
    assert result.exit_code == 0


def test_reports_cashflow_stub() -> None:
    result = runner.invoke(app, ["reports", "cashflow"])
    assert result.exit_code == 0


def test_reports_budget_stub() -> None:
    result = runner.invoke(app, ["reports", "budget"])
    assert result.exit_code == 0


def test_track_group_dissolved() -> None:
    """The track group must no longer exist as a top-level command."""
    result = runner.invoke(app, ["track", "--help"])
    # Either non-zero exit (unknown command) or output indicates no such command
    assert result.exit_code != 0 or "No such command" in result.output
