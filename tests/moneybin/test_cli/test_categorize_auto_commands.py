"""CLI argument parsing for auto-rule commands.

Business logic is tested via auto_rule_service tests.
"""

from typer.testing import CliRunner

from moneybin.cli.commands.categorize import app

# Wide terminal prevents Rich from wrapping flag names mid-token (e.g.,
# `--approve-all` → `--approve\n-all`), which would break substring asserts
# on CI runners that default to a narrow $COLUMNS.
runner = CliRunner(env={"COLUMNS": "200"})


def test_auto_review_help():
    """auto-review --help mentions pending proposals."""
    result = runner.invoke(app, ["auto-review", "--help"])
    assert result.exit_code == 0
    assert "pending" in result.stdout.lower()


def test_auto_confirm_help_lists_approve_and_reject_flags():
    """auto-confirm --help exposes batch approve/reject flags."""
    result = runner.invoke(app, ["auto-confirm", "--help"])
    assert result.exit_code == 0
    assert "--approve" in result.stdout
    assert "--reject" in result.stdout
    assert "--approve-all" in result.stdout
    assert "--reject-all" in result.stdout


def test_auto_stats_help():
    """auto-stats --help renders without error."""
    result = runner.invoke(app, ["auto-stats", "--help"])
    assert result.exit_code == 0


def test_auto_rules_help():
    """auto-rules --help renders without error."""
    result = runner.invoke(app, ["auto-rules", "--help"])
    assert result.exit_code == 0
