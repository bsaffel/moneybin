"""CLI argument parsing for auto-rule commands.

Business logic is tested via auto_rule_service tests.
"""

import re

from typer.testing import CliRunner

from moneybin.cli.commands.categorize import app

runner = CliRunner()

# Rich's help output styles flags with ANSI escapes that can split tokens
# (e.g., `--approve-all` rendered as `--approve` + reset + `-all`), which
# breaks substring asserts. Strip ANSI before matching.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def _plain(s: str) -> str:
    return _ANSI_RE.sub("", s)


def test_auto_review_help():
    """auto-review --help mentions pending proposals."""
    result = runner.invoke(app, ["auto-review", "--help"])
    assert result.exit_code == 0
    assert "pending" in _plain(result.stdout).lower()


def test_auto_confirm_help_lists_approve_and_reject_flags():
    """auto-confirm --help exposes batch approve/reject flags."""
    result = runner.invoke(app, ["auto-confirm", "--help"])
    assert result.exit_code == 0
    out = _plain(result.stdout)
    assert "--approve" in out
    assert "--reject" in out
    assert "--approve-all" in out
    assert "--reject-all" in out


def test_auto_stats_help():
    """auto-stats --help renders without error."""
    result = runner.invoke(app, ["auto-stats", "--help"])
    assert result.exit_code == 0


def test_auto_rules_help():
    """auto-rules --help renders without error."""
    result = runner.invoke(app, ["auto-rules", "--help"])
    assert result.exit_code == 0
