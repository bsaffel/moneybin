"""CLI argument parsing for auto-rule commands.

Business logic is tested via auto_rule_service tests.
"""

import re
from unittest.mock import MagicMock, patch

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


def _confirm_result(
    approved: int = 0, rejected: int = 0, skipped: int = 0
) -> dict[str, object]:
    return {
        "approved": approved,
        "newly_categorized": 0,
        "rule_ids": [],
        "rejected": rejected,
        "skipped": skipped,
    }


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.categorize.handle_database_errors")
def test_auto_confirm_explicit_approve(
    mock_db_ctx: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """Explicit --approve forwards exactly the given IDs to confirm()."""
    mock_db_ctx.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.confirm.return_value = _confirm_result(approved=2)

    result = runner.invoke(app, ["auto-confirm", "--approve", "a1", "--approve", "a2"])
    assert result.exit_code == 0
    svc.confirm.assert_called_once_with(approve=["a1", "a2"], reject=[])


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.categorize.handle_database_errors")
def test_auto_confirm_explicit_reject(
    mock_db_ctx: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """Explicit --reject forwards exactly the given IDs to confirm()."""
    mock_db_ctx.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.confirm.return_value = _confirm_result(rejected=1)

    result = runner.invoke(app, ["auto-confirm", "--reject", "r1"])
    assert result.exit_code == 0
    svc.confirm.assert_called_once_with(approve=[], reject=["r1"])


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.categorize.handle_database_errors")
def test_auto_confirm_approve_all_expands_pending(
    mock_db_ctx: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """--approve-all expands to every pending proposal ID."""
    mock_db_ctx.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.list_pending_proposals.return_value = [
        {"proposed_rule_id": "p1"},
        {"proposed_rule_id": "p2"},
    ]
    svc.confirm.return_value = _confirm_result(approved=2)

    result = runner.invoke(app, ["auto-confirm", "--approve-all"])
    assert result.exit_code == 0
    svc.confirm.assert_called_once_with(approve=["p1", "p2"], reject=[])


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.categorize.handle_database_errors")
def test_auto_confirm_reject_all_expands_pending(
    mock_db_ctx: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """--reject-all expands to every pending proposal ID."""
    mock_db_ctx.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.list_pending_proposals.return_value = [
        {"proposed_rule_id": "p1"},
        {"proposed_rule_id": "p2"},
    ]
    svc.confirm.return_value = _confirm_result(rejected=2)

    result = runner.invoke(app, ["auto-confirm", "--reject-all"])
    assert result.exit_code == 0
    svc.confirm.assert_called_once_with(approve=[], reject=["p1", "p2"])


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.categorize.handle_database_errors")
def test_auto_confirm_approve_all_with_explicit_reject_excludes_id(
    mock_db_ctx: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """--approve-all --reject <id> approves all pending except <id>, which is rejected."""
    mock_db_ctx.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.list_pending_proposals.return_value = [
        {"proposed_rule_id": "p1"},
        {"proposed_rule_id": "p2"},
        {"proposed_rule_id": "p3"},
    ]
    svc.confirm.return_value = _confirm_result(approved=2, rejected=1)

    result = runner.invoke(app, ["auto-confirm", "--approve-all", "--reject", "p2"])
    assert result.exit_code == 0
    svc.confirm.assert_called_once_with(approve=["p1", "p3"], reject=["p2"])


def test_auto_confirm_rejects_both_all_flags() -> None:
    """--approve-all and --reject-all are mutually exclusive (exit code 2)."""
    result = runner.invoke(app, ["auto-confirm", "--approve-all", "--reject-all"])
    assert result.exit_code == 2
