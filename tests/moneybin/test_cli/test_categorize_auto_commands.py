"""CLI argument parsing for auto-rule commands.

Business logic is tested via auto_rule_service tests.
"""

import re
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.transactions.categorize import app
from moneybin.services.auto_rule_service import AutoConfirmResult

runner = CliRunner()

# Rich's help output styles flags with ANSI escapes that can split tokens
# (e.g., `--approve-all` rendered as `--approve` + reset + `-all`), which
# breaks substring asserts. Strip ANSI before matching.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def _plain(s: str) -> str:
    return _ANSI_RE.sub("", s)


def test_auto_review_help():
    """Auto review --help mentions pending proposals."""
    result = runner.invoke(app, ["auto", "review", "--help"])
    assert result.exit_code == 0
    assert "pending" in _plain(result.stdout).lower()


def test_auto_accept_help_lists_accept_and_reject_flags():
    """Auto accept --help exposes batch accept/reject flags."""
    result = runner.invoke(app, ["auto", "accept", "--help"])
    assert result.exit_code == 0
    out = _plain(result.stdout)
    assert "--accept" in out
    assert "--reject" in out
    assert "--accept-all" in out
    assert "--reject-all" in out


def test_auto_stats_help():
    """Auto stats --help renders without error."""
    result = runner.invoke(app, ["auto", "stats", "--help"])
    assert result.exit_code == 0


def test_auto_rules_help():
    """Auto rules --help renders without error."""
    result = runner.invoke(app, ["auto", "rules", "--help"])
    assert result.exit_code == 0


@pytest.mark.unit
def test_auto_subgroup_help_lists_all_actions() -> None:
    """Auto --help lists each sub-action."""
    from moneybin.cli.commands.transactions.categorize import app as categorize_app

    result = runner.invoke(categorize_app, ["auto", "--help"])
    assert result.exit_code == 0
    for action in ("review", "accept", "stats", "rules"):
        assert action in result.stdout


def _confirm_result(
    approved: int = 0, rejected: int = 0, skipped: int = 0
) -> AutoConfirmResult:
    return AutoConfirmResult(
        approved=approved,
        rejected=rejected,
        skipped=skipped,
        newly_categorized=0,
        rule_ids=[],
    )


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.transactions.categorize.auto.handle_cli_errors")
def test_auto_accept_explicit_accept(
    mock_db_ctx: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """Explicit --accept forwards exactly the given IDs to accept()."""
    mock_db_ctx.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.accept.return_value = _confirm_result(approved=2)

    result = runner.invoke(app, ["auto", "accept", "--accept", "a1", "--accept", "a2"])
    assert result.exit_code == 0
    svc.accept.assert_called_once_with(accept=["a1", "a2"], reject=[])


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.transactions.categorize.auto.handle_cli_errors")
def test_auto_accept_explicit_reject(
    mock_db_ctx: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """Explicit --reject forwards exactly the given IDs to accept()."""
    mock_db_ctx.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.accept.return_value = _confirm_result(rejected=1)

    result = runner.invoke(app, ["auto", "accept", "--reject", "r1"])
    assert result.exit_code == 0
    svc.accept.assert_called_once_with(accept=[], reject=["r1"])


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.transactions.categorize.auto.handle_cli_errors")
def test_auto_accept_accept_all_expands_pending(
    mock_db_ctx: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """--accept-all expands to every pending proposal ID."""
    mock_db_ctx.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.list_pending_proposals.return_value = [
        {"proposed_rule_id": "p1"},
        {"proposed_rule_id": "p2"},
    ]
    svc.accept.return_value = _confirm_result(approved=2)

    result = runner.invoke(app, ["auto", "accept", "--accept-all"])
    assert result.exit_code == 0
    svc.accept.assert_called_once_with(accept=["p1", "p2"], reject=[])


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.transactions.categorize.auto.handle_cli_errors")
def test_auto_accept_reject_all_expands_pending(
    mock_db_ctx: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """--reject-all expands to every pending proposal ID."""
    mock_db_ctx.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.list_pending_proposals.return_value = [
        {"proposed_rule_id": "p1"},
        {"proposed_rule_id": "p2"},
    ]
    svc.accept.return_value = _confirm_result(rejected=2)

    result = runner.invoke(app, ["auto", "accept", "--reject-all"])
    assert result.exit_code == 0
    svc.accept.assert_called_once_with(accept=[], reject=["p1", "p2"])


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.transactions.categorize.auto.handle_cli_errors")
def test_auto_accept_accept_all_with_explicit_reject_excludes_id(
    mock_db_ctx: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """--accept-all --reject <id> accepts all pending except <id>, which is rejected."""
    mock_db_ctx.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.list_pending_proposals.return_value = [
        {"proposed_rule_id": "p1"},
        {"proposed_rule_id": "p2"},
        {"proposed_rule_id": "p3"},
    ]
    svc.accept.return_value = _confirm_result(approved=2, rejected=1)

    result = runner.invoke(app, ["auto", "accept", "--accept-all", "--reject", "p2"])
    assert result.exit_code == 0
    svc.accept.assert_called_once_with(accept=["p1", "p3"], reject=["p2"])


def test_auto_accept_rejects_both_all_flags() -> None:
    """--accept-all and --reject-all are mutually exclusive (exit code 2)."""
    result = runner.invoke(app, ["auto", "accept", "--accept-all", "--reject-all"])
    assert result.exit_code == 2
