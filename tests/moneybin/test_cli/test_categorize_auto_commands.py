"""CLI argument parsing for auto-rule commands.

Business logic is tested via auto_rule_service tests.
"""

import logging
import re
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.transactions.categorize import app
from moneybin.services.auto_rule_service import AutoConfirmResult, AutoReviewResult

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


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.transactions.categorize.auto.get_database")
@patch("moneybin.cli.commands.transactions.categorize.auto.handle_cli_errors")
def test_auto_review_text_output_shows_match_count_and_flags_broad(
    mock_handle_errors: MagicMock,
    _mock_get_db: MagicMock,
    mock_svc_cls: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Text `auto review` prints estimated_match_count and marks a broad proposal.

    Regression guard: the text loop used to print pattern/match_type/category/
    trigger_count/samples but never estimated_match_count or is_broad — only
    ``--output json`` carried them, so a human running ``auto review`` had no
    visibility into blast radius before accepting a proposal that would
    recategorize hundreds of rows.
    """
    mock_handle_errors.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.review.return_value = AutoReviewResult(
        proposals=[
            {
                "proposed_rule_id": "safe1",
                "merchant_pattern": "AMZN",
                "match_type": "contains",
                "category": "Shopping",
                "subcategory": None,
                "trigger_count": 3,
                "sample_txn_ids": [],
                "estimated_match_count": 3,
                "is_broad": False,
            },
            {
                "proposed_rule_id": "broad1",
                "merchant_pattern": "TO",
                "match_type": "exact",
                "category": "Internal Transfer",
                "subcategory": None,
                "trigger_count": 1,
                "sample_txn_ids": [],
                "estimated_match_count": 400,
                "is_broad": True,
            },
        ],
        total_count=2,
    )

    with caplog.at_level(
        logging.INFO, logger="moneybin.cli.commands.transactions.categorize.auto"
    ):
        result = runner.invoke(app, ["auto", "review"])

    assert result.exit_code == 0, result.output
    messages = [r.message for r in caplog.records]
    safe_line = next(m for m in messages if "safe1" in m)
    broad_line = next(m for m in messages if "broad1" in m)
    assert "~3 matches" in safe_line
    assert "BROAD" not in safe_line
    assert "~400 matches" in broad_line
    assert "BROAD" in broad_line
    assert "--allow-broad" in broad_line


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
@patch("moneybin.cli.commands.transactions.categorize.auto.get_database")
@patch("moneybin.cli.commands.transactions.categorize.auto.handle_cli_errors")
def test_auto_accept_explicit_accept(
    mock_db_ctx: MagicMock, _mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """Explicit --accept forwards exactly the given IDs to accept()."""
    mock_db_ctx.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.accept.return_value = _confirm_result(approved=2)

    result = runner.invoke(app, ["auto", "accept", "--accept", "a1", "--accept", "a2"])
    assert result.exit_code == 0
    svc.accept.assert_called_once_with(
        accept=["a1", "a2"], reject=[], actor="cli", allow_broad=False
    )


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.transactions.categorize.auto.get_database")
@patch("moneybin.cli.commands.transactions.categorize.auto.handle_cli_errors")
def test_auto_accept_explicit_reject(
    mock_db_ctx: MagicMock, _mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """Explicit --reject forwards exactly the given IDs to accept()."""
    mock_db_ctx.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.accept.return_value = _confirm_result(rejected=1)

    result = runner.invoke(app, ["auto", "accept", "--reject", "r1"])
    assert result.exit_code == 0
    svc.accept.assert_called_once_with(
        accept=[], reject=["r1"], actor="cli", allow_broad=False
    )


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.transactions.categorize.auto.get_database")
@patch("moneybin.cli.commands.transactions.categorize.auto.handle_cli_errors")
def test_auto_accept_accept_all_expands_pending(
    mock_db_ctx: MagicMock, _mock_get_db: MagicMock, mock_svc_cls: MagicMock
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
    svc.accept.assert_called_once_with(
        accept=["p1", "p2"], reject=[], actor="cli", allow_broad=False
    )


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.transactions.categorize.auto.get_database")
@patch("moneybin.cli.commands.transactions.categorize.auto.handle_cli_errors")
def test_auto_accept_reject_all_expands_pending(
    mock_db_ctx: MagicMock, _mock_get_db: MagicMock, mock_svc_cls: MagicMock
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
    svc.accept.assert_called_once_with(
        accept=[], reject=["p1", "p2"], actor="cli", allow_broad=False
    )


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.transactions.categorize.auto.get_database")
@patch("moneybin.cli.commands.transactions.categorize.auto.handle_cli_errors")
def test_auto_accept_accept_all_with_explicit_reject_excludes_id(
    mock_db_ctx: MagicMock, _mock_get_db: MagicMock, mock_svc_cls: MagicMock
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
    svc.accept.assert_called_once_with(
        accept=["p1", "p3"], reject=["p2"], actor="cli", allow_broad=False
    )


@patch("moneybin.services.auto_rule_service.AutoRuleService")
@patch("moneybin.cli.commands.transactions.categorize.auto.get_database")
@patch("moneybin.cli.commands.transactions.categorize.auto.handle_cli_errors")
def test_auto_accept_allow_broad_forwards_true(
    mock_db_ctx: MagicMock, _mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """--allow-broad forwards allow_broad=True to accept() (F17 Layer 3 override)."""
    mock_db_ctx.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.accept.return_value = _confirm_result(approved=1)

    result = runner.invoke(app, ["auto", "accept", "--accept", "a1", "--allow-broad"])
    assert result.exit_code == 0
    svc.accept.assert_called_once_with(
        accept=["a1"], reject=[], actor="cli", allow_broad=True
    )


def test_auto_accept_help_mentions_allow_broad() -> None:
    """Auto accept --help documents --allow-broad and its blast-radius caution."""
    result = runner.invoke(app, ["auto", "accept", "--help"])
    assert result.exit_code == 0
    out = _plain(result.stdout)
    assert "--allow-broad" in out


def test_auto_accept_rejects_both_all_flags() -> None:
    """--accept-all and --reject-all are mutually exclusive (exit code 2)."""
    result = runner.invoke(app, ["auto", "accept", "--accept-all", "--reject-all"])
    assert result.exit_code == 2
