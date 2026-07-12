"""CLI coverage for `moneybin transactions categorize pending`.

Regression test for a Task 8 gap: `PendingTxnRow` (privacy/payloads/categorize.py)
gained a required `pending_transfer_match` field (F19), and the MCP tool
(`transactions_categorize_pending`) was updated to pass it through, but this
CLI command's `PendingTxnRow(...)` construction was missed. Because that
construction sits outside `handle_cli_errors()`, any non-empty result crashed
with an unhandled `TypeError` instead of a clean CLI error. No existing test
invoked this command with non-empty service results, so the gap shipped
unnoticed.
"""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.transactions.categorize import app

runner = CliRunner()

_ROW = {
    "transaction_id": "txn_1",
    "account_id": "acct_1",
    "account_name": "Checking",
    "txn_date": "2026-07-01",
    "amount": -42.50,
    "description": "Test Merchant",
    "merchant_id": None,
    "merchant_normalized": "Test Merchant",
    "age_days": 3,
    "priority_score": 127.5,
    "source_type": "ofx",
    "source_id": None,
    "pending_transfer_match": True,
}


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.get_database")
def test_categorize_pending_nonempty_result_does_not_crash(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """A non-empty pending queue renders instead of raising TypeError."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.list_uncategorized_transactions.return_value = [_ROW]

    result = runner.invoke(app, ["pending"])

    assert result.exit_code == 0, result.output


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.get_database")
def test_categorize_pending_json_includes_pending_transfer_match(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """`--output json` threads `pending_transfer_match` through, matching MCP."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.list_uncategorized_transactions.return_value = [_ROW]

    result = runner.invoke(app, ["pending", "--output", "json"])

    assert result.exit_code == 0, result.output
    assert '"pending_transfer_match": true' in result.output


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.get_database")
def test_categorize_pending_empty_result(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """An empty pending queue still renders cleanly (pre-existing behavior)."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.list_uncategorized_transactions.return_value = []

    result = runner.invoke(app, ["pending"])

    assert result.exit_code == 0, result.output
