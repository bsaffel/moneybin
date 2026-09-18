"""Human-output coverage for the bounded categorization assist queue."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from moneybin.cli.commands.transactions.categorize import app
from moneybin.services.categorization.assist import RedactedTransaction


def _row() -> RedactedTransaction:
    return RedactedTransaction(
        transaction_id="txn_12345678901234567890",
        description_scrubbed="Example merchant",
        memo_scrubbed="",
        source_type="ofx",
        transaction_type=None,
        check_number=None,
        is_transfer=False,
        transfer_pair_id=None,
        payment_channel=None,
        amount_sign="-",
    )


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.privacy.sensitivity.audit_log")
@patch("moneybin.cli.commands.transactions.categorize.get_database")
def test_categorize_assist_at_limit_discloses_unknown_total(
    mock_get_db: MagicMock, _mock_audit: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """The LLM handoff count is bounded, so it cannot imply completeness."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_svc_cls.return_value.categorize_assist.return_value = [_row(), _row()]

    result = CliRunner().invoke(app, ["assist", "--limit", "2", "--no-pager"])

    assert result.exit_code == 0, result.output
    assert "Showing 2 (limit 2; total unknown)." in result.stdout
