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

import pytest
from typer.testing import CliRunner

from moneybin.cli.commands.transactions.categorize import app
from moneybin.cli.terminal import TerminalPolicy, TerminalSymbols

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


def _pager_policy(*, page: bool, width: int = 80) -> TerminalPolicy:
    return TerminalPolicy(
        output="text",
        interactive=True,
        page=page,
        color=False,
        style=False,
        animate_progress=False,
        stage_chatter=False,
        ascii=True,
        width=width,
        height=3,
        symbols=TerminalSymbols(success="OK", attention="!", failure="X", action=">"),
        minus="-",
    )


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


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.get_database")
def test_categorize_pending_renders_amount_as_a_signed_flow(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """The queue's `amount` is a transaction amount, so it renders like one.

    Undeclared, `_cells` falls back to `str()` and prints `-42.5` —
    hyphen-minus, no separator, left-aligned — leaving one migrated command as
    an exception to the contract the rest of this milestone establishes.
    `priority_score` stays undeclared on purpose: it is `ABS(amount) *
    age_days`, a ranking weight in no currency.
    """
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    svc = mock_svc_cls.return_value
    svc.list_uncategorized_transactions.return_value = [_ROW]

    result = runner.invoke(app, ["pending"], env={"COLUMNS": "250"})

    assert result.exit_code == 0, result.output
    assert "−42.50" in result.output
    assert "-42.5" not in result.output


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.get_database")
def test_categorize_pending_empty_queue_explains_scope_and_next_action(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """A clear queue must distinguish no work from a failed lookup."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_svc_cls.return_value.list_uncategorized_transactions.return_value = []

    result = runner.invoke(app, ["pending", "--no-pager"])

    assert result.exit_code == 0, result.output
    assert "No uncategorized transactions" in result.stdout
    assert "moneybin transactions categorize stats" in result.stdout


def test_categorize_pending_help_offers_no_pager_without_opening_database() -> None:
    """The finite queue must expose its safe paging bypass in static help."""
    result = runner.invoke(app, ["pending", "--help"])

    assert result.exit_code == 0, result.output
    assert "--no-pager" in result.stdout


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.get_database")
def test_categorize_pending_at_limit_discloses_unknown_total_under_quiet(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """A bounded queue must not claim its first batch is the whole queue."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_svc_cls.return_value.list_uncategorized_transactions.return_value = [_ROW] * 2

    result = runner.invoke(app, ["pending", "--limit", "2", "--quiet", "--no-pager"])

    assert result.exit_code == 0, result.output
    assert "Showing 2 (limit 2; total unknown)." in result.stdout


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.services.account_service.AccountService")
@patch("moneybin.cli.commands.transactions.categorize.get_database")
def test_categorize_pending_filtered_empty_state_names_effective_scope(
    mock_get_db: MagicMock, mock_accounts: MagicMock, mock_svc_cls: MagicMock
) -> None:
    """A filtered empty queue must not read like the global queue is empty."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_accounts.return_value.resolve_strict.return_value = "acct-12345678901234567890"
    mock_svc_cls.return_value.list_uncategorized_transactions.return_value = []

    result = runner.invoke(
        app,
        [
            "pending",
            "--account",
            "Checking",
            "--min-amount",
            "20",
            "--sort",
            "impact",
            "--no-pager",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "account acct-12345678901234567890" in result.stdout
    assert "minimum amount 20" in result.stdout
    assert "sort impact" in result.stdout
    assert "moneybin transactions categorize pending --min-amount 0" in result.stdout


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.get_database")
def test_categorize_pending_pager_and_no_pager_receive_the_same_complete_answer(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Paging consumes the rendered answer; bypass prints that exact answer."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    long_row = {**_ROW, "transaction_id": "txn_123456789012345678901234567890"}
    mock_svc_cls.return_value.list_uncategorized_transactions.return_value = [
        long_row
    ] * 4
    captured: list[str] = []

    monkeypatch.setattr(
        "moneybin.cli.commands.transactions.categorize.get_terminal_policy",
        lambda *, no_pager=False: _pager_policy(page=not no_pager),
    )

    def capture_page(text: str, *, color: bool, wide: bool) -> bool:
        captured.append(text)
        return True

    monkeypatch.setattr(
        "moneybin.cli.pager.page_text",
        capture_page,
    )

    paged = runner.invoke(app, ["pending", "--limit", "4"])
    direct = runner.invoke(app, ["pending", "--limit", "4", "--no-pager"])

    assert paged.exit_code == direct.exit_code == 0
    assert len(captured) == 1
    assert "Showing 4 (limit 4; total unknown)." in captured[0]
    assert captured[0].removesuffix("\nq return to shell\n") == direct.stdout


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.get_database")
@pytest.mark.parametrize("columns", [40, 80, 81, 120])
def test_categorize_pending_narrow_layout_keeps_identity_and_money_facts(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock, columns: int
) -> None:
    """Mandatory selection and financial facts survive terminal widths."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    transaction_id = "txn_123456789012345678901234567890"
    mock_svc_cls.return_value.list_uncategorized_transactions.return_value = [
        {
            **_ROW,
            "transaction_id": transaction_id,
            "amount": -123456789.12,
            "currency_code": "USD",
        }
    ]

    result = runner.invoke(
        app, ["pending", "--no-pager"], env={"COLUMNS": str(columns)}
    )

    assert result.exit_code == 0, result.output
    assert transaction_id in result.stdout
    assert "−123,456,789.12 USD" in result.stdout
    assert "2026-07-01" in result.stdout
    assert "Test Merchant" in result.stdout
    assert "acct_1" in result.stdout
    assert "Age days" in result.stdout


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.get_database")
def test_categorize_pending_stacked_amount_uses_the_ascii_minus(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The stack must honor the same terminal minus as shared money tables."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_svc_cls.return_value.list_uncategorized_transactions.return_value = [_ROW]
    monkeypatch.setattr(
        "moneybin.cli.commands.transactions.categorize.get_terminal_policy",
        lambda *, no_pager=False: _pager_policy(page=False),
    )

    result = runner.invoke(app, ["pending", "--no-pager"])

    assert result.exit_code == 0, result.output
    assert "-42.50" in result.stdout
    assert "−42.50" not in result.stdout


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.get_database")
@pytest.mark.parametrize(
    ("columns", "expects_table"),
    [(80, False), (81, True)],
)
def test_categorize_pending_uses_a_table_only_when_complete_rows_fit(
    mock_get_db: MagicMock,
    mock_svc_cls: MagicMock,
    columns: int,
    expects_table: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The measured boundary keeps complete facts in a table when possible."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_svc_cls.return_value.list_uncategorized_transactions.return_value = [_ROW]
    monkeypatch.setattr(
        "moneybin.cli.commands.transactions.categorize.get_terminal_policy",
        lambda *, no_pager=False: _pager_policy(page=False, width=columns),
    )

    result = runner.invoke(app, ["pending", "--no-pager"])

    assert result.exit_code == 0, result.output
    assert ("┏" in result.stdout) is expects_table
    assert ("Transaction 1" in result.stdout) is not expects_table


@patch("moneybin.services.categorization.CategorizationService")
@patch("moneybin.cli.commands.transactions.categorize.get_database")
def test_categorize_pending_wide_fitting_queue_keeps_one_row_per_transaction(
    mock_get_db: MagicMock, mock_svc_cls: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A normal terminal keeps a short multi-row queue scannable."""
    mock_get_db.return_value.__enter__.return_value = MagicMock()
    mock_svc_cls.return_value.list_uncategorized_transactions.return_value = [
        {**_ROW, "transaction_id": "txn_a", "description": "Coffee"},
        {**_ROW, "transaction_id": "txn_b", "description": "Groceries"},
    ]

    monkeypatch.setattr(
        "moneybin.cli.commands.transactions.categorize.get_terminal_policy",
        lambda *, no_pager=False: _pager_policy(page=False, width=120),
    )
    result = runner.invoke(app, ["pending", "--no-pager"])

    assert result.exit_code == 0, result.output
    assert "Transaction ID" in result.stdout
    assert "Amount" in result.stdout
    assert "Date" in result.stdout
    assert "Description" in result.stdout
    assert "Account" in result.stdout
    assert "Age days" in result.stdout
    assert any(
        "txn_a" in line and "Coffee" in line for line in result.stdout.splitlines()
    )
    assert any(
        "txn_b" in line and "Groceries" in line for line in result.stdout.splitlines()
    )
