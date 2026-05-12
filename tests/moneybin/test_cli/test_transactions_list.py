"""Tests for 'moneybin transactions list' CLI command."""

from __future__ import annotations

from contextlib import contextmanager
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.services.transaction_service import (
    Transaction,
    TransactionGetResult,
    TransactionService,
)

runner = CliRunner()


def _make_txn(**overrides: object) -> Transaction:
    """Build a Transaction with sensible defaults."""
    defaults: dict[str, object] = {
        "transaction_id": "T1",
        "account_id": "A1",
        "transaction_date": "2026-04-10",
        "amount": Decimal("-50.00"),
        "description": "Coffee Shop",
        "memo": None,
        "source_type": "ofx",
        "category": "Food & Drink",
        "subcategory": None,
        "notes": None,
        "tags": None,
        "splits": None,
    }
    defaults.update(overrides)
    return Transaction(**defaults)  # type: ignore[arg-type]


def _mock_result(
    transactions: list[Transaction], next_cursor: str | None = None
) -> TransactionGetResult:
    return TransactionGetResult(transactions=transactions, next_cursor=next_cursor)


@contextmanager
def _mock_db_ctx(**_kwargs: object):
    """Context manager that yields a mock database — replaces handle_cli_errors."""
    yield MagicMock()


@pytest.mark.unit
def test_list_text_output_shows_columns() -> None:
    """Text output renders date, description, amount, category, account columns."""
    txns = [_make_txn()]
    with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
        with patch.object(TransactionService, "get", return_value=_mock_result(txns)):
            result = runner.invoke(app, ["transactions", "list"])
    assert result.exit_code == 0
    assert "2026-04-10" in result.output
    assert "Coffee Shop" in result.output
    assert "Food & Drink" in result.output


@pytest.mark.unit
def test_list_json_output_returns_envelope() -> None:
    """--output json returns a ResponseEnvelope JSON object."""
    import json

    txns = [_make_txn()]
    with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
        with patch.object(TransactionService, "get", return_value=_mock_result(txns)):
            result = runner.invoke(app, ["transactions", "list", "--output", "json"])
    assert result.exit_code == 0
    parsed = json.loads(result.output)
    assert "summary" in parsed
    assert "data" in parsed
    assert parsed["summary"]["sensitivity"] == "medium"


@pytest.mark.unit
def test_list_empty_text_output() -> None:
    """Empty result set prints a 'no transactions' message."""
    with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
        with patch.object(TransactionService, "get", return_value=_mock_result([])):
            result = runner.invoke(app, ["transactions", "list"])
    assert result.exit_code == 0
    assert "No transactions" in result.output


@pytest.mark.unit
def test_list_passes_account_to_service() -> None:
    """--account is forwarded to TransactionService.get()."""
    with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
        with patch.object(
            TransactionService, "get", return_value=_mock_result([])
        ) as mock_get:
            runner.invoke(app, ["transactions", "list", "--account", "Test Bank"])
    mock_get.assert_called_once()
    assert mock_get.call_args.kwargs["accounts"] == ["Test Bank"]


@pytest.mark.unit
def test_list_repeatable_account_flag() -> None:
    """Multiple --account flags accumulate into a list."""
    with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
        with patch.object(
            TransactionService, "get", return_value=_mock_result([])
        ) as mock_get:
            runner.invoke(
                app, ["transactions", "list", "--account", "A1", "--account", "A2"]
            )
    assert mock_get.call_args.kwargs["accounts"] == ["A1", "A2"]


@pytest.mark.unit
def test_list_repeatable_category_flag() -> None:
    """Multiple --category flags accumulate into a list."""
    with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
        with patch.object(
            TransactionService, "get", return_value=_mock_result([])
        ) as mock_get:
            runner.invoke(
                app,
                [
                    "transactions",
                    "list",
                    "--category",
                    "Food & Drink",
                    "--category",
                    "Travel",
                ],
            )
    assert mock_get.call_args.kwargs["categories"] == ["Food & Drink", "Travel"]


@pytest.mark.unit
def test_list_uncategorized_flag() -> None:
    """--uncategorized sets uncategorized_only=True."""
    with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
        with patch.object(
            TransactionService, "get", return_value=_mock_result([])
        ) as mock_get:
            runner.invoke(app, ["transactions", "list", "--uncategorized"])
    assert mock_get.call_args.kwargs["uncategorized_only"] is True


@pytest.mark.unit
def test_list_cursor_forwarded() -> None:
    """--cursor is forwarded to TransactionService.get()."""
    with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
        with patch.object(
            TransactionService, "get", return_value=_mock_result([])
        ) as mock_get:
            runner.invoke(app, ["transactions", "list", "--cursor", "dGVzdA=="])
    assert mock_get.call_args.kwargs["cursor"] == "dGVzdA=="


@pytest.mark.unit
def test_list_json_fields_projection() -> None:
    """--json-fields projects the requested fields from the JSON envelope data."""
    import json

    txns = [_make_txn()]
    with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
        with patch.object(TransactionService, "get", return_value=_mock_result(txns)):
            result = runner.invoke(
                app,
                [
                    "transactions",
                    "list",
                    "--output",
                    "json",
                    "--json-fields",
                    "transaction_id,amount",
                ],
            )
    assert result.exit_code == 0
    parsed = json.loads(result.output)
    assert parsed["data"] == [{"transaction_id": "T1", "amount": "-50.00"}]
