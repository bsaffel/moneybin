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
    transactions: list[Transaction],
    next_cursor: str | None = None,
    total_count: int | None = None,
) -> TransactionGetResult:
    return TransactionGetResult(
        transactions=transactions,
        next_cursor=next_cursor,
        total_count=len(transactions) if total_count is None else total_count,
    )


@contextmanager
def _mock_db_ctx(*_args: object, **_kwargs: object):
    """Context manager that yields a mock database.

    Used as both a handle_cli_errors replacement and a get_database mock.
    Accepts any args/kwargs so it can replace get_database(read_only=True).
    """
    yield MagicMock()


@pytest.mark.unit
def test_list_text_output_shows_columns() -> None:
    """Text output renders date, description, amount, category, account columns."""
    txns = [_make_txn()]
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService, "get", return_value=_mock_result(txns)
            ):
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
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService, "get", return_value=_mock_result(txns)
            ):
                result = runner.invoke(
                    app, ["transactions", "list", "--output", "json"]
                )
    assert result.exit_code == 0
    parsed = json.loads(result.output)
    assert "summary" in parsed
    assert "data" in parsed
    # Transaction rows carry amount (TXN_AMOUNT → HIGH); account_id is RECORD_ID
    # (spec D6). CLI render_or_json stamps the derived tier over the declared value.
    assert parsed["summary"]["sensitivity"] == "high"


@pytest.mark.unit
def test_list_json_reports_the_service_total_not_the_page_size() -> None:
    """summary.total_count carries the service's match count, not len(page).

    Breaks if the ``total_count=`` argument is dropped from ``build_envelope``
    — the envelope would infer 1 from the single returned row.
    """
    import json

    txns = [_make_txn()]
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService,
                "get",
                return_value=_mock_result(txns, next_cursor="Y3Vyc29y", total_count=42),
            ):
                result = runner.invoke(
                    app, ["transactions", "list", "--limit", "1", "--output", "json"]
                )
    assert result.exit_code == 0
    summary = json.loads(result.stdout)["summary"]
    assert summary["total_count"] == 42
    assert summary["returned_count"] == 1
    assert summary["has_more"] is True


@pytest.mark.unit
def test_list_json_has_more_is_false_on_the_last_page_of_a_walk() -> None:
    """The final page must not tell an agent to keep paginating.

    ``build_envelope`` defaults ``has_more`` to
    ``next_cursor is not None or total_count > returned_count``. Once the real
    cross-page ``total_count`` is threaded through, that second clause is true
    on every page of a multi-page walk — including the last, where there is no
    cursor left to follow. Its sibling test cannot see this: it exercises a
    page that *does* carry a cursor, where the buggy default agrees with the
    right answer by coincidence.
    """
    import json

    page = [_make_txn(transaction_id="T3"), _make_txn(transaction_id="T4")]
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService,
                "get",
                return_value=_mock_result(page, next_cursor=None, total_count=4),
            ):
                result = runner.invoke(
                    app, ["transactions", "list", "--limit", "2", "--output", "json"]
                )
    assert result.exit_code == 0
    summary = json.loads(result.stdout)["summary"]
    assert summary["total_count"] == 4
    assert summary["returned_count"] == 2
    assert summary["has_more"] is False


@pytest.mark.unit
def test_list_actions_name_cli_commands_not_mcp_tools() -> None:
    """Hints must be runnable by the agent that received them.

    Breaks if a hint reverts to naming an MCP tool — an agent driving the CLI
    cannot call one, and MCP renames would silently stale the CLI's output.
    """
    import json

    txns = [_make_txn()]
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService,
                "get",
                return_value=_mock_result(txns, next_cursor="Y3Vyc29y", total_count=42),
            ):
                result = runner.invoke(
                    app, ["transactions", "list", "--output", "json"]
                )
    actions = json.loads(result.stdout)["actions"]
    assert actions
    assert all("moneybin " in action for action in actions)
    assert any("--cursor Y3Vyc29y" in action for action in actions)


@pytest.mark.unit
def test_list_omits_the_cursor_hint_on_the_last_page() -> None:
    """No next_cursor means no 'fetch the next page' hint to follow."""
    import json

    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService, "get", return_value=_mock_result([_make_txn()])
            ):
                result = runner.invoke(
                    app, ["transactions", "list", "--output", "json"]
                )
    actions = json.loads(result.stdout)["actions"]
    assert not any("--cursor" in action for action in actions)


@pytest.mark.unit
def test_list_empty_text_output() -> None:
    """Empty result set prints a 'no transactions' message."""
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(TransactionService, "get", return_value=_mock_result([])):
                result = runner.invoke(app, ["transactions", "list"])
    assert result.exit_code == 0
    assert "No transactions" in result.output


@pytest.mark.unit
def test_list_passes_account_to_service() -> None:
    """--account is forwarded to TransactionService.get()."""
    with patch("moneybin.database.get_database", _mock_db_ctx):
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
    with patch("moneybin.database.get_database", _mock_db_ctx):
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
    with patch("moneybin.database.get_database", _mock_db_ctx):
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
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService, "get", return_value=_mock_result([])
            ) as mock_get:
                runner.invoke(app, ["transactions", "list", "--uncategorized"])
    assert mock_get.call_args.kwargs["uncategorized_only"] is True


@pytest.mark.unit
def test_list_cursor_forwarded() -> None:
    """--cursor is forwarded to TransactionService.get()."""
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService, "get", return_value=_mock_result([])
            ) as mock_get:
                runner.invoke(app, ["transactions", "list", "--cursor", "dGVzdA=="])
    assert mock_get.call_args.kwargs["cursor"] == "dGVzdA=="


@pytest.mark.unit
def test_the_suggested_continuation_command_reruns_with_the_same_filters() -> None:
    """An agent must be able to run the continuation hint verbatim.

    The cursor is bound to the filters that produced it
    (`TransactionService._get_cursor_scope`), so a continuation that drops
    `--account` decodes against a different scope and the service rejects it —
    `test_cursor_is_rejected_when_the_filters_change` pins exactly that. A
    filter-less hint is therefore guaranteed-invalid for every filtered query,
    which is the common case. The MCP twin (`_transaction_actions`) already
    echoes each active filter back into a complete continuation call.
    """
    import json
    import shlex

    txns = [_make_txn()]
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService,
                "get",
                return_value=_mock_result(txns, next_cursor="c1", total_count=4),
            ):
                first = runner.invoke(
                    app,
                    [
                        "transactions",
                        "list",
                        "--account",
                        "Test Bank",
                        "--from",
                        "2026-01-01",
                        "--uncategorized",
                        "--limit",
                        "2",
                        "--output",
                        "json",
                    ],
                )

    assert first.exit_code == 0
    hint = json.loads(first.output)["actions"][0]
    argv = shlex.split(hint.split("`")[1])
    assert argv[0] == "moneybin"

    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService, "get", return_value=_mock_result([])
            ) as rerun:
                second = runner.invoke(app, argv[1:])

    assert second.exit_code == 0
    kwargs = rerun.call_args.kwargs
    assert kwargs["accounts"] == ["Test Bank"]
    assert kwargs["date_from"] == "2026-01-01"
    assert kwargs["uncategorized_only"] is True
    assert kwargs["limit"] == 2
    assert kwargs["cursor"] == "c1"


@pytest.mark.unit
def test_the_suggested_continuation_command_keeps_the_json_output_mode() -> None:
    """A machine-readable page walk must stay machine-readable past page one.

    `--output` is not a filter, so it is absent from the cursor scope and the
    continuation still *succeeds* without it — it just renders page two as a
    human table and drops the next cursor into the text branch. An agent
    walking pages via `--output json` gets a parse error rather than an
    envelope, which is the same dead end the missing filters caused.
    """
    import json
    import shlex

    txns = [_make_txn()]
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService,
                "get",
                return_value=_mock_result(txns, next_cursor="c1", total_count=4),
            ):
                first = runner.invoke(
                    app,
                    ["transactions", "list", "--limit", "2", "--output", "json"],
                )

    assert first.exit_code == 0
    hint = json.loads(first.output)["actions"][0]
    argv = shlex.split(hint.split("`")[1])

    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(TransactionService, "get", return_value=_mock_result([])):
                second = runner.invoke(app, argv[1:])

    assert second.exit_code == 0
    # The page-two response must still parse as an envelope.
    assert "summary" in json.loads(second.output)
