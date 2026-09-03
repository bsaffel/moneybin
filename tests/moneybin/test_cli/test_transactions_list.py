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
def test_list_text_names_the_account_column_for_the_key_it_holds() -> None:
    """Requirement 28: the column is `account_id` on both sides of the join.

    It has always held an id. Calling it `account` left the reader to guess
    whether they were looking at a name, and left the shared key with
    `accounts list` — the whole of the F7 fix — implicit.
    """
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService, "get", return_value=_mock_result([_make_txn()])
            ):
                result = runner.invoke(app, ["transactions", "list"])
    assert result.exit_code == 0
    assert "account_id" in result.output
    assert "A1" in result.output


@pytest.mark.unit
def test_list_text_frames_the_page_against_the_whole_result() -> None:
    """Requirement 34: `N of M shown` replaces the raw cursor line.

    The count comes from `total_count`, which the envelope already carried —
    the text branch simply never rendered it.
    """
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService,
                "get",
                return_value=_mock_result(
                    [_make_txn()], next_cursor="Y3Vyc29y", total_count=42
                ),
            ):
                result = runner.invoke(app, ["transactions", "list"])
    assert result.exit_code == 0
    assert "1 of 42 shown" in result.output


@pytest.mark.unit
def test_list_text_offers_no_continuation_on_the_last_page_of_a_walk() -> None:
    """Requirement 34's remedy follows `next_cursor`, not the remainder.

    `total_count` is every row matching the filters and does not shrink as a
    walk advances, so the last page of one still shows fewer rows than the
    total. Reading the remainder alone would offer `--limit` against a page
    that does not exist. The slice is still disclosed — the promise is not.
    """
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService,
                "get",
                return_value=_mock_result(
                    [_make_txn()], next_cursor=None, total_count=42
                ),
            ):
                result = runner.invoke(
                    app, ["transactions", "list", "--cursor", "Y3Vyc29y"]
                )
    assert result.exit_code == 0
    assert "1 of 42 shown" in result.output
    assert "--limit" not in result.output


@pytest.mark.unit
def test_list_text_never_prints_the_raw_cursor() -> None:
    """Requirement 34 deletes the line, it does not relocate it.

    F10 reported a base64 keyset token printed at the user. It is unreadable,
    cannot be retyped reliably, and told the reader nothing about the result.
    The continuation remains available where a caller can actually use it: the
    JSON envelope's `actions[]`, covered by its own test below.
    """
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService,
                "get",
                return_value=_mock_result(
                    [_make_txn()], next_cursor="Y3Vyc29y", total_count=42
                ),
            ):
                result = runner.invoke(app, ["transactions", "list"])
    assert result.exit_code == 0
    assert "Y3Vyc29y" not in result.output
    assert "Next page" not in result.output


@pytest.mark.unit
def test_list_text_names_an_uncategorized_row_and_counts_it() -> None:
    """Requirements 29, 30: one word for an absent category, and a count.

    `core.fct_transactions.category` is NULL for a row categorization has not
    reached, and the text branch is the only surface that owes the reader a
    word for that. The count rides the framing line rather than a
    `render_note`, which `-q` would suppress — a taxonomy gap the user cannot
    see is what requirement 29 exists to prevent.
    """
    txns = [_make_txn(), _make_txn(transaction_id="T2", category=None)]
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService, "get", return_value=_mock_result(txns)
            ):
                result = runner.invoke(app, ["transactions", "list"])
    assert result.exit_code == 0
    assert "Uncategorized" in result.output
    assert "1 uncategorized" in result.output


@pytest.mark.unit
def test_list_text_keeps_the_uncategorized_count_under_quiet() -> None:
    """Requirement 30: `-q` asks for less chatter, not for a narrower truth.

    The count states how far the categories on screen can be trusted, which
    `cli.md` keeps printing under `-q` for the same reason it keeps a
    truncation notice.
    """
    txns = [_make_txn(transaction_id="T2", category=None)]
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService, "get", return_value=_mock_result(txns)
            ):
                result = runner.invoke(app, ["transactions", "list", "--quiet"])
    assert result.exit_code == 0
    assert "1 uncategorized" in result.output


@pytest.mark.unit
def test_list_json_leaves_an_absent_category_null() -> None:
    """Requirement 30: the placeholder is a text-rendering decision only.

    JSON carries the NULL through untouched, so a caller can still tell an
    uncategorized row from one categorized as the literal string.
    """
    import json

    txns = [_make_txn(category=None)]
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService, "get", return_value=_mock_result(txns)
            ):
                result = runner.invoke(
                    app, ["transactions", "list", "--output", "json"]
                )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["transactions"][0]["category"] is None
    assert "Uncategorized" not in result.stdout


@pytest.mark.unit
def test_list_does_not_elide_a_long_description() -> None:
    """A description wider than the old 50-character cut folds instead of clipping.

    `render_rows` declares `overflow="fold"` precisely so no value is dropped to
    make a row fit, and every other command this renderer covers relies on it.
    Cutting here reached the truncation before the renderer ever saw the value,
    so it fired on a wide terminal too, and a raw bank description carries its
    distinguishing detail at the end — the store, the reference, the autopay
    marker — which is exactly what a trailing ellipsis removes.
    """
    description = "SQ *DOWNTOWN COFFEE ROASTERS LLC 4th and Pine AUTOPAY RENEWAL"
    assert len(description) > 50, "the fixture must exceed the old cut to be a test"
    txns = [_make_txn(description=description)]
    with patch("moneybin.database.get_database", _mock_db_ctx):
        with patch("moneybin.cli.utils.handle_cli_errors", _mock_db_ctx):
            with patch.object(
                TransactionService, "get", return_value=_mock_result(txns)
            ):
                result = runner.invoke(
                    app, ["transactions", "list"], env={"COLUMNS": "250"}
                )
    assert result.exit_code == 0
    assert "\u2026" not in result.output
    assert description in result.output


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
