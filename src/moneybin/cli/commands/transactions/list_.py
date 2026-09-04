# src/moneybin/cli/commands/transactions/list_.py
"""transactions list — fetch and display transactions with optional filters."""

from __future__ import annotations

import logging
import shlex
from dataclasses import replace
from decimal import Decimal
from typing import cast

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.render import (
    UNCATEGORIZED_LABEL,
    Money,
    Placeholder,
    render_rows,
)

logger = logging.getLogger(__name__)


def _continuation_command(invocation: dict[str, object], next_cursor: str) -> str:
    """Reproduce this invocation with the cursor added.

    Two distinct reasons every argument carries, not just the filters. The
    cursor is *bound* to the filters that produced it, so dropping one makes
    the service reject the continuation outright. ``--output`` is not bound —
    the call still succeeds without it, which is worse: page two silently
    renders as a human table and an agent walking pages gets a parse error
    instead of an envelope.

    Mirrors the MCP twin (``_transaction_actions``), which already emits a
    complete continuation call. ``shlex.join`` handles account names and
    description patterns containing spaces or quotes.
    """
    argv = ["moneybin", "transactions", "list"]
    for account in cast("list[str]", invocation["accounts"]):
        argv += ["--account", account]
    for category in cast("list[str]", invocation["categories"]):
        argv += ["--category", category]
    for flag, value in (
        ("--from", invocation["date_from"]),
        ("--to", invocation["date_to"]),
        ("--amount-min", invocation["amount_min"]),
        ("--amount-max", invocation["amount_max"]),
        ("--description", invocation["description"]),
    ):
        if value is not None:
            argv += [flag, str(value)]
    if invocation["uncategorized"]:
        argv.append("--uncategorized")
    if invocation["quiet"]:
        argv.append("--quiet")
    argv += [
        "--limit",
        str(invocation["limit"]),
        "--output",
        str(invocation["output"]),
        "--cursor",
        next_cursor,
    ]
    return shlex.join(argv)


def _list_actions(next_cursor: str | None, invocation: dict[str, object]) -> list[str]:
    """Next-step hints for an agent driving the CLI.

    These name CLI invocations, not MCP tools. An agent reading a CLI envelope
    can only run commands, and citing tool names here also drags the CLI into
    the blast radius of every MCP rename.
    """
    actions = [
        "Use `moneybin reports spending` for category breakdowns",
        "Use `moneybin transactions categorize run` to categorize uncategorized "
        "transactions",
    ]
    if next_cursor is not None:
        actions.insert(
            0,
            f"Use `{_continuation_command(invocation, next_cursor)}` "
            "to fetch the next page",
        )
    return actions


def transactions_list(
    accounts: list[str] = typer.Option(
        [], "--account", help="Account ID or display name (repeatable)."
    ),
    date_from: str | None = typer.Option(
        None, "--from", help="Start date ISO 8601, inclusive."
    ),
    date_to: str | None = typer.Option(
        None, "--to", help="End date ISO 8601, inclusive."
    ),
    categories: list[str] = typer.Option(
        [], "--category", help="Category filter (repeatable)."
    ),
    amount_min: str | None = typer.Option(
        None, "--amount-min", help="Minimum amount as decimal string (e.g. '-50.00')."
    ),
    amount_max: str | None = typer.Option(
        None, "--amount-max", help="Maximum amount as decimal string."
    ),
    description: str | None = typer.Option(
        None, "--description", help="ILIKE pattern against description and memo."
    ),
    uncategorized: bool = typer.Option(
        False,
        "--uncategorized",
        help="Only transactions with no user/AI/rule categorization assigned.",
    ),
    limit: int = typer.Option(50, "--limit", help="Maximum rows to return."),
    cursor: str | None = typer.Option(
        None, "--cursor", help="Pagination token from previous call."
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List transactions with optional filters."""
    from moneybin.cli.utils import handle_cli_errors
    from moneybin.database import get_database
    from moneybin.services.transaction_service import TransactionService

    with handle_cli_errors():
        with get_database(read_only=True) as db:
            result = TransactionService(db).get(
                accounts=accounts or None,
                date_from=date_from,
                date_to=date_to,
                categories=categories or None,
                amount_min=Decimal(amount_min) if amount_min is not None else None,
                amount_max=Decimal(amount_max) if amount_max is not None else None,
                description=description,
                uncategorized_only=uncategorized,
                limit=limit,
                cursor=cursor,
            )

    from moneybin.privacy.payloads.transactions import (
        TransactionGetPayload,
        TransactionRow,
    )
    from moneybin.protocol.envelope import build_envelope

    payload = TransactionGetPayload(
        transactions=[
            TransactionRow(
                transaction_id=t.transaction_id,
                account_id=t.account_id,
                transaction_date=t.transaction_date,
                amount=t.amount,
                currency_code=t.currency_code,
                description=t.description,
                memo=t.memo,
                source_type=t.source_type,
                category=t.category,
                subcategory=t.subcategory,
                notes=t.notes,
                tags=t.tags,
                splits=t.splits,
            )
            for t in result.transactions
        ],
        next_cursor=result.next_cursor,
    )
    envelope = build_envelope(
        data=payload,
        sensitivity="medium",
        total_count=result.total_count,
        returned_count=len(result.transactions),
        next_cursor=result.next_cursor,
        actions=_list_actions(
            result.next_cursor,
            {
                "accounts": accounts,
                "categories": categories,
                "date_from": date_from,
                "date_to": date_to,
                "amount_min": amount_min,
                "amount_max": amount_max,
                "description": description,
                "uncategorized": uncategorized,
                "limit": limit,
                "output": output.value,
                "quiet": quiet,
            },
        ),
    )
    # Under keyset pagination the cursor is the only truth about "more".
    # `build_envelope` also infers has_more from `total_count > returned_count`,
    # which is true on every page of a multi-page walk — including the last,
    # where there is nothing left to fetch. Every other keyset call site applies
    # this same override.
    envelope = replace(
        envelope,
        summary=replace(envelope.summary, has_more=result.next_cursor is not None),
    )

    def _render_text(_: object) -> None:
        if not result.transactions:
            if not quiet:
                typer.echo("No transactions found.")
            return

        rows: list[tuple[object, ...]] = []
        for t in result.transactions:
            rows.append((
                t.transaction_date,
                # Unclipped: `render_rows` folds an overlong value rather than
                # eliding it, and a raw bank description carries the detail that
                # separates two similar charges at the end.
                t.description,
                # Unformatted: `render_rows` stringifies it through
                # `format_money`, which is the only place text output does so.
                t.amount,
                # Passed through as stored, `None` included: `render_rows`
                # substitutes the declared placeholder and counts what it
                # substituted, so a category a person authored as
                # `Uncategorized` is not counted as a missing one.
                t.category,
                t.account_id,
            ))

        render_rows(
            # `account_id`, not `account`: the column holds an id, and naming
            # it what it is makes the join with `accounts list` visible rather
            # than merely possible (requirement 28). The account's display name
            # is deliberately absent — `TransactionRow` carries no account name,
            # and adding one would change the payload requirement 8 keeps
            # untouched.
            ["date", "description", "amount", "category", "account_id"],
            rows,
            # A transaction amount is signed under the AGENTS.md convention —
            # negative is an expense, positive is income — which is `flow`.
            money={"amount": Money("flow")},
            # Requirement 34. This replaces a `Next page: --cursor <base64>`
            # line: the cursor is an opaque keyset token that told the reader
            # nothing about the result and could not be typed back reliably,
            # while `--cursor` itself remains in `--help` and in the JSON
            # envelope's actions for the caller that walks pages.
            total_rows=result.total_count,
            # The same fact the JSON envelope carries as `summary.has_more`,
            # and the only truthful gate on the continuation: `total_count` is
            # every row matching the filters, so it still exceeds the page on
            # the last page of a cursor walk, where nothing more is left to
            # fetch.
            has_more=result.next_cursor is not None,
            placeholder=Placeholder("category", UNCATEGORIZED_LABEL),
        )

    render_or_json(
        envelope, output, render_fn=_render_text, cli_actor="transactions_list"
    )
