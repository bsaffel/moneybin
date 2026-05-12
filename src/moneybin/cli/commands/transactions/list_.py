# src/moneybin/cli/commands/transactions/list_.py
"""transactions list — fetch and display transactions with optional filters."""

from __future__ import annotations

import logging
from decimal import Decimal

import typer

from moneybin.cli.output import (
    OutputFormat,
    output_option,
    quiet_option,
    render_or_json,
)
from moneybin.cli.utils import render_rich_table

logger = logging.getLogger(__name__)


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

    envelope = result.to_envelope()

    def _render_text(_: object) -> None:
        if not result.transactions:
            if not quiet:
                typer.echo("No transactions found.")
            return

        rows: list[tuple[object, ...]] = []
        for t in result.transactions:
            amt = t.amount
            amount_str = f"{amt:,.2f}"
            desc = (
                t.description[:49] + "…" if len(t.description) > 50 else t.description
            )
            rows.append((
                t.transaction_date,
                desc,
                amount_str,
                t.category or "",
                t.account_id,
            ))

        render_rich_table(
            ["date", "description", "amount", "category", "account"], rows
        )

        if result.next_cursor and not quiet:
            typer.echo(f"Next page: --cursor {result.next_cursor}", err=True)

    render_or_json(envelope, output, render_fn=_render_text)
