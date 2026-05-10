"""Single-transaction manual entry command.

Thin wrapper over ``TransactionService.create_manual_batch`` for the one-row
case. Optional ``--note`` and ``--tag`` flags trigger follow-up calls so users
can enter a fully-annotated transaction in one CLI invocation.
"""

from __future__ import annotations

import logging
from datetime import date as date_cls
from decimal import Decimal, InvalidOperation

import typer

from moneybin.cli.output import OutputFormat, output_option
from moneybin.cli.utils import emit_json, handle_cli_errors

logger = logging.getLogger(__name__)


def transactions_create(
    amount: str = typer.Argument(..., help="Signed decimal amount (negative=expense)"),
    description: str = typer.Argument(..., help="Transaction description"),
    account: str = typer.Option(..., "--account", help="Account ID"),
    date: str | None = typer.Option(
        None, "--date", help="Transaction date YYYY-MM-DD (defaults to today)"
    ),
    category: str | None = typer.Option(None, "--category", help="Category name"),
    subcategory: str | None = typer.Option(None, "--subcategory", help="Subcategory"),
    merchant: str | None = typer.Option(None, "--merchant", help="Merchant name"),
    memo: str | None = typer.Option(None, "--memo", help="Memo text"),
    note: str | None = typer.Option(
        None, "--note", help="Free-form note (created as a separate note row)"
    ),
    tag: list[str] = typer.Option(None, "--tag", help="Tag (repeatable)"),
    check_number: str | None = typer.Option(
        None, "--check-number", help="Check number"
    ),
    payment_channel: str | None = typer.Option(
        None, "--payment-channel", help="Payment channel (e.g., online, in_store)"
    ),
    currency: str = typer.Option("USD", "--currency", help="ISO 4217 currency code"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),  # noqa: ARG001 — no interactive prompt yet; flag reserved for parity
    output: OutputFormat = output_option,
) -> None:
    """Create a single manual transaction."""
    from moneybin.services.transaction_service import TransactionService

    try:
        amount_dec = Decimal(amount)
    except InvalidOperation as e:
        typer.echo(f"❌ Invalid --amount {amount!r}: not a decimal", err=True)
        raise typer.Exit(2) from e

    parsed_date: date_cls
    if date is None:
        parsed_date = date_cls.today()
    else:
        try:
            parsed_date = date_cls.fromisoformat(date)
        except ValueError as e:
            typer.echo(f"❌ Invalid --date {date!r}: expected YYYY-MM-DD", err=True)
            raise typer.Exit(2) from e

    entry: dict[str, object] = {
        "account_id": account,
        "amount": amount_dec,
        "transaction_date": parsed_date,
        "description": description,
    }
    if category:
        entry["category"] = category
    if subcategory:
        entry["subcategory"] = subcategory
    if merchant:
        entry["merchant_name"] = merchant
    if memo:
        entry["memo"] = memo
    if check_number:
        entry["check_number"] = check_number
    if payment_channel:
        entry["payment_channel"] = payment_channel
    if currency:
        entry["currency_code"] = currency

    tags = list(tag or [])

    try:
        with handle_cli_errors() as db:
            svc = TransactionService(db)
            batch = svc.create_manual_batch([entry], actor="cli")
            row = batch.results[0]
            transaction_id = row.transaction_id
            note_id: str | None = None
            if note:
                created = svc.add_note(transaction_id, note, actor="cli")
                note_id = created.note_id
            applied_tags: list[str] = []
            if tags:
                applied_tags = svc.add_tags(transaction_id, tags, actor="cli")
    except ValueError as e:
        typer.echo(f"❌ {e}", err=True)
        raise typer.Exit(1) from e

    payload = {
        "transaction_id": transaction_id,
        "source_transaction_id": row.source_transaction_id,
        "import_id": batch.import_id,
        "note_id": note_id,
        "tags": applied_tags,
    }
    if output == OutputFormat.JSON:
        emit_json("manual_create", payload)
        return

    logger.info(
        f"✅ Created transaction {transaction_id} (import_id={batch.import_id})"
    )
    if note_id:
        logger.info(f"   note_id={note_id}")
    if applied_tags:
        logger.info(f"   tags: {', '.join(applied_tags)}")
