"""Transaction splits subgroup: add, list, remove, clear.

Thin wrappers over ``TransactionService`` split methods. After ``add`` and
``remove`` we report the parent's residual balance so users see at a glance
whether children sum to the parent. Non-zero residual is a warning, not an
error (per spec — splits are warn-not-block).
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.services.transaction_service import Split

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Splits: allocate one transaction across categories",
    no_args_is_help=True,
)


def _split_to_dict(s: Split) -> dict[str, object]:
    return {
        "split_id": s.split_id,
        "transaction_id": s.transaction_id,
        "amount": str(s.amount),
        "category": s.category,
        "subcategory": s.subcategory,
        "note": s.note,
        "ord": s.ord,
        "created_at": s.created_at,
        "created_by": s.created_by,
    }


@app.command("add")
def transactions_splits_add(
    transaction_id: str = typer.Argument(..., help="Parent transaction ID"),
    amount: str = typer.Argument(..., help="Signed decimal amount"),
    category: str | None = typer.Option(None, "--category", help="Category"),
    subcategory: str | None = typer.Option(None, "--subcategory", help="Subcategory"),
    note: str | None = typer.Option(None, "--note", help="Optional split note"),
    output: OutputFormat = output_option,
) -> None:
    """Append a split to a transaction."""
    from moneybin.services.transaction_service import TransactionService

    try:
        amount_dec = Decimal(amount)
    except InvalidOperation as e:
        typer.echo(f"❌ Invalid amount {amount!r}", err=True)
        raise typer.Exit(2) from e

    with handle_cli_errors(output=output) as db:
        svc = TransactionService(db)
        split = svc.add_split(
            transaction_id,
            amount_dec,
            category=category,
            subcategory=subcategory,
            note=note,
            actor="cli",
        )
        residual = svc.splits_balance(transaction_id)

    payload = {"split": _split_to_dict(split), "residual": str(residual)}
    if output == OutputFormat.JSON:
        emit_json("split", payload)
    else:
        logger.info(f"✅ Added split {split.split_id} to {transaction_id}")
    if residual != Decimal("0"):
        logger.warning(
            f"⚠️  Splits do not balance: residual={residual} on {transaction_id}"
        )


@app.command("list")
def transactions_splits_list(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """List splits on a transaction."""
    from moneybin.services.transaction_service import TransactionService

    with handle_cli_errors(output=output) as db:
        splits = TransactionService(db).list_splits(transaction_id)

    if output == OutputFormat.JSON:
        emit_json("splits", [_split_to_dict(s) for s in splits])
        return
    if not splits:
        if not quiet:
            logger.info(f"No splits on {transaction_id}")
        return
    for s in splits:
        cat = s.category or "-"
        typer.echo(f"  [{s.split_id}] {s.amount} {cat}")


@app.command("remove")
def transactions_splits_remove(
    split_id: str = typer.Argument(..., help="Split ID"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    output: OutputFormat = output_option,
) -> None:
    """Remove a single split."""
    from moneybin.services.transaction_service import TransactionService

    if not yes:
        if not typer.confirm(f"Remove split {split_id}?"):
            logger.info("Cancelled")
            raise typer.Exit(0)

    with handle_cli_errors(output=output) as db:
        svc = TransactionService(db)
        # Look up parent before delete so we can report residual after.
        parent = db.conn.execute(
            "SELECT transaction_id FROM app.transaction_splits WHERE split_id = ?",
            [split_id],
        ).fetchone()
        if parent is None:
            typer.echo(f"❌ split_id={split_id} not found", err=True)
            raise typer.Exit(1)
        transaction_id = parent[0]
        svc.remove_split(split_id, actor="cli")
        residual = svc.splits_balance(transaction_id)

    if output == OutputFormat.JSON:
        emit_json(
            "split_remove",
            {
                "split_id": split_id,
                "transaction_id": transaction_id,
                "residual": str(residual),
            },
        )
    else:
        logger.info(f"✅ Removed split {split_id}")
    if residual != Decimal("0"):
        logger.warning(
            f"⚠️  Splits do not balance: residual={residual} on {transaction_id}"
        )


@app.command("clear")
def transactions_splits_clear(
    transaction_id: str = typer.Argument(..., help="Transaction ID"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
    output: OutputFormat = output_option,
) -> None:
    """Delete all splits on a transaction."""
    from moneybin.services.transaction_service import TransactionService

    if not yes:
        if not typer.confirm(f"Clear all splits on {transaction_id}?"):
            logger.info("Cancelled")
            raise typer.Exit(0)

    with handle_cli_errors(output=output) as db:
        TransactionService(db).clear_splits(transaction_id, actor="cli")

    if output == OutputFormat.JSON:
        emit_json("split_clear", {"transaction_id": transaction_id, "cleared": True})
        return
    logger.info(f"✅ Cleared splits on {transaction_id}")
