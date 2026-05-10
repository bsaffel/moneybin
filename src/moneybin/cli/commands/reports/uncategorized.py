"""moneybin reports uncategorized — curator queue for uncategorized transactions."""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.tables import REPORTS_UNCATEGORIZED_QUEUE

logger = logging.getLogger(__name__)

uncategorized_app = typer.Typer(
    help="Curator queue for uncategorized transactions",
    no_args_is_help=True,
)


@uncategorized_app.command("show")
def reports_uncategorized_show(
    min_amount: str = typer.Option(
        "0", "--min-amount", help="Filter to absolute amount >= this"
    ),
    account: str | None = typer.Option(
        None, "--account", help="Filter to account name"
    ),
    limit: int = typer.Option(50, "--limit", help="Maximum rows"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show uncategorized transactions ordered by curator-impact."""
    try:
        min_amount_dec = Decimal(min_amount)
    except InvalidOperation as e:
        raise typer.BadParameter(f"Invalid --min-amount: {min_amount}") from e
    with handle_cli_errors() as db:
        sql = f"""
            SELECT transaction_id, account_id, account_name, txn_date, amount,
                   description, merchant_normalized, age_days, priority_score
            FROM {REPORTS_UNCATEGORIZED_QUEUE.full_name}
            WHERE ABS(amount) >= ?
        """  # noqa: S608  # TableRef interpolation, parameterized values
        params: list[object] = [min_amount_dec]
        if account:
            sql += " AND account_name = ?"
            params.append(account)
        sql += " ORDER BY priority_score DESC LIMIT ?"
        params.append(limit)

        cursor = db.execute(sql, params)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
    payload = [dict(zip(cols, r, strict=False)) for r in rows]
    if output == OutputFormat.JSON:
        emit_json("uncategorized", payload)
        return
    from rich.console import Console  # noqa: PLC0415 — defer heavy import
    from rich.table import Table  # noqa: PLC0415 — defer heavy import

    console = Console()
    table = Table(*cols)
    for r in rows:
        table.add_row(*[str(v) if v is not None else "" for v in r])
    console.print(table)
