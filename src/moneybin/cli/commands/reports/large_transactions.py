"""moneybin reports large-transactions — anomaly-flavored transaction lens."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.tables import REPORTS_LARGE_TRANSACTIONS

logger = logging.getLogger(__name__)

large_transactions_app = typer.Typer(
    help="Anomaly-flavored transaction lens (top-N + z-score)",
    no_args_is_help=True,
)

_VALID_ANOMALY = {"none", "account", "category"}


@large_transactions_app.command("show")
def reports_large_transactions_show(
    top: int = typer.Option(25, "--top", help="Top N by ABS(amount)"),
    anomaly: str = typer.Option("none", "--anomaly", help="account | category | none"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show large transactions, optionally filtered to z-score outliers."""
    if anomaly not in _VALID_ANOMALY:
        raise typer.BadParameter(f"Unknown anomaly mode: {anomaly}")
    with handle_cli_errors() as db:
        sql = f"""
            SELECT transaction_id, account_name, txn_date, amount, description,
                   merchant_normalized, category, amount_zscore_account,
                   amount_zscore_category, is_top_100
            FROM {REPORTS_LARGE_TRANSACTIONS.full_name}
        """  # noqa: S608  # TableRef interpolation
        if anomaly == "account":
            sql += " WHERE amount_zscore_account > 2.5"
        elif anomaly == "category":
            sql += " WHERE amount_zscore_category > 2.5"
        sql += " ORDER BY ABS(amount) DESC LIMIT ?"

        cursor = db.execute(sql, [top])
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
    payload = [dict(zip(cols, r, strict=False)) for r in rows]
    if output == OutputFormat.JSON:
        emit_json("large_transactions", payload)
        return
    from rich.console import Console  # noqa: PLC0415 — defer heavy import
    from rich.table import Table  # noqa: PLC0415 — defer heavy import

    console = Console()
    table = Table(*cols)
    for r in rows:
        table.add_row(*[str(v) if v is not None else "" for v in r])
    console.print(table)
