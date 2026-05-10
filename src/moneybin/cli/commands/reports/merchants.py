"""moneybin reports merchants — per-merchant activity rollup."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.tables import REPORTS_MERCHANT_ACTIVITY

logger = logging.getLogger(__name__)

merchants_app = typer.Typer(
    help="Per-merchant lifetime activity totals",
    no_args_is_help=True,
)

_SORT_KEYS = {
    "spend": "total_spend DESC",
    "count": "txn_count DESC",
    "recent": "last_seen DESC",
}


@merchants_app.command("show")
def reports_merchants_show(
    top: int = typer.Option(25, "--top", help="Limit to top N merchants"),
    sort: str = typer.Option("spend", "--sort", help="spend | count | recent"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show per-merchant activity totals."""
    if sort not in _SORT_KEYS:
        raise typer.BadParameter(f"Unknown sort key: {sort}")
    with handle_cli_errors() as db:
        sql = f"""
            SELECT merchant_normalized, total_spend, total_inflow, total_outflow,
                   txn_count, avg_amount, median_amount, first_seen, last_seen,
                   active_months, top_category, account_count
            FROM {REPORTS_MERCHANT_ACTIVITY.full_name}
            ORDER BY {_SORT_KEYS[sort]}
            LIMIT ?
        """  # noqa: S608  # TableRef interpolation + sort-key allowlist
        cursor = db.execute(sql, [top])
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
    payload = [dict(zip(cols, r, strict=False)) for r in rows]
    if output == OutputFormat.JSON:
        emit_json("merchants", payload)
        return
    from rich.console import Console  # noqa: PLC0415 — defer heavy import
    from rich.table import Table  # noqa: PLC0415 — defer heavy import

    console = Console()
    table = Table(*cols)
    for r in rows:
        table.add_row(*[str(v) if v is not None else "" for v in r])
    console.print(table)
