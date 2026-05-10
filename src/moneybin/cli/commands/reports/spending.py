"""moneybin reports spending — monthly spending trend with deltas."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.tables import REPORTS_SPENDING_TREND

logger = logging.getLogger(__name__)

spending_app = typer.Typer(
    help="Spending trend with MoM/YoY/trailing deltas", no_args_is_help=True
)

_VALID_COMPARE = {"yoy", "mom", "trailing"}


@spending_app.command("show")
def reports_spending_show(
    from_month: str | None = typer.Option(None, "--from", help="ISO date YYYY-MM-01"),
    to_month: str | None = typer.Option(None, "--to", help="ISO date YYYY-MM-01"),
    category: str | None = typer.Option(
        None, "--category", help="Filter to category text"
    ),
    compare: str = typer.Option("yoy", "--compare", help="yoy | mom | trailing"),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show spending trend with MoM/YoY/trailing comparisons."""
    if compare not in _VALID_COMPARE:
        raise typer.BadParameter(f"Unknown comparison: {compare}")
    with handle_cli_errors() as db:
        sql = f"""
            SELECT year_month, category, total_spend, txn_count,
                   prev_month_spend, mom_delta, mom_pct,
                   prev_year_spend, yoy_delta, yoy_pct,
                   trailing_3mo_avg
            FROM {REPORTS_SPENDING_TREND.full_name}
            WHERE 1=1
        """  # noqa: S608  # TableRef interpolation
        params: list[object] = []
        if from_month:
            sql += " AND year_month >= ?"
            params.append(from_month)
        if to_month:
            sql += " AND year_month <= ?"
            params.append(to_month)
        if category:
            sql += " AND category = ?"
            params.append(category)
        sql += " ORDER BY year_month, total_spend DESC"

        cursor = db.execute(sql, params)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
    payload = [dict(zip(cols, r, strict=False)) for r in rows]
    if output == OutputFormat.JSON:
        emit_json("spending", payload)
        return
    from rich.console import Console  # noqa: PLC0415 — defer heavy import
    from rich.table import Table  # noqa: PLC0415 — defer heavy import

    console = Console()
    table = Table(*cols)
    for r in rows:
        table.add_row(*[str(v) if v is not None else "" for v in r])
    console.print(table)
