"""moneybin reports cashflow — monthly inflow/outflow/net."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.tables import REPORTS_CASH_FLOW

logger = logging.getLogger(__name__)

cashflow_app = typer.Typer(help="Monthly cash flow", no_args_is_help=True)

_VALID_BY = {"account", "category", "account-and-category"}


@cashflow_app.command("show")
def reports_cashflow_show(
    from_month: str | None = typer.Option(None, "--from", help="ISO date YYYY-MM-01"),
    to_month: str | None = typer.Option(None, "--to", help="ISO date YYYY-MM-01"),
    by: str = typer.Option(
        "account-and-category",
        "--by",
        help="account | category | account-and-category",
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show monthly cash flow rollup."""
    if by not in _VALID_BY:
        raise typer.BadParameter(f"Unknown grouping: {by}")
    with handle_cli_errors() as db:
        select_cols = "year_month"
        group_cols = "year_month"
        if "account" in by:
            select_cols += ", account_name"
            group_cols += ", account_name"
        if "category" in by:
            select_cols += ", category"
            group_cols += ", category"

        sql = f"""
            SELECT {select_cols},
                   SUM(inflow) AS inflow,
                   SUM(outflow) AS outflow,
                   SUM(net) AS net,
                   SUM(txn_count) AS txn_count
            FROM {REPORTS_CASH_FLOW.full_name}
            WHERE 1=1
        """  # noqa: S608  # TableRef + select_cols allowlist
        params: list[object] = []
        if from_month:
            sql += " AND year_month >= ?"
            params.append(from_month)
        if to_month:
            sql += " AND year_month <= ?"
            params.append(to_month)
        sql += f" GROUP BY {group_cols} ORDER BY year_month"  # noqa: S608  # group_cols allowlist

        cursor = db.execute(sql, params)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
    payload = [dict(zip(cols, r, strict=False)) for r in rows]
    if output == OutputFormat.JSON:
        emit_json("cashflow", payload)
        return
    from rich.console import Console  # noqa: PLC0415 — defer heavy import
    from rich.table import Table  # noqa: PLC0415 — defer heavy import

    console = Console()
    table = Table(*cols)
    for r in rows:
        table.add_row(*[str(v) if v is not None else "" for v in r])
    console.print(table)
