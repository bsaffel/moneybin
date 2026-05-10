"""moneybin reports recurring — likely-recurring subscription candidates."""

from __future__ import annotations

import logging

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import emit_json, handle_cli_errors
from moneybin.tables import REPORTS_RECURRING_SUBSCRIPTIONS

logger = logging.getLogger(__name__)

recurring_app = typer.Typer(
    help="Likely-recurring subscription candidates with confidence scores",
    no_args_is_help=True,
)

_VALID_STATUSES = {"active", "inactive", "all"}
_VALID_CADENCES = {"weekly", "biweekly", "monthly", "quarterly", "yearly"}


@recurring_app.command("show")
def reports_recurring_show(
    min_confidence: float = typer.Option(
        0.5,
        "--min-confidence",
        help="Filter to candidates at or above this confidence (0.0-1.0)",
    ),
    status: str = typer.Option("active", "--status", help="active | inactive | all"),
    cadence: str | None = typer.Option(
        None, "--cadence", help="weekly | biweekly | monthly | quarterly | yearly"
    ),
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,  # noqa: ARG001
) -> None:
    """Show likely-recurring subscriptions ordered by annualized cost."""
    if status not in _VALID_STATUSES:
        raise typer.BadParameter(f"Unknown status: {status}")
    if cadence is not None and cadence not in _VALID_CADENCES:
        raise typer.BadParameter(f"Unknown cadence: {cadence}")
    with handle_cli_errors() as db:
        sql = f"""
            SELECT merchant_normalized, cadence, avg_amount, occurrence_count,
                   first_seen, last_seen, status, annualized_cost, confidence
            FROM {REPORTS_RECURRING_SUBSCRIPTIONS.full_name}
            WHERE confidence >= ?
        """  # noqa: S608  # TableRef interpolation, parameterized values
        params: list[object] = [min_confidence]
        if status != "all":
            sql += " AND status = ?"
            params.append(status)
        if cadence:
            sql += " AND cadence = ?"
            params.append(cadence)
        sql += " ORDER BY annualized_cost DESC NULLS LAST"

        cursor = db.execute(sql, params)
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description]
    payload = [dict(zip(cols, r, strict=False)) for r in rows]
    if output == OutputFormat.JSON:
        emit_json("recurring", payload)
        return
    from rich.console import Console  # noqa: PLC0415 — defer heavy import
    from rich.table import Table  # noqa: PLC0415 — defer heavy import

    console = Console()
    table = Table(*cols)
    for r in rows:
        table.add_row(*[str(v) if v is not None else "" for v in r])
    console.print(table)
