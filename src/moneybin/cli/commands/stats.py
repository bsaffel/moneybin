"""Stats command for MoneyBin CLI.

Displays lifetime metric aggregates from the app.metrics table.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Annotated

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.utils import handle_cli_errors
from moneybin.utils.parsing import parse_duration

logger = logging.getLogger(__name__)


def stats_command(
    since: Annotated[
        str | None,
        typer.Option("--since", help="Time window (e.g., 7d, 24h)"),
    ] = None,
    metric: Annotated[
        str | None,
        typer.Option("--metric", help="Filter to a metric family (e.g., import)"),
    ] = None,
    output: OutputFormat = output_option,
    quiet: bool = quiet_option,
) -> None:
    """Display lifetime metric aggregates."""
    with handle_cli_errors(output=output) as db:
        where_clauses: list[str] = []
        params: list[str | datetime] = []

        if since:
            try:
                delta = parse_duration(since)
            except ValueError as e:
                logger.error(f"❌ {e}")
                raise typer.Exit(1) from e
            cutoff = datetime.now(tz=UTC) - delta
            where_clauses.append("recorded_at >= ?")
            params.append(cutoff)

        if metric:
            escaped = metric.replace("!", "!!").replace("%", "!%").replace("_", "!_")
            where_clauses.append("metric_name LIKE ? ESCAPE '!'")
            params.append(f"%{escaped}%")

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        # Use the latest snapshot per (metric_name, metric_type, labels) — values
        # in app.metrics are cumulative, so SUM/AVG would double-count. The
        # ROW_NUMBER() window picks the most recent row; snapshot_count is
        # informational only.
        try:
            rows = db.execute(
                f"""
                SELECT metric_name, metric_type, labels,
                       value AS current_value,
                       snapshot_count,
                       last_recorded
                FROM (
                    SELECT metric_name, metric_type, labels, value,
                           COUNT(*) OVER (
                               PARTITION BY metric_name, metric_type, labels
                           ) AS snapshot_count,
                           MAX(recorded_at) OVER (
                               PARTITION BY metric_name, metric_type, labels
                           ) AS last_recorded,
                           ROW_NUMBER() OVER (
                               PARTITION BY metric_name, metric_type, labels
                               ORDER BY recorded_at DESC
                           ) AS rn
                    FROM app.metrics
                    {where_sql}
                )
                WHERE rn = 1
                ORDER BY metric_name
                """,  # noqa: S608 — where_sql is built from validated fragments
                params if params else None,
            ).fetchall()
        except Exception:  # noqa: BLE001 — app.metrics table may not exist yet
            logger.debug("Failed to query app.metrics", exc_info=True)
            rows = []

        if output == OutputFormat.JSON:
            result = {
                "metrics": [
                    {
                        "name": row[0],
                        "type": row[1],
                        "labels": row[2],
                        "value": row[3],
                        "snapshots": row[4],
                        "last_recorded": row[5].isoformat() if row[5] else None,
                    }
                    for row in rows
                ]
            }
            typer.echo(json.dumps(result, indent=2))
            return

        if not rows:
            if not quiet:
                typer.echo("No metrics recorded yet. Run some operations first.")
            return

        for row in rows:
            name, metric_type, _labels, value, count, _last = row
            display_name = name.replace("moneybin_", "").replace("_", " ").title()
            if metric_type == "counter":
                typer.echo(f"{display_name}: {value:,.0f} total")
            elif metric_type == "gauge":
                typer.echo(f"{display_name}: {value:.2f}")
            elif metric_type == "histogram":
                typer.echo(f"{display_name}: {count} observations (sum={value:.2f}s)")
