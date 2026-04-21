"""Stats command for MoneyBin CLI.

Displays lifetime metric aggregates from the app.metrics table.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Annotated, Literal

import typer

from moneybin.database import DatabaseKeyError, get_database
from moneybin.utils.parsing import parse_duration

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Show lifetime metric aggregates",
    no_args_is_help=True,
)


@app.command("show")
def stats_show(
    since: Annotated[
        str | None,
        typer.Option("--since", help="Time window (e.g., 7d, 24h)"),
    ] = None,
    metric: Annotated[
        str | None,
        typer.Option("--metric", help="Filter to a metric family (e.g., import)"),
    ] = None,
    output: Annotated[
        Literal["text", "json"],
        typer.Option("--output", help="Output format: text or json"),
    ] = "text",
) -> None:
    """Display lifetime metric aggregates."""
    try:
        db = get_database()
    except DatabaseKeyError as e:
        logger.error(f"❌ Database is locked: {e}")
        typer.echo(
            "💡 Run 'moneybin db unlock' to unlock the database first.",
            err=True,
        )
        raise typer.Exit(1) from e

    # Build query with optional filters
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
        # Escape LIKE metacharacters so _ and % match literally
        escaped = metric.replace("!", "!!").replace("%", "!%").replace("_", "!_")
        where_clauses.append("metric_name LIKE ? ESCAPE '!'")
        params.append(f"%{escaped}%")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    try:
        # Use latest snapshot per metric+labels (not SUM — values are cumulative)
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
            """,  # noqa: S608 — where_sql is built from validated fragments, not user input
            params if params else None,
        ).fetchall()
    except Exception:  # noqa: BLE001 — app.metrics table may not exist yet
        rows = []

    if output == "json":
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
        typer.echo("No metrics recorded yet. Run some operations first.")
        return

    # Human-readable output
    for row in rows:
        name, metric_type, _labels, value, count, _last = row
        display_name = name.replace("moneybin_", "").replace("_", " ").title()
        if metric_type == "counter":
            typer.echo(f"{display_name}: {value:,.0f} total")
        elif metric_type == "gauge":
            typer.echo(f"{display_name}: {value:.2f}")
        elif metric_type == "histogram":
            typer.echo(f"{display_name}: {count} observations (sum={value:.2f}s)")
