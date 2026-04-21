"""Stats command for MoneyBin CLI.

Displays lifetime metric aggregates from the app.metrics table.
"""

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Annotated

import typer

from moneybin.database import DatabaseKeyError, get_database

logger = logging.getLogger(__name__)

app = typer.Typer(
    help="Show lifetime metric aggregates",
    no_args_is_help=True,
)


def _parse_duration(duration: str) -> timedelta:
    """Parse a duration string like '30d', '7d', '24h' into a timedelta.

    Args:
        duration: Duration string (e.g., "30d", "7d", "24h", "60m").

    Returns:
        timedelta for the specified duration.

    Raises:
        ValueError: If format is invalid.
    """
    match = re.match(r"^(\d+)([dhm])$", duration.strip())
    if not match:
        raise ValueError(
            f"Invalid duration format: '{duration}'. Use <number><unit> "
            "where unit is d (days), h (hours), or m (minutes)."
        )
    value = int(match.group(1))
    unit = match.group(2)
    if unit == "d":
        return timedelta(days=value)
    elif unit == "h":
        return timedelta(hours=value)
    else:
        return timedelta(minutes=value)


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
        str,
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
            delta = _parse_duration(since)
        except ValueError as e:
            logger.error(f"❌ {e}")
            raise typer.Exit(1) from e
        cutoff = datetime.now() - delta
        where_clauses.append("recorded_at >= ?")
        params.append(cutoff)

    if metric:
        where_clauses.append("metric_name LIKE ?")
        params.append(f"%{metric}%")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    try:
        rows = db.execute(
            f"""
            SELECT metric_name, metric_type,
                   SUM(value) as total_value,
                   COUNT(*) as snapshot_count,
                   MAX(recorded_at) as last_recorded
            FROM app.metrics
            {where_sql}
            GROUP BY metric_name, metric_type
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
                    "value": row[2],
                    "snapshots": row[3],
                    "last_recorded": row[4].isoformat() if row[4] else None,
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
        name, metric_type, value, count, _last = row
        display_name = name.replace("moneybin_", "").replace("_", " ").title()
        if metric_type == "counter":
            logger.info(f"{display_name}: {value:,.0f} total")
        elif metric_type == "gauge":
            logger.info(f"{display_name}: {value:.2f}")
        elif metric_type == "histogram":
            logger.info(f"{display_name}: {count} observations (sum={value:.2f}s)")
