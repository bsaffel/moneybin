"""Stats command for MoneyBin CLI.

Displays lifetime metric aggregates from the app.metrics table.
"""

import json
import logging
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
from itertools import groupby
from typing import Annotated, Any

import typer

from moneybin.cli.output import OutputFormat, output_option, quiet_option
from moneybin.cli.render import render_summary
from moneybin.cli.utils import handle_cli_errors
from moneybin.database import get_database
from moneybin.tables import METRICS
from moneybin.utils.parsing import parse_duration

logger = logging.getLogger(__name__)


# The leading words `str.title()` gets wrong. AGENTS.md writes acronyms in
# caps, and a line reading `Mcp` under the MCP header is the one place a user
# sees this word.
_ACRONYM_CASING = {
    "cli": "CLI",
    "db": "DB",
    "fx": "FX",
    "mcp": "MCP",
    "ofx": "OFX",
    "pdf": "PDF",
    "sqlmesh": "SQLMesh",
}

# Where a metric prints when the registry declares no domain for it: a name
# `app.metrics` kept from a version that has since renamed or dropped it.
_UNDECLARED_DOMAIN = "Other"


def _leading_word(metric_name: str) -> str:
    """The first word after the ``moneybin_`` namespace, cased for a reader."""
    word = metric_name.removeprefix("moneybin_").split("_", 1)[0]
    return _ACRONYM_CASING.get(word, word.title())


def _domain(metric_name: str, domains: Mapping[str, str]) -> str:
    """The subsystem heading a metric groups under (requirement 25).

    Read from the registry's declaration rather than split off the name: a
    subsystem declares metrics under several prefixes, so the leading word
    fragments one of them across headers. `registry.py` carries the why.
    """
    return domains.get(metric_name, _UNDECLARED_DOMAIN)


def _label(metric_name: str, labels_json: str) -> str:
    """The metric's name plus whatever distinguishes this row (requirement 23).

    ``app.metrics`` partitions on the label set, so one metric is as many rows
    as it has label combinations. Printing the name alone renders those as the
    same words repeated against different numbers, which reads as a display
    bug rather than as two dimensions of one measurement.
    """
    # The leading word takes its casing from `_ACRONYM_CASING` rather than
    # from a second `.title()` — otherwise the MCP header sits above a line
    # that reads `Mcp`.
    _, _, rest = metric_name.removeprefix("moneybin_").partition("_")
    display = _leading_word(metric_name)
    if rest:
        display = f"{display} {rest.replace('_', ' ').title()}"
    try:
        labels: dict[str, str] = json.loads(labels_json) if labels_json else {}
    except json.JSONDecodeError:
        # A row whose labels column holds something this version cannot parse
        # still has a name and a value worth printing. The dimension is what is
        # lost, not the line.
        logger.debug(f"Unparseable labels for {metric_name}", exc_info=True)
        return display
    if not labels:
        return display
    # Requirement 16 keeps identifier-shaped copy away from a reader, and
    # requirement 1 calls `key=value` a table spelled badly. The label *name*
    # is renderer-authored copy and gets spelled as prose; the label *value* is
    # the recorded dimension and prints as stored, the way a table cell does.
    dimensions = ", ".join(
        f"{key.replace('_', ' ')}: {value}" for key, value in sorted(labels.items())
    )
    return f"{display} ({dimensions})"


def _value(
    metric_name: str,
    metric_type: str,
    value: float,
    snapshots: int,
    units: Mapping[str, str],
) -> str:
    """The right-hand side of one summary line.

    ``snapshots`` counts persisted rows for this series, not observations —
    ``app.metrics`` stores a running total per flush, so the number of rows is
    how many times the metric was written, and calling them observations
    overstated what the figure knows.
    """
    if metric_type == "counter":
        return f"{value:,.0f} total"
    if metric_type == "histogram":
        # Requirement 24: declared, never derived — a metric that is not a
        # duration must not render `s`. An undeclared name is one persisted by
        # an older version and since renamed or dropped; it keeps its sum.
        total = f"{value:.2f} {units.get(metric_name, '')}".rstrip()
        return f"{snapshots} snapshots (sum={total})"
    return f"{value:.2f}"


def _grouped(
    rows: Sequence[Sequence[Any]],
    units: Mapping[str, str],
    domains: Mapping[str, str],
) -> Iterable[tuple[str, list[tuple[str, str]]]]:
    """Summary pairs per domain, in the order the registry declares them.

    The rows arrive ordered by ``metric_name``, which no longer clusters them:
    one subsystem's metrics carry several prefixes, so grouping the query's
    order straight through would reopen the same header more than once. Sorting
    by domain first also puts the blocks in the registry's own order — import
    through transform to export — rather than alphabetically by whichever name
    each block happens to start with.
    """
    order = {
        domain: rank for rank, domain in enumerate(dict.fromkeys(domains.values()))
    }
    ranked = sorted(
        rows,
        key=lambda row: (
            order.get(_domain(str(row[0]), domains), len(order)),
            str(row[0]),
        ),
    )
    for domain, group in groupby(ranked, key=lambda row: _domain(str(row[0]), domains)):
        yield (
            domain,
            [
                (
                    _label(str(row[0]), str(row[2])),
                    _value(str(row[0]), str(row[1]), float(row[3]), int(row[4]), units),
                )
                for row in group
            ],
        )


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

    with handle_cli_errors():
        with get_database(read_only=True) as db:
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
                        FROM {METRICS.full_name}
                        {where_sql}
                    )
                    WHERE rn = 1
                    ORDER BY metric_name
                    """,  # noqa: S608  # where_sql is built from validated fragments
                    params if params else None,
                ).fetchall()
            except Exception:  # app.metrics table may not exist yet
                logger.debug("Failed to query app.metrics", exc_info=True)
                rows = []

            if output == OutputFormat.JSON:
                # Not `render_or_json`: operations metadata, judged by
                # disclosure rather than by column class (security.md). The
                # checkable condition is registry-boundedness — every metric
                # name and label here is declared in `metrics/registry.py` and
                # every value is numeric, so there is no free text for a typed
                # transform to mask. A metric that ever carries a free-text
                # label voids this exemption and the command must migrate.
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
                # Left on stdout deliberately. `render_note` would be the
                # coherent move if the tree had one empty-result idiom, but it
                # has three — `transactions/list_.py:211` echoes to stdout,
                # `accounts/__init__.py:418` to stderr, `transactions/tags.py`
                # logs — so moving this one settles nothing and silently
                # changes what `moneybin stats > file` writes. Requirements
                # 23-25 are about the rendering of metrics that exist.
                if not quiet:
                    typer.echo("No metrics recorded yet. Run some operations first.")
                return

            # Deferred, matching every other CLI reference to the registry:
            # importing it at module scope pulls prometheus_client into the
            # startup path of every command, not just this one.
            from moneybin.metrics.registry import HISTOGRAM_UNITS, METRIC_DOMAINS

            for domain, pairs in _grouped(rows, HISTOGRAM_UNITS, METRIC_DOMAINS):
                render_summary(pairs, title=domain)
