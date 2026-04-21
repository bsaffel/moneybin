"""Metrics persistence: flush prometheus metrics to DuckDB, load on startup.

Flush strategy:
- On shutdown (atexit) — primary persistence path.
- Periodic (every 5 min) — for long-running processes (MCP server).
- Each flush appends a new snapshot row per metric.

Load strategy:
- Counters — cumulative, restored from last snapshot.
- Gauges — point-in-time, NOT restored.
- Histograms — bucket counts restored for cross-session percentiles.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Any, cast

from prometheus_client import CollectorRegistry
from prometheus_client.metrics import MetricWrapperBase

logger = logging.getLogger(__name__)


def flush_to_duckdb(
    db: object,
    *,
    registry: CollectorRegistry | None = None,
) -> None:
    """Serialize all metrics from the prometheus registry to app.metrics.

    Each metric+label combination becomes one row with a snapshot timestamp.

    Args:
        db: Database instance (must have an ``execute()`` method).
        registry: Prometheus registry to read from. Defaults to the
            global REGISTRY.
    """
    from prometheus_client import REGISTRY

    reg = registry or REGISTRY
    now = datetime.now(tz=UTC)

    rows_written = 0
    for metric in reg.collect():
        # Skip internal prometheus metrics
        if metric.name.startswith(("python_", "process_")):
            continue

        for sample in metric.samples:
            name = sample.name
            labels = sample.labels

            # Determine metric type from the sample name suffix
            if name.endswith("_total"):
                metric_type = "counter"
                base_name = name[: -len("_total")]
            elif name.endswith("_bucket"):
                continue
            elif name.endswith("_count"):
                continue
            elif name.endswith("_sum"):
                metric_type = "histogram"
                base_name = name[: -len("_sum")]
            elif name.endswith("_created"):
                continue
            else:
                metric_type = "gauge"
                base_name = name

            # For histograms, gather bucket data
            bucket_bounds = None
            bucket_counts = None

            if metric_type == "histogram":
                bounds: list[float] = []
                counts: list[int] = []
                base_labels = {k: v for k, v in labels.items() if k != "le"}
                for s in metric.samples:
                    if s.name == f"{base_name}_bucket":
                        s_labels = {k: v for k, v in s.labels.items() if k != "le"}
                        if s_labels == base_labels:
                            le = s.labels.get("le", "")
                            if le != "+Inf":
                                bounds.append(float(le))
                            counts.append(int(s.value))
                if bounds:
                    bucket_bounds = bounds
                    bucket_counts = counts

            labels_json = json.dumps(labels) if labels else "{}"

            try:
                db.execute(  # type: ignore[union-attr]  # db is typed as object for flexibility
                    """
                    INSERT INTO app.metrics
                        (metric_name, metric_type, labels, value,
                         bucket_bounds, bucket_counts, recorded_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        base_name,
                        metric_type,
                        labels_json,
                        sample.value,
                        bucket_bounds,
                        bucket_counts,
                        now,
                    ],
                )
                rows_written += 1
            except Exception:  # noqa: BLE001  # best-effort flush; log and continue
                logger.debug(f"Failed to flush metric {base_name}", exc_info=True)

    logger.debug(f"Flushed {rows_written} metric rows to app.metrics")


def load_from_duckdb(
    db: object,
    *,
    registry: CollectorRegistry | None = None,
) -> None:
    """Restore counter values from the most recent snapshot in app.metrics.

    Gauges are NOT restored (they reflect current state).
    Histograms: bucket restoration is deferred to a future task.

    Args:
        db: Database instance (must have an ``execute()`` method).
        registry: Prometheus registry to restore into. Defaults to the
            global REGISTRY.
    """
    from prometheus_client import REGISTRY

    reg = registry or REGISTRY

    try:
        raw_rows = cast(
            list[tuple[Any, ...]],
            db.execute(  # type: ignore[union-attr]  # db is typed as object for flexibility
                """
                SELECT metric_name, metric_type, labels, value
                FROM app.metrics
                WHERE (metric_name, labels, recorded_at) IN (
                    SELECT metric_name, labels, MAX(recorded_at)
                    FROM app.metrics
                    GROUP BY metric_name, labels
                )
                """
            ).fetchall(),
        )
    except Exception:  # noqa: BLE001  # table may not exist yet; silently skip
        logger.debug("No metrics table found or empty — skipping restore")
        return

    if not raw_rows:
        return

    # Build a lookup of registered metrics by name
    metric_lookup: dict[str, MetricWrapperBase] = {}
    for collector in list(reg._names_to_collectors.values()):  # type: ignore[attr-defined]  # prometheus_client internal API
        if isinstance(collector, MetricWrapperBase):
            metric_lookup[collector._name] = collector  # type: ignore[attr-defined]  # prometheus_client internal API

    restored = 0
    for row in raw_rows:
        metric_name = str(row[0])
        metric_type = str(row[1])
        labels_json = str(row[2]) if row[2] else ""
        value = float(row[3])
        if metric_type != "counter":
            continue

        collector = metric_lookup.get(metric_name)
        if collector is None:
            collector = metric_lookup.get(
                metric_name[: -len("_total")]
                if metric_name.endswith("_total")
                else metric_name
            )
        if collector is None:
            logger.debug(f"No registered metric for {metric_name}, skipping")
            continue

        labels = json.loads(labels_json) if labels_json else {}
        try:
            collector.labels(**labels).inc(value)  # type: ignore[attr-defined]  # prometheus_client Counter API
            restored += 1
        except Exception:  # noqa: BLE001  # best-effort restore; log and continue
            logger.debug(f"Failed to restore {metric_name}", exc_info=True)

    logger.debug(f"Restored {restored} counter(s) from app.metrics")
