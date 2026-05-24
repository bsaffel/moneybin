"""Run a report: execute the runner's query, classify, redact, summarize.

The generic execution path shared by the generated MCP tool and CLI command.
It mirrors ``execute_sql_query`` — same ``redact_records`` /
``derive_query_tier`` bottleneck — but the SQL comes from a report runner and
the per-column classes come from the report's view (see ``classify``) rather
than live lineage on a user query.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from moneybin.database import Database
from moneybin.privacy.redaction import redact_records
from moneybin.privacy.sql_lineage import derive_query_tier
from moneybin.privacy.taxonomy import DataClass, Tier
from moneybin.reports._framework.classify import classify_columns
from moneybin.reports._framework.contract import ReportSpec


@dataclass(frozen=True)
class ReportResult:
    """Redacted rows plus the envelope-relevant metadata for one report call.

    Mirrors the envelope-facing fields of ``SqlQueryResult`` so the MCP and CLI
    registrars build identical envelopes to the SQL surface.
    """

    records: list[dict[str, Any]]
    columns: list[str]
    output_classes: dict[str, DataClass]
    tier: Tier
    total_count: int
    truncated: bool

    @property
    def classes_returned(self) -> list[str]:
        """Sorted data-class values for the envelope/audit."""
        if not self.output_classes:
            return ["aggregate"]
        return sorted({c.value for c in self.output_classes.values()})


def run_report(
    spec: ReportSpec, db: Database, *, max_rows: int, **params: Any
) -> ReportResult:
    """Execute ``spec``'s runner with ``params`` and return redacted results.

    Fetches one extra row to detect truncation, classifies each output column
    via the report's view, and masks CRITICAL columns before returning.
    """
    rq = spec.runner(db, **params)
    cursor = db.execute(rq.sql, list(rq.params))
    columns = [d[0] for d in cursor.description] if cursor.description else []
    rows = cursor.fetchmany(max_rows + 1)
    truncated = len(rows) > max_rows
    records = [dict(zip(columns, r, strict=False)) for r in rows[:max_rows]]

    col_classes = classify_columns(db, spec.view, columns)
    redacted = redact_records(records, col_classes, consent=None)

    return ReportResult(
        records=redacted,
        columns=columns,
        output_classes=col_classes,
        tier=derive_query_tier(col_classes),
        # total_count > returned makes has_more true downstream; +1 means "at
        # least one more row" without paying for an exact COUNT(*).
        total_count=max_rows + 1 if truncated else len(records),
        truncated=truncated,
    )
