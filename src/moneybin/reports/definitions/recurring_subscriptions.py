"""reports_recurring / `reports recurring` — likely-recurring subscriptions."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    report,
)
from moneybin.reports.definitions._shared import RECURRING_CADENCES, RECURRING_STATUSES
from moneybin.tables import REPORTS_RECURRING_SUBSCRIPTIONS


@report(
    report_id="core:recurring",
    name="recurring",
    view=REPORTS_RECURRING_SUBSCRIPTIONS,
    classes={
        "merchant_id": DataClass.RECORD_ID,
        "merchant_normalized": DataClass.MERCHANT_NAME,
        "avg_amount": DataClass.TXN_AMOUNT,
        "cadence": DataClass.TXN_TYPE,
        "interval_days_avg": DataClass.AGGREGATE,
        "interval_days_stddev": DataClass.AGGREGATE,
        "occurrence_count": DataClass.AGGREGATE,
        "first_seen": DataClass.TXN_DATE,
        "last_seen": DataClass.TXN_DATE,
        "status": DataClass.TXN_TYPE,
        "annualized_cost": DataClass.TXN_AMOUNT,
        "confidence": DataClass.AGGREGATE,
    },
    parameter_classes={
        "min_confidence": DataClass.AGGREGATE,
        "status": DataClass.TXN_TYPE,
        "cadence": DataClass.TXN_TYPE,
    },
    columns=(
        OutputColumn(
            "merchant_id", "Canonical merchant identifier.", DataClass.RECORD_ID
        ),
        OutputColumn(
            "merchant_normalized",
            "Canonical merchant label or uncategorized bucket.",
            DataClass.MERCHANT_NAME,
        ),
        OutputColumn(
            "avg_amount", "Mean absolute recurring charge.", DataClass.TXN_AMOUNT
        ),
        OutputColumn("cadence", "Inferred recurrence cadence.", DataClass.TXN_TYPE),
        OutputColumn(
            "interval_days_avg",
            "Mean days between consecutive charges.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "interval_days_stddev",
            "Standard deviation of days between charges.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "occurrence_count",
            "Matching charge count in the observation window.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "first_seen", "Earliest matching charge date.", DataClass.TXN_DATE
        ),
        OutputColumn("last_seen", "Latest matching charge date.", DataClass.TXN_DATE),
        OutputColumn(
            "status", "Active or inactive recurrence status.", DataClass.TXN_TYPE
        ),
        OutputColumn(
            "annualized_cost",
            "Estimated yearly cost from inferred cadence.",
            DataClass.TXN_AMOUNT,
        ),
        OutputColumn(
            "confidence", "Recurrence confidence from 0 to 1.", DataClass.AGGREGATE
        ),
    ),
    semantics=ReportSemantics(
        unit="currency",
        currency="summary.display_currency",
        sign="cost amounts are positive absolute outflows",
        kind="flow",
        valuation_basis="mean observed transaction amount annualized by inferred cadence",
        fx_basis="no FX conversion in v1; assumes single-currency inputs",
        time_basis="inclusive rolling 18-month period ending on current date",
        denominator=(
            "six occurrences and fourteen days of interval variation scale confidence"
        ),
        comparison_window="inter-arrival intervals within the rolling 18-month period",
        exclusions=(
            "transfers",
            "archived accounts",
            "non-outflows",
            "clusters with fewer than three occurrences",
        ),
        provenance=("reports.recurring_subscriptions",),
    ),
)
def recurring_subscriptions(
    db: Database,  # noqa: ARG001  # contract handle; this runner builds pure SQL
    *,
    min_confidence: float = 0.5,
    status: str = "active",
    cadence: str | None = None,
) -> ReportQuery:
    """Likely-recurring subscription candidates with confidence scores.

    Average and annualized costs are positive absolute outflows in the currency
    named by summary.display_currency.

    Args:
        db: Open read-only database connection.
        min_confidence: 0.0-1.0; filter to candidates >= threshold.
        status: active | inactive | all.
        cadence: weekly | biweekly | monthly | quarterly | yearly | irregular
            (None returns all).

    Examples:
        reports(report_id="core:recurring", parameters={"min_confidence": 0.7})
        reports(report_id="core:recurring", parameters={"cadence": "monthly", "status": "all"})
    """
    if status not in RECURRING_STATUSES:
        raise ValueError(f"Unknown status: {status}")
    if cadence is not None and cadence not in RECURRING_CADENCES:
        raise ValueError(f"Unknown cadence: {cadence}")
    # Out-of-range confidence (e.g. percentage-thinking 85.0, or 1.5) would
    # silently return an empty set — indistinguishable from "none found".
    if not 0.0 <= min_confidence <= 1.0:
        raise ValueError(
            f"min_confidence must be in [0.0, 1.0], got {min_confidence!r}"
        )

    sql = f"""
        SELECT merchant_id, merchant_normalized, cadence, avg_amount,
               interval_days_avg, interval_days_stddev,
               occurrence_count, first_seen, last_seen, status,
               annualized_cost, confidence
        FROM {REPORTS_RECURRING_SUBSCRIPTIONS.full_name}
        WHERE confidence >= ?
    """  # noqa: S608  # TableRef interpolation
    params: list[object] = [min_confidence]
    if status != "all":
        sql += " AND status = ?"
        params.append(status)
    if cadence:
        sql += " AND cadence = ?"
        params.append(cadence)
    sql += " ORDER BY annualized_cost DESC NULLS LAST"
    return ReportQuery(sql, params)
