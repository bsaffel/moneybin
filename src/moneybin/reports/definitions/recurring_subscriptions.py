"""reports_recurring / `reports recurring` — likely-recurring subscriptions."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.reports._framework.contract import ReportQuery, report
from moneybin.reports.definitions._shared import RECURRING_CADENCES, RECURRING_STATUSES
from moneybin.tables import REPORTS_RECURRING_SUBSCRIPTIONS


@report(name="recurring", view=REPORTS_RECURRING_SUBSCRIPTIONS)
def recurring_subscriptions(
    db: Database,  # noqa: ARG001  # contract handle; this runner builds pure SQL
    *,
    min_confidence: float = 0.5,
    status: str = "active",
    cadence: str | None = None,
) -> ReportQuery:
    """Likely-recurring subscription candidates with confidence scores.

    Amounts use the accounting convention (negative = expense, positive =
    income) in the currency named by summary.display_currency.

    Args:
        db: Open read-only database connection.
        min_confidence: 0.0-1.0; filter to candidates >= threshold.
        status: active | inactive | all.
        cadence: weekly | biweekly | monthly | quarterly | yearly | irregular
            (None returns all).

    Examples:
        reports_recurring(min_confidence=0.7)
        reports_recurring(cadence="monthly", status="all")
    """
    if status not in RECURRING_STATUSES:
        raise ValueError(f"Unknown status: {status}")
    if cadence is not None and cadence not in RECURRING_CADENCES:
        raise ValueError(f"Unknown cadence: {cadence}")

    sql = f"""
        SELECT merchant_id, merchant_normalized, cadence, avg_amount,
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
