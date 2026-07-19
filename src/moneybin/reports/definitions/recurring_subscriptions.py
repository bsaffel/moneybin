"""reports_recurring / `reports recurring` — likely-recurring subscriptions."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import ReportQuery, report
from moneybin.reports.definitions._shared import RECURRING_CADENCES, RECURRING_STATUSES
from moneybin.tables import REPORTS_RECURRING_SUBSCRIPTIONS


@report(
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
    # The model's LAG(...) OVER (PARTITION BY account_id, merchant_id,
    # amount_bucket ...) uses amount_bucket (ROUND(amount, 0)) purely to scope
    # which rows are compared for the SAME cadence cluster — it never flows
    # into the computed value. Derivation's referenced-column sweep can't
    # distinguish a window partition key from data flow, so it conservatively
    # inherits amount's HIGH tier for every column downstream of interval_days.
    # None of the five below touch amount's value; they're all statistics over
    # inter-transaction date GAPS (interval_days_avg/stddev), or categorical
    # summaries of those gaps (cadence, status) and a count (confidence).
    class_downgrades={
        "cadence": "categorical label from thresholding interval_days_avg/"
        "stddev (date-gap statistics); the amount tier is an artifact of "
        "the model's LAG(...) OVER (PARTITION BY ... amount_bucket ...) "
        "window using amount_bucket only to scope the partition, never "
        "as a value",
        "interval_days_avg": "mean inter-transaction gap in days, a "
        "GROUP-level aggregate over date differences — never over amount; "
        "see cadence's note on the window partition-key artifact",
        "interval_days_stddev": "stddev of inter-transaction gaps in days, "
        "same date-only aggregate as interval_days_avg",
        "status": "'active'/'inactive' from thresholding last_seen recency "
        "against interval_days_avg — a coarse boolean, and the amount tier "
        "is the same window partition-key artifact as cadence",
        "confidence": "0.0-1.0 score from occurrence_count and "
        "interval_days_stddev (both non-amount); same window "
        "partition-key artifact",
    },
)
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
