"""reports_merchants / `reports merchants` — per-merchant lifetime totals."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import ReportQuery, report
from moneybin.reports.definitions._shared import MERCHANTS_SORTS
from moneybin.tables import REPORTS_MERCHANT_ACTIVITY


@report(
    name="merchants",
    view=REPORTS_MERCHANT_ACTIVITY,
    classes={
        "merchant_id": DataClass.RECORD_ID,
        "merchant_normalized": DataClass.MERCHANT_NAME,
        "total_spend": DataClass.TXN_AMOUNT,
        "total_inflow": DataClass.TXN_AMOUNT,
        "total_outflow": DataClass.TXN_AMOUNT,
        "txn_count": DataClass.AGGREGATE,
        "avg_amount": DataClass.TXN_AMOUNT,
        "median_amount": DataClass.TXN_AMOUNT,
        "first_seen": DataClass.TXN_DATE,
        "last_seen": DataClass.TXN_DATE,
        "active_months": DataClass.AGGREGATE,
        "top_category": DataClass.CATEGORY,
        "account_count": DataClass.AGGREGATE,
    },
)
def merchant_activity(
    db: Database,  # noqa: ARG001  # contract handle; this runner builds pure SQL
    *,
    top: int = 25,
    sort: str = "spend",
) -> ReportQuery:
    """Per-merchant lifetime activity totals (spend, count, first/last seen).

    Amounts use the accounting convention (negative = expense, positive =
    income) in the currency named by summary.display_currency.

    Args:
        db: Open read-only database connection.
        top: Limit rows.
        sort: spend | count | recent.

    Examples:
        reports_merchants(top=10, sort="count")
    """
    if sort not in MERCHANTS_SORTS:
        raise ValueError(f"Unknown sort: {sort}")
    sql = f"""
        SELECT merchant_id, merchant_normalized, total_spend, total_inflow,
               total_outflow, txn_count, avg_amount, median_amount,
               first_seen, last_seen, active_months, top_category,
               account_count
        FROM {REPORTS_MERCHANT_ACTIVITY.full_name}
        ORDER BY {MERCHANTS_SORTS[sort]}
        LIMIT ?
    """  # noqa: S608  # TableRef + MERCHANTS_SORTS allowlists
    return ReportQuery(sql, [top])
