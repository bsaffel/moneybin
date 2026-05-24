"""reports_merchants / `reports merchants` — per-merchant lifetime totals."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.reports._framework.contract import ReportQuery, report
from moneybin.reports.definitions._shared import MERCHANTS_SORTS
from moneybin.tables import REPORTS_MERCHANT_ACTIVITY


@report(name="merchants", view=REPORTS_MERCHANT_ACTIVITY)
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
