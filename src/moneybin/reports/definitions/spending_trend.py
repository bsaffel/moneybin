"""reports_spending / `reports spending` — monthly spending trend with deltas."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import ReportQuery, report
from moneybin.reports.definitions._shared import SPENDING_COMPARES, resolve_window
from moneybin.tables import REPORTS_SPENDING_TREND


@report(
    name="spending",
    view=REPORTS_SPENDING_TREND,
    classes={
        "year_month": DataClass.TXN_DATE,
        "category": DataClass.CATEGORY,
        "total_spend": DataClass.TXN_AMOUNT,
        "txn_count": DataClass.AGGREGATE,
        "prev_month_spend": DataClass.TXN_AMOUNT,
        "mom_delta": DataClass.TXN_AMOUNT,
        "mom_pct": DataClass.AGGREGATE,
        "prev_year_spend": DataClass.TXN_AMOUNT,
        "yoy_delta": DataClass.TXN_AMOUNT,
        "yoy_pct": DataClass.AGGREGATE,
        "trailing_3mo_avg": DataClass.TXN_AMOUNT,
    },
)
def spending_trend(
    db: Database,  # noqa: ARG001  # contract handle; this runner builds pure SQL
    *,
    from_month: str | None = None,
    to_month: str | None = None,
    category: str | None = None,
    compare: str = "yoy",
) -> ReportQuery:
    """Monthly spending trend with MoM, YoY, and 3-month-trailing deltas.

    Defaults to the last 12 calendar months when both bounds are omitted. YoY
    columns come from the underlying view (all history), so narrowing the window
    does not null out yoy_pct. Amounts use the accounting convention (negative =
    expense, positive = income) in the currency named by summary.display_currency.

    Args:
        db: Open read-only database connection.
        from_month: Lower bound (inclusive) as 'YYYY-MM'.
        to_month: Upper bound (inclusive) as 'YYYY-MM'.
        category: Filter to a specific category text. None returns all.
        compare: yoy | mom | trailing — caller-side intent only; the view
            returns all three comparison columns regardless.

    Examples:
        reports_spending(category="Groceries")
        reports_spending(from_month="2023-01", to_month="2023-12")
    """
    # Validate so agents see the allowed values and can't pass arbitrary strings;
    # the view returns all three comparison columns regardless, so `compare` has
    # no effect on the SQL below (caller-side intent only — the raise is reachable).
    if compare not in SPENDING_COMPARES:
        raise ValueError(f"Unknown compare: {compare}")
    from_month, to_month, period, hint = resolve_window(from_month, to_month)

    sql = f"""
        SELECT year_month, category, total_spend, txn_count,
               prev_month_spend, mom_delta, mom_pct,
               prev_year_spend, yoy_delta, yoy_pct,
               trailing_3mo_avg
        FROM {REPORTS_SPENDING_TREND.full_name}
        WHERE 1=1
    """  # noqa: S608  # TableRef interpolation
    params: list[object] = []
    if from_month:
        sql += " AND year_month >= substr(?, 1, 7)"
        params.append(from_month)
    if to_month:
        sql += " AND year_month <= substr(?, 1, 7)"
        params.append(to_month)
    if category:
        sql += " AND category = ?"
        params.append(category)
    sql += " ORDER BY year_month, total_spend DESC"

    actions = [
        "Filter to one category with category='<name>' (see categories)",
        "Use reports_cashflow for inflow/outflow/net (includes income)",
        "Use reports_recurring to find subscription-like patterns",
    ]
    if hint:
        actions.insert(0, hint)
    return ReportQuery(sql, params, actions=actions, period=period)
