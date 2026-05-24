"""reports_cashflow / `reports cashflow` — monthly inflow/outflow/net rollup."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import ReportQuery, report
from moneybin.reports.definitions._shared import CASHFLOW_GROUPINGS, resolve_window
from moneybin.tables import REPORTS_CASH_FLOW


@report(
    name="cashflow",
    view=REPORTS_CASH_FLOW,
    classes={
        "year_month": DataClass.TXN_DATE,
        "account_id": DataClass.ACCOUNT_IDENTIFIER,
        "account_name": DataClass.USER_NOTE,
        "category": DataClass.CATEGORY,
        "inflow": DataClass.TXN_AMOUNT,
        "outflow": DataClass.TXN_AMOUNT,
        "net": DataClass.TXN_AMOUNT,
        "txn_count": DataClass.AGGREGATE,
    },
)
def cash_flow(
    db: Database,  # noqa: ARG001  # contract handle; this runner builds pure SQL
    *,
    from_month: str | None = None,
    to_month: str | None = None,
    by: str = "account-and-category",
) -> ReportQuery:
    """Monthly cash flow rollup: inflow/outflow/net per account x category.

    Defaults to the last 12 calendar months when both bounds are omitted.
    Amounts use the accounting convention (negative = expense, positive =
    income) in the currency named by summary.display_currency.

    Args:
        db: Open read-only database connection.
        from_month: Lower bound (inclusive) as 'YYYY-MM' (also accepts
            'YYYY-MM-DD' and ignores the day).
        to_month: Upper bound (inclusive) as 'YYYY-MM'.
        by: account | category | account-and-category — how to group.

    Examples:
        reports_cashflow(by="category", from_month="2024-01")
        reports_cashflow(by="account")
    """
    if by not in CASHFLOW_GROUPINGS:
        raise ValueError(f"Unknown by: {by}")
    from_month, to_month, period, hint = resolve_window(from_month, to_month)

    select_cols = "year_month"
    group_cols = "year_month"
    if by in ("account", "account-and-category"):
        # account_id keeps rows distinct when two accounts share a display_name.
        select_cols += ", account_id, account_name"
        group_cols += ", account_id, account_name"
    if by in ("category", "account-and-category"):
        select_cols += ", category"
        group_cols += ", category"

    sql = f"""
        SELECT {select_cols},
               SUM(inflow) AS inflow,
               SUM(outflow) AS outflow,
               SUM(net) AS net,
               SUM(txn_count) AS txn_count
        FROM {REPORTS_CASH_FLOW.full_name}
        WHERE 1=1
    """  # noqa: S608  # select_cols + TableRef allowlists
    params: list[object] = []
    if from_month:
        sql += " AND year_month >= substr(?, 1, 7)"
        params.append(from_month)
    if to_month:
        sql += " AND year_month <= substr(?, 1, 7)"
        params.append(to_month)
    sql += f" GROUP BY {group_cols} ORDER BY year_month"  # noqa: S608  # group_cols allowlist

    actions = [
        "Switch `by` to 'account', 'category', or 'account-and-category' to regroup",
        "Use reports_spending for outflow-only trend with MoM/YoY deltas",
    ]
    if hint:
        actions.insert(0, hint)
    return ReportQuery(sql, params, actions=actions, period=period)
