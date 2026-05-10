"""Reports namespace tools — v2 per docs/specs/mcp-tool-surface.md.

Cross-domain analytical views (read-only). All tools read directly from the
``reports.*`` SQLMesh views; the views are the canonical surface for
report-shaped data (see ``docs/specs/reports-recipe-library.md``).

Read tools:
  - reports_networth_get (medium)
  - reports_networth_history_get (medium)
  - reports_spending_get (medium)
  - reports_cashflow_get (medium)
  - reports_recurring_get (medium)
  - reports_merchants_get (medium)
  - reports_uncategorized_get (medium)
  - reports_large_transactions_get (medium)
  - reports_balance_drift_get (medium)
  - reports_budget_status (low)

All tools delegate to services or to the ``reports.*`` views — no business
logic here.
"""

from __future__ import annotations

from datetime import date as _date

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.budget_service import BudgetService
from moneybin.services.networth_service import NetworthService


@mcp_tool(sensitivity="medium")
def reports_networth_get(
    as_of_date: str | None = None,
    account_ids: list[str] | None = None,
) -> ResponseEnvelope:
    """Current or as-of net worth snapshot with per-account breakdown.

    Net worth = sum of balances across accounts where include_in_net_worth=True
    AND archived=False. Excluded/archived accounts do not contribute.

    Args:
        as_of_date: ISO date (YYYY-MM-DD); shows networth on or before this
            date. Default: latest available.
        account_ids: Filter the per-account breakdown to specific account IDs.
            The headline net_worth total still reflects all included accounts.
    """
    parsed_date = _date.fromisoformat(as_of_date) if as_of_date else None
    snapshot = NetworthService(get_database()).current(
        as_of_date=parsed_date, account_ids=account_ids
    )
    return build_envelope(data=snapshot.to_dict(), sensitivity="medium")


@mcp_tool(sensitivity="medium")
def reports_networth_history_get(
    from_date: str,
    to_date: str,
    interval: str = "monthly",
) -> ResponseEnvelope:
    """Net worth history time series with period-over-period change.

    Args:
        from_date: ISO date (YYYY-MM-DD); inclusive start
        to_date: ISO date (YYYY-MM-DD); inclusive end
        interval: 'daily' | 'weekly' | 'monthly' (default: monthly)

    Returns a list of {period, net_worth, change_abs, change_pct} dicts.
    The first period has change_abs=None and change_pct=None (no prior period).
    """
    parsed_from = _date.fromisoformat(from_date)
    parsed_to = _date.fromisoformat(to_date)
    rows = NetworthService(get_database()).history(
        parsed_from, parsed_to, interval=interval
    )
    return build_envelope(data=rows, sensitivity="medium")


@mcp_tool(sensitivity="medium")
def reports_spending_get(
    from_month: str | None = None,
    to_month: str | None = None,
    category: str | None = None,
    compare: str = "yoy",
) -> ResponseEnvelope:
    """Monthly spending trend with MoM, YoY, and 3-month-trailing deltas.

    Args:
        from_month: ISO date YYYY-MM-01; lower bound (inclusive).
        to_month: ISO date YYYY-MM-01; upper bound (inclusive).
        category: Filter to a specific category text. None returns all.
        compare: yoy | mom | trailing — which comparison column the consumer
            cares about. The view returns all three; this parameter is
            advisory metadata.
    """
    if compare not in ("yoy", "mom", "trailing"):
        raise ValueError(f"Unknown compare: {compare}")
    db = get_database()
    sql = """
        SELECT year_month, category, total_spend, txn_count,
               prev_month_spend, mom_delta, mom_pct,
               prev_year_spend, yoy_delta, yoy_pct,
               trailing_3mo_avg
        FROM reports.spending_trend
        WHERE 1=1
    """
    params: list[object] = []
    if from_month:
        sql += " AND year_month >= ?"
        params.append(from_month)
    if to_month:
        sql += " AND year_month <= ?"
        params.append(to_month)
    if category:
        sql += " AND category = ?"
        params.append(category)
    sql += " ORDER BY year_month, total_spend DESC"
    cursor = db.execute(sql, params)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description] if cursor.description else []
    return build_envelope(
        data=[dict(zip(cols, r, strict=False)) for r in rows],
        sensitivity="medium",
    )


@mcp_tool(sensitivity="medium")
def reports_cashflow_get(
    from_month: str | None = None,
    to_month: str | None = None,
    by: str = "account-and-category",
) -> ResponseEnvelope:
    """Monthly cash flow rollup: inflow/outflow/net per account x category.

    Args:
        from_month: ISO date YYYY-MM-01; lower bound (inclusive).
        to_month: ISO date YYYY-MM-01; upper bound (inclusive).
        by: account | category | account-and-category — how to group.
    """
    if by not in ("account", "category", "account-and-category"):
        raise ValueError(f"Unknown by: {by}")
    db = get_database()
    select_cols = "year_month"
    group_cols = "year_month"
    if "account" in by:
        select_cols += ", account_name"
        group_cols += ", account_name"
    if "category" in by:
        select_cols += ", category"
        group_cols += ", category"
    sql = f"""
        SELECT {select_cols},
               SUM(inflow) AS inflow,
               SUM(outflow) AS outflow,
               SUM(net) AS net,
               SUM(txn_count) AS txn_count
        FROM reports.cash_flow
        WHERE 1=1
    """  # noqa: S608  # select_cols allowlist
    params: list[object] = []
    if from_month:
        sql += " AND year_month >= ?"
        params.append(from_month)
    if to_month:
        sql += " AND year_month <= ?"
        params.append(to_month)
    sql += f" GROUP BY {group_cols} ORDER BY year_month"  # noqa: S608  # group_cols allowlist
    cursor = db.execute(sql, params)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description] if cursor.description else []
    return build_envelope(
        data=[dict(zip(cols, r, strict=False)) for r in rows],
        sensitivity="medium",
    )


@mcp_tool(sensitivity="medium")
def reports_recurring_get(
    min_confidence: float = 0.5,
    status: str = "active",
    cadence: str | None = None,
) -> ResponseEnvelope:
    """Likely-recurring subscription candidates with confidence scores.

    Args:
        min_confidence: 0.0-1.0; filter to candidates >= threshold.
        status: active | inactive | all.
        cadence: weekly | biweekly | monthly | quarterly | yearly | irregular
            (None returns all).
    """
    if status not in ("active", "inactive", "all"):
        raise ValueError(f"Unknown status: {status}")
    if cadence is not None and cadence not in (
        "weekly",
        "biweekly",
        "monthly",
        "quarterly",
        "yearly",
        "irregular",
    ):
        raise ValueError(f"Unknown cadence: {cadence}")
    db = get_database()
    sql = """
        SELECT merchant_normalized, cadence, avg_amount, occurrence_count,
               first_seen, last_seen, status, annualized_cost, confidence
        FROM reports.recurring_subscriptions
        WHERE confidence >= ?
    """
    params: list[object] = [min_confidence]
    if status != "all":
        sql += " AND status = ?"
        params.append(status)
    if cadence:
        sql += " AND cadence = ?"
        params.append(cadence)
    sql += " ORDER BY annualized_cost DESC NULLS LAST"
    cursor = db.execute(sql, params)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description] if cursor.description else []
    return build_envelope(
        data=[dict(zip(cols, r, strict=False)) for r in rows],
        sensitivity="medium",
    )


@mcp_tool(sensitivity="medium")
def reports_merchants_get(
    top: int = 25,
    sort: str = "spend",
) -> ResponseEnvelope:
    """Per-merchant lifetime activity totals.

    Args:
        top: limit rows.
        sort: spend | count | recent.
    """
    sort_keys = {
        "spend": "total_spend DESC",
        "count": "txn_count DESC",
        "recent": "last_seen DESC",
    }
    if sort not in sort_keys:
        raise ValueError(f"Unknown sort: {sort}")
    db = get_database()
    sql = f"""
        SELECT merchant_normalized, total_spend, total_inflow, total_outflow,
               txn_count, avg_amount, median_amount, first_seen, last_seen,
               active_months, top_category, account_count
        FROM reports.merchant_activity
        ORDER BY {sort_keys[sort]}
        LIMIT ?
    """  # noqa: S608  # sort_keys allowlist
    cursor = db.execute(sql, [top])
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description] if cursor.description else []
    return build_envelope(
        data=[dict(zip(cols, r, strict=False)) for r in rows],
        sensitivity="medium",
    )


@mcp_tool(sensitivity="medium")
def reports_uncategorized_get(
    min_amount: float = 0.0,
    account: str | None = None,
    limit: int = 50,
) -> ResponseEnvelope:
    """Uncategorized transactions queue, ranked by curator-impact.

    Args:
        min_amount: filter to ABS(amount) >= this.
        account: filter to account name; None for all accounts.
        limit: max rows.
    """
    db = get_database()
    sql = """
        SELECT transaction_id, account_id, account_name, txn_date, amount,
               description, merchant_normalized, age_days, priority_score
        FROM reports.uncategorized_queue
        WHERE ABS(amount) >= ?
    """
    params: list[object] = [min_amount]
    if account:
        sql += " AND account_name = ?"
        params.append(account)
    sql += " ORDER BY priority_score DESC LIMIT ?"
    params.append(limit)
    cursor = db.execute(sql, params)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description] if cursor.description else []
    return build_envelope(
        data=[dict(zip(cols, r, strict=False)) for r in rows],
        sensitivity="medium",
    )


@mcp_tool(sensitivity="medium")
def reports_large_transactions_get(
    top: int = 25,
    anomaly: str = "none",
) -> ResponseEnvelope:
    """Anomaly-flavored transaction lens (top-N + per-account/category z-scores).

    Args:
        top: top N by ABS(amount).
        anomaly: account | category | none — filter to z>2.5 in the named scope.
    """
    if anomaly not in ("none", "account", "category"):
        raise ValueError(f"Unknown anomaly: {anomaly}")
    db = get_database()
    sql = """
        SELECT transaction_id, account_name, txn_date, amount, description,
               merchant_normalized, category, amount_zscore_account,
               amount_zscore_category, is_top_100
        FROM reports.large_transactions
    """
    params: list[object] = []
    if anomaly == "account":
        sql += " WHERE amount_zscore_account > 2.5"
    elif anomaly == "category":
        sql += " WHERE amount_zscore_category > 2.5"
    sql += " ORDER BY ABS(amount) DESC LIMIT ?"
    params.append(top)
    cursor = db.execute(sql, params)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description] if cursor.description else []
    return build_envelope(
        data=[dict(zip(cols, r, strict=False)) for r in rows],
        sensitivity="medium",
    )


@mcp_tool(sensitivity="medium")
def reports_balance_drift_get(
    account: str | None = None,
    status: str = "all",
    since: str | None = None,
) -> ResponseEnvelope:
    """Balance reconciliation drift: asserted vs computed.

    Args:
        account: filter to account name; None for all.
        status: drift | warning | clean | no-data | all.
        since: ISO date; only assertions on or after.
    """
    if status not in ("drift", "warning", "clean", "no-data", "all"):
        raise ValueError(f"Unknown status: {status}")
    db = get_database()
    sql = """
        SELECT account_id, account_name, assertion_date, asserted_balance,
               computed_balance, drift, drift_abs, drift_pct,
               days_since_assertion, status
        FROM reports.balance_drift
        WHERE 1=1
    """
    params: list[object] = []
    if account:
        sql += " AND account_name = ?"
        params.append(account)
    if status != "all":
        sql += " AND status = ?"
        params.append(status)
    if since:
        sql += " AND assertion_date >= ?"
        params.append(since)
    sql += " ORDER BY drift_abs DESC"
    cursor = db.execute(sql, params)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description] if cursor.description else []
    return build_envelope(
        data=[dict(zip(cols, r, strict=False)) for r in rows],
        sensitivity="medium",
    )


@mcp_tool(sensitivity="low", domain="budget")
def reports_budget_status(
    month: str | None = None,
) -> ResponseEnvelope:
    """Get budget vs actual spending comparison for a month.

    Shows each budgeted category with its target, actual spending,
    remaining amount, and status (OK / WARNING / OVER).

    Args:
        month: Month to check (YYYY-MM). Defaults to current month.
    """
    service = BudgetService(get_database())
    result = service.status(month=month)
    return result.to_envelope()


def register_reports_tools(mcp: FastMCP) -> None:
    """Register all reports namespace tools with the FastMCP server."""
    register(
        mcp,
        reports_networth_get,
        "reports_networth_get",
        "Current or historical net worth snapshot with per-account breakdown.",
    )
    register(
        mcp,
        reports_networth_history_get,
        "reports_networth_history_get",
        "Net worth time series with period-over-period change (daily/weekly/monthly).",
    )
    register(
        mcp,
        reports_spending_get,
        "reports_spending_get",
        "Monthly spending trend per category with MoM, YoY, and trailing-3mo deltas. "
        "Reads from reports.spending_trend.",
    )
    register(
        mcp,
        reports_cashflow_get,
        "reports_cashflow_get",
        "Monthly cash flow rollup: inflow/outflow/net per account x category. "
        "Reads from reports.cash_flow.",
    )
    register(
        mcp,
        reports_recurring_get,
        "reports_recurring_get",
        "Likely-recurring subscription candidates with confidence scores and "
        "annualized cost. Reads from reports.recurring_subscriptions.",
    )
    register(
        mcp,
        reports_merchants_get,
        "reports_merchants_get",
        "Per-merchant lifetime activity totals (spend, count, first/last seen, "
        "top category). Reads from reports.merchant_activity.",
    )
    register(
        mcp,
        reports_uncategorized_get,
        "reports_uncategorized_get",
        "Uncategorized transactions queue, ranked by curator-impact "
        "(amount x age). Reads from reports.uncategorized_queue.",
    )
    register(
        mcp,
        reports_large_transactions_get,
        "reports_large_transactions_get",
        "Top transactions by absolute amount with per-account and per-category "
        "z-scores for anomaly filtering. Reads from reports.large_transactions.",
    )
    register(
        mcp,
        reports_balance_drift_get,
        "reports_balance_drift_get",
        "Balance reconciliation drift: asserted vs computed per assertion date. "
        "Reads from reports.balance_drift.",
    )
    register(
        mcp,
        reports_budget_status,
        "reports_budget_status",
        "Get budget vs actual spending comparison for a month. "
        "Shows target, spent, remaining, and status for each category.",
    )
