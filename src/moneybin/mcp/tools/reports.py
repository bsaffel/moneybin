"""Reports namespace tools — v2 per docs/specs/moneybin-mcp.md.

Cross-domain analytical views (read-only). All tools delegate to a service
layer; no business logic here.

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
"""

from __future__ import annotations

from datetime import date as _date
from datetime import datetime as _datetime
from typing import Any, Literal

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.services.budget_service import BudgetService
from moneybin.services.networth_service import NetworthService
from moneybin.services.reports_service import ReportsService


def _envelope(
    cols: list[str],
    rows: list[tuple[Any, ...]],
    *,
    sensitivity: Literal["low", "medium", "high"] = "medium",
    actions: list[str] | None = None,
    period: str | None = None,
) -> ResponseEnvelope:
    """Wrap a (cols, rows) result as a response envelope at ``sensitivity``."""
    return build_envelope(
        data=[dict(zip(cols, r, strict=False)) for r in rows],
        sensitivity=sensitivity,
        actions=actions,
        period=period,
    )


def _default_window(months: int = 12) -> tuple[str, str]:
    """Return (from_month, to_month) as YYYY-MM strings covering the last N months."""
    today = _datetime.now()
    end = today.replace(day=1)
    # Walk back `months - 1` calendar months to get an inclusive N-month window.
    year = end.year
    month = end.month - (months - 1)
    while month <= 0:
        month += 12
        year -= 1
    start = end.replace(year=year, month=month)
    return start.strftime("%Y-%m"), end.strftime("%Y-%m")


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
    with get_database(read_only=True) as db:
        snapshot = NetworthService(db).current(
            as_of_date=parsed_date, account_ids=account_ids
        )
    return build_envelope(
        data=snapshot.to_dict(),
        sensitivity="medium",
        actions=[
            "Use reports_networth_history_get(from_date, to_date) for the time series",
            "Use accounts_balance_history(account_id=...) to drill into one account",
            "Use accounts_list to see archived / excluded accounts not counted here",
        ],
    )


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
    with get_database(read_only=True) as db:
        rows = NetworthService(db).history(parsed_from, parsed_to, interval=interval)
    return build_envelope(
        data=rows,
        sensitivity="medium",
        actions=[
            "Use reports_networth_get(as_of_date=...) for a single-date snapshot with per-account breakdown",
            "Switch `interval` to 'daily' or 'weekly' for finer resolution",
        ],
    )


@mcp_tool(sensitivity="low")
def reports_spending_get(
    from_month: str | None = None,
    to_month: str | None = None,
    category: str | None = None,
    compare: str = "yoy",
) -> ResponseEnvelope:
    """Monthly spending trend with MoM, YoY, and 3-month-trailing deltas.

    Defaults to the last 12 calendar months when both bounds are omitted.
    YoY columns are populated from the underlying view (which includes all
    history), not from the windowed result — narrowing the window does not
    null out yoy_pct.

    Args:
        from_month: Lower bound (inclusive) as 'YYYY-MM' (also accepts
            'YYYY-MM-DD' and ignores the day).
        to_month: Upper bound (inclusive) as 'YYYY-MM' (also accepts
            'YYYY-MM-DD' and ignores the day).
        category: Filter to a specific category text. None returns all.
        compare: yoy | mom | trailing — caller-side intent only; the view
            returns all three comparison columns regardless.
    """
    defaulted = from_month is None and to_month is None
    if defaulted:
        from_month, to_month = _default_window(months=12)
    with get_database(read_only=True) as db:
        cols, rows = ReportsService(db).spending_trend(
            from_month=from_month, to_month=to_month, category=category, compare=compare
        )
    actions = [
        "Filter to one category with category='<name>' (see categories_list)",
        "Use reports_cashflow_get for inflow/outflow/net (includes income; spending excludes it)",
        "Use reports_recurring_get to find subscription-like patterns",
    ]
    if defaulted:
        actions.insert(
            0,
            "Showing the last 12 months — pass from_month='YYYY-MM' and/or "
            "to_month='YYYY-MM' to widen or shift the window.",
        )
    return _envelope(
        cols,
        rows,
        sensitivity="low",
        actions=actions,
        period=f"{from_month} to {to_month}" if from_month and to_month else None,
    )


@mcp_tool(sensitivity="low")
def reports_cashflow_get(
    from_month: str | None = None,
    to_month: str | None = None,
    by: str = "account-and-category",
) -> ResponseEnvelope:
    """Monthly cash flow rollup: inflow/outflow/net per account x category.

    Defaults to the last 12 calendar months when both bounds are omitted.

    Args:
        from_month: Lower bound (inclusive) as 'YYYY-MM' (also accepts
            'YYYY-MM-DD' and ignores the day).
        to_month: Upper bound (inclusive) as 'YYYY-MM' (also accepts
            'YYYY-MM-DD' and ignores the day).
        by: account | category | account-and-category — how to group.
    """
    defaulted = from_month is None and to_month is None
    if defaulted:
        from_month, to_month = _default_window(months=12)
    with get_database(read_only=True) as db:
        cols, rows = ReportsService(db).cash_flow(
            from_month=from_month, to_month=to_month, by=by
        )
    actions = [
        "Switch `by` to 'account', 'category', or 'account-and-category' to regroup",
        "Use reports_spending_get for outflow-only trend with MoM/YoY deltas",
    ]
    if defaulted:
        actions.insert(
            0,
            "Showing the last 12 months — pass from_month='YYYY-MM' and/or "
            "to_month='YYYY-MM' to widen or shift the window.",
        )
    return _envelope(
        cols,
        rows,
        sensitivity="low",
        actions=actions,
        period=f"{from_month} to {to_month}" if from_month and to_month else None,
    )


@mcp_tool(sensitivity="low")
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
    with get_database(read_only=True) as db:
        cols, rows = ReportsService(db).recurring_subscriptions(
            min_confidence=min_confidence, status=status, cadence=cadence
        )
    return _envelope(cols, rows, sensitivity="low")


@mcp_tool(sensitivity="low")
def reports_merchants_get(
    top: int = 25,
    sort: str = "spend",
) -> ResponseEnvelope:
    """Per-merchant lifetime activity totals.

    Args:
        top: limit rows.
        sort: spend | count | recent.
    """
    with get_database(read_only=True) as db:
        cols, rows = ReportsService(db).merchant_activity(top=top, sort=sort)
    return _envelope(cols, rows, sensitivity="low")


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
    with get_database(read_only=True) as db:
        cols, rows = ReportsService(db).uncategorized_queue(
            min_amount=min_amount, account=account, limit=limit
        )
    return _envelope(cols, rows)


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
    with get_database(read_only=True) as db:
        cols, rows = ReportsService(db).large_transactions(top=top, anomaly=anomaly)
    return _envelope(cols, rows)


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
    with get_database(read_only=True) as db:
        cols, rows = ReportsService(db).balance_drift(
            account=account, status=status, since=since
        )
    return _envelope(cols, rows)


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
    with get_database(read_only=True) as db:
        result = BudgetService(db).status(month=month)
    return result.to_envelope()


def register_reports_tools(mcp: FastMCP) -> None:
    """Register all reports namespace tools with the FastMCP server."""
    register(
        mcp,
        reports_networth_get,
        "reports_networth_get",
        "Current or historical net worth snapshot with per-account breakdown. "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        reports_networth_history_get,
        "reports_networth_history_get",
        "Net worth time series with period-over-period change (daily/weekly/monthly). "
        "Amounts are in the currency named by `summary.display_currency`.",
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
        "Shows target, spent, remaining, and status for each category. "
        "Amounts use the accounting convention: negative = expense, positive = income; transfers exempt. "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
