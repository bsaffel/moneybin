"""Reports namespace tools.

Cross-domain analytical views (read-only). The view-backed reports
(spending, cashflow, recurring, merchants, large_transactions, balance_drift)
are generated from ``@report`` runners in ``moneybin.reports.definitions`` and
registered via ``register_reports_mcp``. ``networth`` / ``networth_history``
are NetworthService-backed (not single reports.* view reads) and stay
hand-written — a documented exception. ``budget`` is hand-written pending a
``reports.budget`` view.
"""

from __future__ import annotations

from datetime import date as _date

from fastmcp import FastMCP

from moneybin.database import get_database
from moneybin.mcp._registration import register
from moneybin.mcp.decorator import mcp_tool
from moneybin.privacy.payloads.budget import BudgetStatusPayload
from moneybin.privacy.payloads.networth import (
    NetWorthHistoryPayload,
    NetWorthSnapshotPayload,
)
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.reports._framework.registry import register_reports_mcp
from moneybin.reports.definitions import ALL_REPORTS
from moneybin.services.budget_service import BudgetService
from moneybin.services.networth_service import NetworthService


@mcp_tool()
def reports_networth(
    as_of_date: str | None = None, account_ids: list[str] | None = None
) -> ResponseEnvelope[NetWorthSnapshotPayload]:
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
        data=snapshot,
        actions=[
            "Use reports_networth_history(from_date, to_date) for the time series",
            "Use accounts_balance_history(account_id=...) to drill into one account",
            "Use accounts to see archived / excluded accounts not counted here",
        ],
    )


@mcp_tool()
def reports_networth_history(
    from_date: str, to_date: str, interval: str = "monthly"
) -> ResponseEnvelope[NetWorthHistoryPayload]:
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
        payload = NetworthService(db).history(parsed_from, parsed_to, interval=interval)
    return build_envelope(
        data=payload,
        actions=[
            "Use reports_networth(as_of_date=...) for a single-date snapshot with per-account breakdown",
            "Switch `interval` to 'daily' or 'weekly' for finer resolution",
        ],
    )


@mcp_tool(domain="budget")
def reports_budget(month: str | None = None) -> ResponseEnvelope[BudgetStatusPayload]:
    """Get budget vs actual spending comparison for a month.

    Shows each budgeted category with its target, actual spending,
    remaining amount, and status (OK / WARNING / OVER).

    Args:
        month: Month to check (YYYY-MM). Defaults to current month.
    """
    with get_database(read_only=True) as db:
        payload = BudgetService(db).status(month=month)
    return build_envelope(
        data=payload,
        period=payload.month,
        actions=[
            "Use `moneybin budget set` (CLI) to adjust a budget target",
            "Use reports_spending for detailed category breakdown",
        ],
    )


def register_reports_tools(mcp: FastMCP) -> None:
    """Register all reports namespace tools with the FastMCP server.

    The view-backed reports register from ``ALL_REPORTS`` via the framework;
    the NetworthService-backed and budget tools register by hand.
    """
    register(
        mcp,
        reports_networth,
        "reports_networth",
        "Current or historical net worth snapshot with per-account breakdown. "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        reports_networth_history,
        "reports_networth_history",
        "Net worth time series with period-over-period change (daily/weekly/monthly). "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register(
        mcp,
        reports_budget,
        "reports_budget",
        "Get budget vs actual spending comparison for a month. "
        "Shows target, spent, remaining, and status for each category. "
        "Amounts use the accounting convention: negative = expense, positive = income; transfers exempt. "
        "Amounts are in the currency named by `summary.display_currency`.",
    )
    register_reports_mcp(ALL_REPORTS, mcp)
