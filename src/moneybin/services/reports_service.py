"""Reports service — query layer for the reports.* SQLMesh views.

CLI commands and MCP tools both read the reports.* views through this
service per the architecture rule that MCP tools and CLI commands are
thin wrappers around a shared service layer (see
``.claude/rules/mcp-server.md`` "Architecture" and ``.claude/rules/cli.md``
"Core Principle"). Each method returns ``(columns, rows)`` so callers can
render Rich tables, build JSON envelopes, or pass the data to other
consumers without re-encoding.

The module-level allowlist constants (``CASHFLOW_GROUPINGS``,
``MERCHANTS_SORTS``, etc.) are the canonical enum vocabularies — surfaces
that need their own validation messages (typer's ``BadParameter``) import
the constant rather than redefining it.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal
from typing import Any

from moneybin.database import Database
from moneybin.tables import (
    REPORTS_BALANCE_DRIFT,
    REPORTS_CASH_FLOW,
    REPORTS_LARGE_TRANSACTIONS,
    REPORTS_MERCHANT_ACTIVITY,
    REPORTS_RECURRING_SUBSCRIPTIONS,
    REPORTS_SPENDING_TREND,
    REPORTS_UNCATEGORIZED_QUEUE,
)

QueryResult = tuple[list[str], list[tuple[Any, ...]]]


CASHFLOW_GROUPINGS: tuple[str, ...] = (
    "account",
    "category",
    "account-and-category",
)
SPENDING_COMPARES: tuple[str, ...] = ("yoy", "mom", "trailing")
RECURRING_STATUSES: tuple[str, ...] = ("active", "inactive", "all")
RECURRING_CADENCES: tuple[str, ...] = (
    "weekly",
    "biweekly",
    "monthly",
    "quarterly",
    "yearly",
    "irregular",
)
MERCHANTS_SORTS: dict[str, str] = {
    "spend": "total_spend DESC",
    "count": "txn_count DESC",
    "recent": "last_seen DESC",
}
LARGE_TXN_ANOMALIES: tuple[str, ...] = ("none", "account", "category")
DRIFT_STATUSES: tuple[str, ...] = ("drift", "warning", "clean", "no-data", "all")


class ReportsService:
    """Query layer for the reports.* SQLMesh views."""

    def __init__(self, db: Database) -> None:
        """Initialize with an open Database connection."""
        self._db = db

    def _execute(self, sql: str, params: Sequence[object]) -> QueryResult:
        cursor = self._db.execute(sql, list(params))
        rows = cursor.fetchall()
        cols = [d[0] for d in cursor.description] if cursor.description else []
        return cols, rows

    def cash_flow(
        self,
        *,
        from_month: str | None = None,
        to_month: str | None = None,
        by: str = "account-and-category",
    ) -> QueryResult:
        """Monthly inflow/outflow/net rollup, grouped per ``by``."""
        if by not in CASHFLOW_GROUPINGS:
            raise ValueError(f"Unknown by: {by}")
        select_cols = "year_month"
        group_cols = "year_month"
        if "account" in by:
            # account_id keeps rows distinct when two accounts share a display_name
            select_cols += ", account_id, account_name"
            group_cols += ", account_id, account_name"
        if "category" in by:
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
        return self._execute(sql, params)

    def spending_trend(
        self,
        *,
        from_month: str | None = None,
        to_month: str | None = None,
        category: str | None = None,
        compare: str = "yoy",
    ) -> QueryResult:
        """Monthly spending trend with MoM, YoY, and trailing-3mo deltas.

        ``compare`` is validated for caller-side intent only — the view
        returns all three comparison columns regardless.
        """
        if compare not in SPENDING_COMPARES:
            raise ValueError(f"Unknown compare: {compare}")
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
        return self._execute(sql, params)

    def recurring_subscriptions(
        self,
        *,
        min_confidence: float = 0.5,
        status: str = "active",
        cadence: str | None = None,
    ) -> QueryResult:
        """Likely-recurring subscription candidates with confidence scores."""
        if status not in RECURRING_STATUSES:
            raise ValueError(f"Unknown status: {status}")
        if cadence is not None and cadence not in RECURRING_CADENCES:
            raise ValueError(f"Unknown cadence: {cadence}")
        sql = f"""
            SELECT merchant_normalized, cadence, avg_amount, occurrence_count,
                   first_seen, last_seen, status, annualized_cost, confidence
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
        return self._execute(sql, params)

    def merchant_activity(
        self,
        *,
        top: int = 25,
        sort: str = "spend",
    ) -> QueryResult:
        """Per-merchant lifetime activity totals."""
        if sort not in MERCHANTS_SORTS:
            raise ValueError(f"Unknown sort: {sort}")
        sql = f"""
            SELECT merchant_normalized, total_spend, total_inflow, total_outflow,
                   txn_count, avg_amount, median_amount, first_seen, last_seen,
                   active_months, top_category, account_count
            FROM {REPORTS_MERCHANT_ACTIVITY.full_name}
            ORDER BY {MERCHANTS_SORTS[sort]}
            LIMIT ?
        """  # noqa: S608  # TableRef + MERCHANTS_SORTS allowlists
        return self._execute(sql, [top])

    def uncategorized_queue(
        self,
        *,
        min_amount: Decimal | float | int = 0,
        account: str | None = None,
        limit: int = 50,
    ) -> QueryResult:
        """Uncategorized transactions queue, ranked by curator-impact."""
        sql = f"""
            SELECT transaction_id, account_id, account_name, txn_date, amount,
                   description, merchant_normalized, age_days, priority_score,
                   source_type, source_id
            FROM {REPORTS_UNCATEGORIZED_QUEUE.full_name}
            WHERE ABS(amount) >= ?
        """  # noqa: S608  # TableRef interpolation
        params: list[object] = [min_amount]
        if account:
            sql += " AND account_name = ?"
            params.append(account)
        sql += " ORDER BY priority_score DESC LIMIT ?"
        params.append(limit)
        return self._execute(sql, params)

    def large_transactions(
        self,
        *,
        top: int = 25,
        anomaly: str = "none",
    ) -> QueryResult:
        """Top transactions with per-account/category z-score columns."""
        if anomaly not in LARGE_TXN_ANOMALIES:
            raise ValueError(f"Unknown anomaly: {anomaly}")
        sql = f"""
            SELECT transaction_id, account_name, txn_date, amount, description,
                   merchant_normalized, category, amount_zscore_account,
                   amount_zscore_category, is_top_100
            FROM {REPORTS_LARGE_TRANSACTIONS.full_name}
        """  # noqa: S608  # TableRef interpolation
        if anomaly == "account":
            sql += " WHERE amount_zscore_account > 2.5"
        elif anomaly == "category":
            sql += " WHERE amount_zscore_category > 2.5"
        sql += " ORDER BY ABS(amount) DESC LIMIT ?"
        return self._execute(sql, [top])

    def balance_drift(
        self,
        *,
        account: str | None = None,
        status: str = "all",
        since: str | None = None,
    ) -> QueryResult:
        """Asserted vs computed balance reconciliation deltas."""
        if status not in DRIFT_STATUSES:
            raise ValueError(f"Unknown status: {status}")
        sql = f"""
            SELECT account_id, account_name, assertion_date, asserted_balance,
                   computed_balance, drift, drift_abs, drift_pct,
                   days_since_assertion, status
            FROM {REPORTS_BALANCE_DRIFT.full_name}
            WHERE 1=1
        """  # noqa: S608  # TableRef interpolation
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
        return self._execute(sql, params)
