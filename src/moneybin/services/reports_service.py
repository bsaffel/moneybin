"""Reports service — query layer for the reports.* SQLMesh views.

CLI commands and MCP tools both read the reports.* views through this
service per the architecture rule that MCP tools and CLI commands are
thin wrappers around a shared service layer (see
``.claude/rules/mcp-server.md`` "Architecture" and ``.claude/rules/cli.md``
"Core Principle"). Each method constructs and returns a typed payload
dataclass so callers can iterate payload.rows with attribute access.

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
from moneybin.privacy.payloads.reports import (
    BalanceDriftPayload,
    BalanceDriftRow,
    CashFlowPayload,
    CashFlowRow,
    LargeTransactionRow,
    LargeTransactionsPayload,
    MerchantActivityPayload,
    MerchantActivityRow,
    RecurringSubscriptionRow,
    RecurringSubscriptionsPayload,
    SpendingTrendPayload,
    SpendingTrendRow,
    UncategorizedQueuePayload,
    UncategorizedQueueRow,
)
from moneybin.tables import (
    REPORTS_BALANCE_DRIFT,
    REPORTS_CASH_FLOW,
    REPORTS_LARGE_TRANSACTIONS,
    REPORTS_MERCHANT_ACTIVITY,
    REPORTS_RECURRING_SUBSCRIPTIONS,
    REPORTS_SPENDING_TREND,
    REPORTS_UNCATEGORIZED_QUEUE,
)

# Internal type alias for the raw cursor result; used only by _execute.
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
    ) -> CashFlowPayload:
        """Monthly inflow/outflow/net rollup, grouped per ``by``."""
        if by not in CASHFLOW_GROUPINGS:
            raise ValueError(f"Unknown by: {by}")
        include_account = "account" in by
        include_category = "category" in by
        select_cols = "year_month"
        group_cols = "year_month"
        if include_account:
            # account_id keeps rows distinct when two accounts share a display_name
            select_cols += ", account_id, account_name"
            group_cols += ", account_id, account_name"
        if include_category:
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
        cols, rows = self._execute(sql, params)
        result: list[CashFlowRow] = []
        for r in rows:
            d = dict(zip(cols, r, strict=False))
            result.append(
                CashFlowRow(
                    year_month=d["year_month"],
                    account_id=d.get("account_id") if include_account else None,
                    account_name=d.get("account_name") if include_account else None,
                    category=d.get("category") if include_category else None,
                    inflow=Decimal(str(d["inflow"])),
                    outflow=Decimal(str(d["outflow"])),
                    net=Decimal(str(d["net"])),
                    txn_count=int(d["txn_count"]),
                )
            )
        return CashFlowPayload(rows=result)

    def spending_trend(
        self,
        *,
        from_month: str | None = None,
        to_month: str | None = None,
        category: str | None = None,
        compare: str = "yoy",
    ) -> SpendingTrendPayload:
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
        cols, rows = self._execute(sql, params)
        result: list[SpendingTrendRow] = []
        for r in rows:
            d = dict(zip(cols, r, strict=False))
            result.append(
                SpendingTrendRow(
                    year_month=d["year_month"],
                    category=d.get("category"),
                    total_spend=Decimal(str(d["total_spend"])),
                    txn_count=int(d["txn_count"]),
                    prev_month_spend=(
                        Decimal(str(d["prev_month_spend"]))
                        if d.get("prev_month_spend") is not None
                        else None
                    ),
                    mom_delta=(
                        Decimal(str(d["mom_delta"]))
                        if d.get("mom_delta") is not None
                        else None
                    ),
                    mom_pct=d.get("mom_pct"),
                    prev_year_spend=(
                        Decimal(str(d["prev_year_spend"]))
                        if d.get("prev_year_spend") is not None
                        else None
                    ),
                    yoy_delta=(
                        Decimal(str(d["yoy_delta"]))
                        if d.get("yoy_delta") is not None
                        else None
                    ),
                    yoy_pct=d.get("yoy_pct"),
                    trailing_3mo_avg=(
                        Decimal(str(d["trailing_3mo_avg"]))
                        if d.get("trailing_3mo_avg") is not None
                        else None
                    ),
                )
            )
        return SpendingTrendPayload(rows=result)

    def recurring_subscriptions(
        self,
        *,
        min_confidence: float = 0.5,
        status: str = "active",
        cadence: str | None = None,
    ) -> RecurringSubscriptionsPayload:
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
        cols, rows = self._execute(sql, params)
        result: list[RecurringSubscriptionRow] = []
        for r in rows:
            d = dict(zip(cols, r, strict=False))
            result.append(
                RecurringSubscriptionRow(
                    merchant_normalized=d.get("merchant_normalized"),
                    cadence=d.get("cadence"),
                    avg_amount=(
                        Decimal(str(d["avg_amount"]))
                        if d.get("avg_amount") is not None
                        else None
                    ),
                    occurrence_count=int(d["occurrence_count"]),
                    first_seen=d.get("first_seen"),
                    last_seen=d.get("last_seen"),
                    status=d.get("status"),
                    annualized_cost=(
                        Decimal(str(d["annualized_cost"]))
                        if d.get("annualized_cost") is not None
                        else None
                    ),
                    confidence=float(d["confidence"]),
                )
            )
        return RecurringSubscriptionsPayload(rows=result)

    def merchant_activity(
        self,
        *,
        top: int = 25,
        sort: str = "spend",
    ) -> MerchantActivityPayload:
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
        cols, rows = self._execute(sql, [top])
        result: list[MerchantActivityRow] = []
        for r in rows:
            d = dict(zip(cols, r, strict=False))

            def _dec(v: Any) -> Decimal | None:
                return Decimal(str(v)) if v is not None else None

            result.append(
                MerchantActivityRow(
                    merchant_normalized=d.get("merchant_normalized"),
                    total_spend=_dec(d.get("total_spend")),
                    total_inflow=_dec(d.get("total_inflow")),
                    total_outflow=_dec(d.get("total_outflow")),
                    txn_count=int(d["txn_count"]),
                    avg_amount=_dec(d.get("avg_amount")),
                    median_amount=_dec(d.get("median_amount")),
                    first_seen=d.get("first_seen"),
                    last_seen=d.get("last_seen"),
                    active_months=(
                        int(d["active_months"])
                        if d.get("active_months") is not None
                        else None
                    ),
                    top_category=d.get("top_category"),
                    account_count=(
                        int(d["account_count"])
                        if d.get("account_count") is not None
                        else None
                    ),
                )
            )
        return MerchantActivityPayload(rows=result)

    def uncategorized_queue(
        self,
        *,
        min_amount: Decimal | float | int = 0,
        account: str | None = None,
        limit: int = 50,
    ) -> UncategorizedQueuePayload:
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
        cols, rows = self._execute(sql, params)
        result: list[UncategorizedQueueRow] = []
        for r in rows:
            d = dict(zip(cols, r, strict=False))
            result.append(
                UncategorizedQueueRow(
                    transaction_id=d["transaction_id"],
                    account_id=d["account_id"],
                    account_name=d.get("account_name"),
                    txn_date=d["txn_date"],
                    amount=Decimal(str(d["amount"])),
                    description=d.get("description"),
                    merchant_normalized=d.get("merchant_normalized"),
                    age_days=(
                        int(d["age_days"]) if d.get("age_days") is not None else None
                    ),
                    priority_score=(
                        float(d["priority_score"])
                        if d.get("priority_score") is not None
                        else None
                    ),
                    source_type=d.get("source_type"),
                    source_id=d.get("source_id"),
                )
            )
        return UncategorizedQueuePayload(rows=result)

    def large_transactions(
        self,
        *,
        top: int = 25,
        anomaly: str = "none",
    ) -> LargeTransactionsPayload:
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
        cols, rows = self._execute(sql, [top])
        result: list[LargeTransactionRow] = []
        for r in rows:
            d = dict(zip(cols, r, strict=False))
            result.append(
                LargeTransactionRow(
                    transaction_id=d["transaction_id"],
                    account_name=d.get("account_name"),
                    txn_date=d["txn_date"],
                    amount=Decimal(str(d["amount"])),
                    description=d.get("description"),
                    merchant_normalized=d.get("merchant_normalized"),
                    category=d.get("category"),
                    amount_zscore_account=(
                        float(d["amount_zscore_account"])
                        if d.get("amount_zscore_account") is not None
                        else None
                    ),
                    amount_zscore_category=(
                        float(d["amount_zscore_category"])
                        if d.get("amount_zscore_category") is not None
                        else None
                    ),
                    is_top_100=bool(d["is_top_100"]),
                )
            )
        return LargeTransactionsPayload(rows=result)

    def balance_drift(
        self,
        *,
        account: str | None = None,
        status: str = "all",
        since: str | None = None,
    ) -> BalanceDriftPayload:
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
        cols, rows = self._execute(sql, params)
        result: list[BalanceDriftRow] = []
        for r in rows:
            d = dict(zip(cols, r, strict=False))

            def _dec(v: Any) -> Decimal | None:
                return Decimal(str(v)) if v is not None else None

            result.append(
                BalanceDriftRow(
                    account_id=d["account_id"],
                    account_name=d.get("account_name"),
                    assertion_date=d["assertion_date"],
                    asserted_balance=_dec(d.get("asserted_balance")),
                    computed_balance=_dec(d.get("computed_balance")),
                    drift=_dec(d.get("drift")),
                    drift_abs=_dec(d.get("drift_abs")),
                    drift_pct=(
                        float(d["drift_pct"])
                        if d.get("drift_pct") is not None
                        else None
                    ),
                    days_since_assertion=(
                        int(d["days_since_assertion"])
                        if d.get("days_since_assertion") is not None
                        else None
                    ),
                    status=d.get("status"),
                )
            )
        return BalanceDriftPayload(rows=result)
