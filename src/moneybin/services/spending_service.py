# src/moneybin/services/spending_service.py
"""Spending analysis service.

Business logic for income vs expense summaries, category breakdowns,
merchant analysis, and period comparisons. Consumed by both MCP tools
and CLI commands.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from moneybin.database import Database
from moneybin.mcp.envelope import ResponseEnvelope, build_envelope
from moneybin.tables import FCT_TRANSACTIONS, TRANSACTION_CATEGORIES

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MonthlySpending:
    """Income vs expense totals for a single month."""

    period: str
    income: float
    expenses: float
    net: float
    transaction_count: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for JSON serialization."""
        return {
            "period": self.period,
            "income": self.income,
            "expenses": self.expenses,
            "net": self.net,
            "transaction_count": self.transaction_count,
        }


@dataclass(slots=True)
class SpendingSummary:
    """Result of spending summary query."""

    months: list[MonthlySpending]
    period_label: str = ""

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=[m.to_dict() for m in self.months],
            sensitivity="low",
            period=self.period_label,
            actions=[
                "Use spending.by_category for category breakdown",
                "Use spending.compare to compare periods",
            ],
        )


@dataclass(frozen=True, slots=True)
class CategorySpending:
    """Spending total for a single category."""

    category: str
    subcategory: str | None
    total: float
    transaction_count: int
    percent_of_total: float

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for JSON serialization."""
        d: dict[str, Any] = {
            "category": self.category,
            "total": self.total,
            "transaction_count": self.transaction_count,
            "percent_of_total": self.percent_of_total,
        }
        if self.subcategory:
            d["subcategory"] = self.subcategory
        return d


@dataclass(slots=True)
class CategoryBreakdown:
    """Result of spending-by-category query."""

    categories: list[CategorySpending]
    period_label: str = ""

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=[c.to_dict() for c in self.categories],
            sensitivity="low",
            period=self.period_label,
            actions=[
                "Use spending.merchants for merchant-level breakdown",
                "Use transactions.search to see individual transactions in a category",
            ],
        )


class SpendingService:
    """Spending analysis operations.

    All methods return typed dataclasses with a ``to_envelope()`` method.
    MCP tools call ``to_envelope().to_json()``. CLI commands render the
    dataclass directly as a table or call ``to_envelope().to_json()``
    for ``--output json``.
    """

    def __init__(self, db: Database) -> None:
        """Initialize SpendingService with an open Database connection."""
        self._db = db

    def summary(
        self,
        months: int = 3,
        start_date: str | None = None,
        end_date: str | None = None,
        account_id: list[str] | None = None,
    ) -> SpendingSummary:
        """Get income vs expense totals by month.

        Args:
            months: Number of recent months to include.
            start_date: ISO 8601 start date (overrides months).
            end_date: ISO 8601 end date.
            account_id: Filter to specific accounts.

        Returns:
            SpendingSummary with monthly breakdown.
        """
        conditions: list[str] = []
        params: list[object] = []

        if start_date:
            conditions.append("transaction_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("transaction_date <= ?")
            params.append(end_date)
        if account_id:
            placeholders = ", ".join("?" for _ in account_id)
            conditions.append(f"account_id IN ({placeholders})")
            params.extend(account_id)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        sql = f"""
            SELECT
                transaction_year_month AS period,
                SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS income,
                SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) AS expenses,
                SUM(amount) AS net,
                COUNT(*) AS transaction_count
            FROM {FCT_TRANSACTIONS.full_name}
            {where}
            GROUP BY transaction_year_month
            ORDER BY transaction_year_month DESC
        """

        result = self._db.execute(sql, params)
        rows = result.fetchall()

        # Apply months-based limit in Python when no explicit start_date is given.
        # DuckDB LIMIT ? with a parameterized value is unreliable in all contexts.
        if not start_date:
            rows = rows[:months]

        monthly = [
            MonthlySpending(
                period=str(row[0]),
                income=float(row[1]),
                expenses=float(row[2]),
                net=float(row[3]),
                transaction_count=int(row[4]),
            )
            for row in rows
        ]

        period_label = ""
        if monthly:
            first = monthly[-1].period
            last = monthly[0].period
            period_label = f"{first} to {last}" if first != last else first

        return SpendingSummary(months=monthly, period_label=period_label)

    def by_category(
        self,
        months: int = 3,
        start_date: str | None = None,
        end_date: str | None = None,
        account_id: list[str] | None = None,
        top_n: int = 10,
        include_uncategorized: bool = True,
    ) -> CategoryBreakdown:
        """Get spending broken down by category.

        Args:
            months: Number of recent months to include.
            start_date: ISO 8601 start date (overrides months).
            end_date: ISO 8601 end date.
            account_id: Filter to specific accounts.
            top_n: Limit to top N categories.
            include_uncategorized: Include uncategorized rollup row.

        Returns:
            CategoryBreakdown with per-category totals.
        """
        conditions: list[str] = ["t.amount < 0"]
        params: list[object] = []

        if start_date:
            conditions.append("t.transaction_date >= ?")
            params.append(start_date)
        else:
            # months is a validated integer, safe to interpolate in INTERVAL
            conditions.append(
                f"t.transaction_year_month >= strftime(CURRENT_DATE - INTERVAL '{months} months', '%Y-%m')"
            )
        if end_date:
            conditions.append("t.transaction_date <= ?")
            params.append(end_date)
        if account_id:
            placeholders = ", ".join("?" for _ in account_id)
            conditions.append(f"t.account_id IN ({placeholders})")
            params.extend(account_id)

        where = "WHERE " + " AND ".join(conditions)

        sql = f"""
            SELECT
                COALESCE(c.category, 'Uncategorized') AS category,
                c.subcategory,
                SUM(ABS(t.amount)) AS total,
                COUNT(*) AS transaction_count
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                ON t.transaction_id = c.transaction_id
            {where}
            GROUP BY COALESCE(c.category, 'Uncategorized'), c.subcategory
            ORDER BY total DESC
        """

        result = self._db.execute(sql, params)
        rows = result.fetchall()

        grand_total = sum(float(row[2]) for row in rows) or 1.0
        categories: list[CategorySpending] = []
        for row in rows:
            cat_name = str(row[0])
            if not include_uncategorized and cat_name == "Uncategorized":
                continue
            categories.append(
                CategorySpending(
                    category=cat_name,
                    subcategory=row[1],
                    total=float(row[2]),
                    transaction_count=int(row[3]),
                    percent_of_total=round(float(row[2]) / grand_total * 100, 1),
                )
            )

        if top_n and len(categories) > top_n:
            categories = categories[:top_n]

        return CategoryBreakdown(categories=categories)
