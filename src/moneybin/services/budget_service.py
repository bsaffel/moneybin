# src/moneybin/services/budget_service.py
"""Budget management service.

Business logic for setting budget targets and checking spending against
them. Consumed by both MCP tools and CLI commands.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from moneybin.database import Database
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope
from moneybin.tables import BUDGETS, FCT_TRANSACTIONS, TRANSACTION_CATEGORIES

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class BudgetSetResult:
    """Result of setting a budget target."""

    category: str
    monthly_amount: Decimal
    action: str  # "created" or "updated"

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for JSON serialization."""
        return {
            "category": self.category,
            "monthly_amount": self.monthly_amount,
            "action": self.action,
        }

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=self.to_dict(),
            sensitivity="low",
            actions=[
                "Use budget.status to see spending vs budget",
                "Use spending.by_category for category breakdown",
            ],
        )


@dataclass(frozen=True, slots=True)
class BudgetCategoryStatus:
    """Budget status for a single category."""

    category: str
    budget: Decimal
    spent: Decimal
    remaining: Decimal
    status: str  # "OK", "WARNING", "OVER"

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for JSON serialization."""
        return {
            "category": self.category,
            "budget": self.budget,
            "spent": self.spent,
            "remaining": self.remaining,
            "status": self.status,
        }


@dataclass(slots=True)
class BudgetStatusResult:
    """Result of budget status query."""

    categories: list[BudgetCategoryStatus]
    month: str

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=[c.to_dict() for c in self.categories],
            sensitivity="low",
            period=self.month,
            actions=[
                "Use budget.set to adjust a budget target",
                "Use spending.by_category for detailed category breakdown",
            ],
        )


class BudgetService:
    """Budget management operations.

    All methods return typed dataclasses with a ``to_envelope()`` method.
    """

    def __init__(self, db: Database) -> None:
        """Initialize BudgetService with an open Database connection."""
        self._db = db

    def set_budget(
        self,
        category: str,
        monthly_amount: Decimal,
        start_month: str | None = None,
    ) -> BudgetSetResult:
        """Create or update a budget target for a category.

        If a budget already exists for this category with an overlapping
        date range, it is updated. Otherwise a new budget is created.

        Args:
            category: Spending category name.
            monthly_amount: Monthly spending target in USD.
            start_month: First active month (YYYY-MM). Defaults to current
                month.

        Returns:
            BudgetSetResult indicating whether the budget was created or
            updated.
        """
        if start_month is None:
            start_month = date.today().strftime("%Y-%m")

        # Check for existing budget with overlapping date range
        check_sql = f"""
            SELECT budget_id
            FROM {BUDGETS.full_name}
            WHERE category = ?
              AND start_month <= ?
              AND (end_month IS NULL OR end_month >= ?)
            ORDER BY start_month DESC
        """
        result = self._db.execute(check_sql, [category, start_month, start_month])
        existing = result.fetchone()

        if existing:
            # Update existing budget
            update_sql = f"""
                UPDATE {BUDGETS.full_name}
                SET monthly_amount = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE budget_id = ?
            """
            self._db.execute(update_sql, [monthly_amount, str(existing[0])])
            action = "updated"
        else:
            # Create new budget
            budget_id = uuid.uuid4().hex[:12]
            insert_sql = f"""
                INSERT INTO {BUDGETS.full_name}
                    (budget_id, category, monthly_amount, start_month)
                VALUES (?, ?, ?, ?)
            """
            self._db.execute(
                insert_sql,
                [budget_id, category, monthly_amount, start_month],
            )
            action = "created"

        logger.info(f"Budget {action} for category")
        return BudgetSetResult(
            category=category,
            monthly_amount=monthly_amount,
            action=action,
        )

    def status(self, month: str | None = None) -> BudgetStatusResult:
        """Get budget vs actual spending for a month.

        Joins budget targets with actual spending aggregated from
        fct_transactions + transaction_categories.

        Args:
            month: Month to check (YYYY-MM). Defaults to current month.

        Returns:
            BudgetStatusResult with per-category budget status.
        """
        if month is None:
            month = date.today().strftime("%Y-%m")

        sql = f"""
            SELECT
                b.category,
                b.monthly_amount AS budget,
                COALESCE(SUM(ABS(t.amount)), 0) AS spent
            FROM {BUDGETS.full_name} b
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                ON b.category = c.category
            LEFT JOIN {FCT_TRANSACTIONS.full_name} t
                ON c.transaction_id = t.transaction_id
                AND t.transaction_year_month = ?
                AND t.amount < 0
            WHERE b.start_month <= ?
              AND (b.end_month IS NULL OR b.end_month >= ?)
            GROUP BY b.category, b.monthly_amount
            ORDER BY b.category
        """

        result = self._db.execute(sql, [month, month, month])
        rows = result.fetchall()

        categories: list[BudgetCategoryStatus] = []
        for row in rows:
            budget_amount = Decimal(str(row[1]))
            spent = Decimal(str(row[2]))
            remaining = budget_amount - spent

            if spent > budget_amount:
                status = "OVER"
            elif spent > budget_amount * Decimal("0.8"):
                status = "WARNING"
            else:
                status = "OK"

            categories.append(
                BudgetCategoryStatus(
                    category=str(row[0]),
                    budget=budget_amount,
                    spent=spent,
                    remaining=remaining,
                    status=status,
                )
            )

        logger.info(f"Budget status: {len(categories)} categories for {month}")
        return BudgetStatusResult(categories=categories, month=month)
