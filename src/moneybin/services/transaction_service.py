# src/moneybin/services/transaction_service.py
"""Transaction search and recurring pattern service.

Business logic for transaction search, filtering, and recurring pattern
detection. Consumed by both MCP tools and CLI commands.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from moneybin.database import Database
from moneybin.mcp.envelope import ResponseEnvelope, build_envelope
from moneybin.tables import FCT_TRANSACTIONS, TRANSACTION_CATEGORIES

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Transaction:
    """Single transaction record."""

    transaction_id: str
    account_id: str
    transaction_date: str
    amount: Decimal
    description: str
    memo: str | None
    source_type: str
    category: str | None
    subcategory: str | None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for JSON serialization."""
        d: dict[str, Any] = {
            "transaction_id": self.transaction_id,
            "account_id": self.account_id,
            "transaction_date": self.transaction_date,
            "amount": self.amount,
            "description": self.description,
            "source_type": self.source_type,
        }
        if self.memo:
            d["memo"] = self.memo
        if self.category:
            d["category"] = self.category
        if self.subcategory:
            d["subcategory"] = self.subcategory
        return d


@dataclass(slots=True)
class TransactionSearchResult:
    """Result of transaction search query."""

    transactions: list[Transaction]
    total_count: int

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=[t.to_dict() for t in self.transactions],
            sensitivity="medium",
            total_count=self.total_count,
            actions=[
                "Use transactions.recurring to find subscription patterns",
                "Use categorize.bulk to categorize uncategorized transactions",
            ],
        )


@dataclass(frozen=True, slots=True)
class RecurringTransaction:
    """A detected recurring transaction pattern."""

    description: str
    avg_amount: Decimal
    occurrence_count: int
    first_seen: str
    last_seen: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to a plain dict for JSON serialization."""
        return {
            "description": self.description,
            "avg_amount": self.avg_amount,
            "occurrence_count": self.occurrence_count,
            "first_seen": self.first_seen,
            "last_seen": self.last_seen,
        }


@dataclass(slots=True)
class RecurringResult:
    """Result of recurring transaction detection."""

    transactions: list[RecurringTransaction]

    def to_envelope(self) -> ResponseEnvelope:
        """Build a ResponseEnvelope for MCP/CLI output."""
        return build_envelope(
            data=[t.to_dict() for t in self.transactions],
            sensitivity="medium",
            actions=[
                "Use transactions.search to see individual occurrences",
                "Use budget.set to create a budget for a recurring expense",
            ],
        )


class TransactionService:
    """Transaction search and recurring pattern operations.

    All methods return typed dataclasses with a ``to_envelope()`` method.
    """

    def __init__(self, db: Database) -> None:
        """Initialize TransactionService with an open Database connection."""
        self._db = db

    def search(
        self,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        min_amount: Decimal | None = None,
        max_amount: Decimal | None = None,
        description: str | None = None,
        account_id: str | None = None,
        category: str | None = None,
        uncategorized_only: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> TransactionSearchResult:
        """Search transactions with flexible filtering.

        Args:
            start_date: ISO 8601 start date (inclusive).
            end_date: ISO 8601 end date (inclusive).
            min_amount: Minimum amount filter.
            max_amount: Maximum amount filter.
            description: ILIKE pattern matched against description and memo.
            account_id: Filter to a specific account.
            category: Filter by category (from transaction_categories).
            uncategorized_only: Only return uncategorized transactions.
            limit: Maximum rows to return.
            offset: Number of rows to skip.

        Returns:
            TransactionSearchResult with matching transactions and total count.
        """
        conditions: list[str] = []
        params: list[object] = []

        if start_date:
            conditions.append("t.transaction_date >= ?")
            params.append(start_date)
        if end_date:
            conditions.append("t.transaction_date <= ?")
            params.append(end_date)
        if min_amount is not None:
            conditions.append("t.amount >= ?")
            params.append(min_amount)
        if max_amount is not None:
            conditions.append("t.amount <= ?")
            params.append(max_amount)
        if description:
            conditions.append("(t.description ILIKE ? OR t.memo ILIKE ?)")
            like_pattern = f"%{description}%"
            params.extend([like_pattern, like_pattern])
        if account_id:
            conditions.append("t.account_id = ?")
            params.append(account_id)
        if category:
            conditions.append("c.category = ?")
            params.append(category)
        if uncategorized_only:
            conditions.append("c.transaction_id IS NULL")

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        # Count query (same conditions, no limit/offset)
        count_sql = f"""
            SELECT COUNT(*)
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                ON t.transaction_id = c.transaction_id
            {where}
        """
        count_result = self._db.execute(count_sql, params)
        total_count = int(count_result.fetchone()[0])  # type: ignore[index]

        # Data query
        sql = f"""
            SELECT
                t.transaction_id,
                t.account_id,
                t.transaction_date,
                t.amount,
                t.description,
                t.memo,
                t.source_type,
                c.category,
                c.subcategory
            FROM {FCT_TRANSACTIONS.full_name} t
            LEFT JOIN {TRANSACTION_CATEGORIES.full_name} c
                ON t.transaction_id = c.transaction_id
            {where}
            ORDER BY t.transaction_date DESC, t.transaction_id
            LIMIT {int(limit)} OFFSET {int(offset)}
        """

        result = self._db.execute(sql, params)
        rows = result.fetchall()

        transactions = [
            Transaction(
                transaction_id=str(row[0]),
                account_id=str(row[1]),
                transaction_date=str(row[2]),
                amount=Decimal(str(row[3])),
                description=str(row[4]),
                memo=str(row[5]) if row[5] else None,
                source_type=str(row[6]),
                category=str(row[7]) if row[7] else None,
                subcategory=str(row[8]) if row[8] else None,
            )
            for row in rows
        ]

        logger.info(
            f"Search returned {len(transactions)} of {total_count} transactions"
        )
        return TransactionSearchResult(
            transactions=transactions, total_count=total_count
        )

    def recurring(self, min_occurrences: int = 3) -> RecurringResult:
        """Detect recurring transaction patterns.

        Groups transactions by description and rounded absolute amount
        to identify subscriptions and recurring charges.

        Args:
            min_occurrences: Minimum number of occurrences to consider
                a transaction as recurring.

        Returns:
            RecurringResult with detected recurring patterns.
        """
        sql = f"""
            SELECT
                description,
                AVG(amount) AS avg_amount,
                COUNT(*) AS occurrence_count,
                MIN(transaction_date) AS first_seen,
                MAX(transaction_date) AS last_seen
            FROM {FCT_TRANSACTIONS.full_name}
            WHERE amount < 0
            GROUP BY description, ROUND(ABS(amount), 0)
            HAVING COUNT(*) >= ?
            ORDER BY occurrence_count DESC, description
        """

        result = self._db.execute(sql, [min_occurrences])
        rows = result.fetchall()

        transactions = [
            RecurringTransaction(
                description=str(row[0]),
                avg_amount=Decimal(str(row[1])),
                occurrence_count=int(row[2]),
                first_seen=str(row[3]),
                last_seen=str(row[4]),
            )
            for row in rows
        ]

        logger.info(f"Found {len(transactions)} recurring patterns")
        return RecurringResult(transactions=transactions)
