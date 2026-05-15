"""System-status service: data inventory + queue counts + pipeline freshness."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime

from moneybin.database import Database
from moneybin.services.categorization_service import CategorizationService
from moneybin.services.matching_service import MatchingService
from moneybin.services.review_service import ReviewService
from moneybin.tables import DIM_ACCOUNTS, FCT_TRANSACTIONS, IMPORT_LOG

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SystemStatus:
    """Snapshot of data inventory and pending review queue counts."""

    accounts_count: int
    transactions_count: int
    transactions_date_range: tuple[date | None, date | None]
    last_import_at: date | None
    matches_pending: int
    categorize_pending: int


@dataclass(slots=True)
class ModelFreshness:
    """Freshness facts for a single SQLMesh model.

    See docs/specs/core-updated-at-convention.md for column semantics.
    """

    model_name: str
    last_changed_at: datetime | None
    last_applied_at: datetime | None


class SystemService:
    """Data inventory and review queue aggregation for system-level status."""

    def __init__(self, db: Database) -> None:
        """Bind to an open Database connection."""
        self._db = db

    def status(self) -> SystemStatus:
        """Return a current snapshot of data inventory and pending queue counts."""
        accounts_count = self._count_accounts()
        transactions_count, min_date, max_date = self._query_transactions()
        last_import_at = self._last_import_at()
        review = ReviewService(
            MatchingService(self._db), CategorizationService(self._db)
        ).status()

        logger.info(
            f"System status: {accounts_count} accounts, {transactions_count} transactions, "
            f"{review.matches_pending} matches pending, {review.categorize_pending} uncategorized"
        )
        return SystemStatus(
            accounts_count=accounts_count,
            transactions_count=transactions_count,
            transactions_date_range=(min_date, max_date),
            last_import_at=last_import_at,
            matches_pending=review.matches_pending,
            categorize_pending=review.categorize_pending,
        )

    def _count_accounts(self) -> int:
        try:
            row = self._db.execute(
                f"SELECT COUNT(*) FROM {DIM_ACCOUNTS.full_name}"  # noqa: S608  # TableRef constant, not user input
            ).fetchone()
            return int(row[0]) if row else 0
        except Exception:  # noqa: BLE001 — core schema may not exist before first transform
            return 0

    def _query_transactions(self) -> tuple[int, date | None, date | None]:
        try:
            row = self._db.execute(
                f"""
                SELECT
                    COUNT(*),
                    MIN(transaction_date),
                    MAX(transaction_date)
                FROM {FCT_TRANSACTIONS.full_name}
                """  # noqa: S608  # TableRef constant, not user input
            ).fetchone()
        except Exception:  # noqa: BLE001 — core schema may not exist before first transform
            return 0, None, None
        if not row:
            return 0, None, None
        count = int(row[0])
        min_date: date | None = row[1]
        max_date: date | None = row[2]
        return count, min_date, max_date

    def _last_import_at(self) -> date | None:
        """Return the date of the most recent completed import, or None."""
        try:
            row = self._db.execute(
                f"""
                SELECT MAX(completed_at)::DATE
                FROM {IMPORT_LOG.full_name}
                WHERE status = 'complete'
                """  # noqa: S608  # TableRef constant, not user input
            ).fetchone()
            return row[0] if row and row[0] is not None else None
        except Exception:  # noqa: BLE001 — table may not exist before first import
            return None

    def model_freshness(self, model_name: str) -> ModelFreshness | None:
        """Return freshness for one SQLMesh model, or None if not yet applied.

        Reads from meta.model_freshness; returns None when the model has never
        been materialized (no row in sqlmesh._snapshots).
        """
        row = self._db.execute(
            "SELECT last_changed_at, last_applied_at "
            "FROM meta.model_freshness "
            "WHERE model_name = ?",
            [model_name],
        ).fetchone()
        if row is None:
            return None
        return ModelFreshness(
            model_name=model_name,
            last_changed_at=row[0],
            last_applied_at=row[1],
        )
