"""System-status service: data inventory + queue counts.

v2: Replaces OverviewService.status() under the new system_* namespace.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

from moneybin.database import Database
from moneybin.services.categorization_service import CategorizationService
from moneybin.services.matching_service import MatchingService
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
        matches_pending = MatchingService(self._db).count_pending()
        categorize_pending = CategorizationService(self._db).count_uncategorized()

        logger.info(
            f"System status: {accounts_count} accounts, {transactions_count} transactions, "
            f"{matches_pending} matches pending, {categorize_pending} uncategorized"
        )
        return SystemStatus(
            accounts_count=accounts_count,
            transactions_count=transactions_count,
            transactions_date_range=(min_date, max_date),
            last_import_at=last_import_at,
            matches_pending=matches_pending,
            categorize_pending=categorize_pending,
        )

    def _count_accounts(self) -> int:
        row = self._db.execute(
            f"SELECT COUNT(*) FROM {DIM_ACCOUNTS.full_name}"  # noqa: S608  # TableRef constant, not user input
        ).fetchone()
        return int(row[0]) if row else 0

    def _query_transactions(self) -> tuple[int, date | None, date | None]:
        row = self._db.execute(
            f"""
            SELECT
                COUNT(*),
                MIN(transaction_date),
                MAX(transaction_date)
            FROM {FCT_TRANSACTIONS.full_name}
            """  # noqa: S608  # TableRef constant, not user input
        ).fetchone()
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
