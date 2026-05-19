"""MatchingService — thin facade over TransactionMatcher.

Exists so the scenario runner, MCP tools, and CLI can call
``MatchingService(db).run()`` uniformly alongside other services.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from moneybin.config import MatchingSettings, get_settings
from moneybin.database import Database
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.persistence import get_match_log, undo_match
from moneybin.matching.priority import seed_source_priority
from moneybin.tables import MATCH_DECISIONS

if TYPE_CHECKING:
    from moneybin.matching.engine import MatchResult

logger = logging.getLogger(__name__)


class MatchingService:
    """Thin facade over :class:`TransactionMatcher` for uniform service-layer access."""

    def __init__(self, db: Database, settings: MatchingSettings | None = None) -> None:
        """Initialize MatchingService with a Database and optional MatchingSettings."""
        self._db = db
        self._settings = settings or get_settings().matching

    def count_pending(self) -> int:
        """Return the number of match decisions awaiting user review."""
        try:
            row = self._db.execute(
                f"""
                SELECT COUNT(*) FROM {MATCH_DECISIONS.full_name}
                WHERE match_status = 'pending' AND reversed_at IS NULL
                """  # noqa: S608  # TableRef constant, no user input
            ).fetchone()
            return int(row[0]) if row else 0
        except Exception:  # noqa: BLE001 — table may not exist before first run
            return 0

    def run(self, *, auto_accept_transfers: bool = False) -> MatchResult:
        """Run same-record dedup (Tier 2b/3) and transfer detection (Tier 4).

        ``auto_accept_transfers`` simulates automated review — used by the
        scenario runner so evaluations can read accepted transfers from
        ``core.bridge_transfers`` without an interactive step.
        """
        seed_source_priority(self._db, self._settings)
        return TransactionMatcher(self._db, self._settings).run(
            auto_accept_transfers=auto_accept_transfers
        )

    def seed_priority(self) -> None:
        """Seed ``app.seed_source_priority`` from current MatchingSettings.

        Exposed so callers that need only the seed step (e.g. SQLMesh
        transforms that LEFT JOIN onto the priority table) can route
        through the service rather than importing the matching module
        directly.
        """
        seed_source_priority(self._db, self._settings)

    def undo(self, match_id: str, *, reversed_by: str = "user") -> None:
        """Reverse a match decision.

        Wraps :func:`moneybin.matching.persistence.undo_match` so adapters
        route through the service rather than importing the persistence
        module directly.
        """
        undo_match(self._db, match_id, reversed_by=reversed_by)

    def get_log(
        self, *, limit: int = 50, match_type: str | None = None
    ) -> list[dict[str, Any]]:
        """Return recent match decisions for display.

        Wraps :func:`moneybin.matching.persistence.get_match_log`.
        """
        return get_match_log(self._db, limit=limit, match_type=match_type)
