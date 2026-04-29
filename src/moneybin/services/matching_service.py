"""MatchingService — thin facade over TransactionMatcher.

Exists so the scenario runner, MCP tools, and CLI can call
``MatchingService(db).run()`` uniformly alongside other services.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from moneybin.config import MatchingSettings, get_settings
from moneybin.database import Database
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.priority import seed_source_priority

if TYPE_CHECKING:
    from moneybin.matching.engine import MatchResult

logger = logging.getLogger(__name__)


class MatchingService:
    """Thin facade over :class:`TransactionMatcher` for uniform service-layer access."""

    def __init__(self, db: Database, settings: MatchingSettings | None = None) -> None:
        """Initialize MatchingService with a Database and optional MatchingSettings."""
        self._db = db
        self._settings = settings or get_settings().matching

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
