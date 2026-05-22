"""MatchingService — thin facade over the matching package's primitives.

Exposes ``run``, ``seed_priority``, ``undo``, ``get_log``, and
``count_pending`` so adapters and other services call
``MatchingService(db).method(...)`` uniformly instead of importing
:mod:`moneybin.matching.engine` / ``persistence`` / ``priority`` directly.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast

from moneybin import error_codes
from moneybin.config import MatchingSettings, get_settings
from moneybin.database import Database
from moneybin.errors import RecoveryAction, UserError
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.persistence import (
    MatchStatus,
    get_match_decision,
    get_match_log,
    undo_match,
    update_match_status,
)
from moneybin.matching.priority import seed_source_priority
from moneybin.tables import MATCH_DECISIONS

if TYPE_CHECKING:
    from moneybin.matching.engine import MatchResult

logger = logging.getLogger(__name__)

_SETTABLE_STATUSES: frozenset[str] = frozenset({"accepted", "rejected"})


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

    def set_status(
        self, match_id: str, *, status: str, decided_by: str = "user"
    ) -> None:
        """Accept or reject one pending match decision by id.

        Validates the transition the raw persistence primitive does not:
        only a ``pending`` decision may move to ``accepted``/``rejected``.
        Re-asserting the current status is an idempotent no-op (shape 1b).
        Any other transition raises ``UserError`` carrying ``recovery_actions``.
        """
        if status not in _SETTABLE_STATUSES:
            raise UserError(
                f"status must be one of {sorted(_SETTABLE_STATUSES)}, got {status!r}",
                code=error_codes.MUTATION_INVALID_INPUT,
                recovery_actions=[
                    RecoveryAction(
                        tool="transactions_matches_pending",
                        arguments={},
                        rationale="List pending matches to pick a valid match and status.",
                        confidence="suggested",
                        idempotent=True,
                    )
                ],
            )

        current = get_match_decision(self._db, match_id)
        if current is None:
            raise UserError(
                f"No match decision found for id {match_id!r}.",
                code=error_codes.MUTATION_NOT_FOUND,
                recovery_actions=[
                    RecoveryAction(
                        tool="transactions_matches_pending",
                        arguments={},
                        rationale="List current pending matches to find a valid match_id.",
                        confidence="suggested",
                        idempotent=True,
                    )
                ],
            )

        current_status = current["match_status"]
        if current_status == status:
            return  # idempotent re-assertion

        if current_status != "pending":
            if current_status == "accepted":
                action = RecoveryAction(
                    tool="transactions_matches_undo",
                    arguments={"match_id": match_id},
                    rationale=(
                        "This match is already accepted and merged into core. Reverse it "
                        "with 'moneybin transactions matches undo' first, then re-run matching."
                    ),
                    confidence="suggested",
                    idempotent=False,
                )
            else:
                action = RecoveryAction(
                    tool="transactions_matches_pending",
                    arguments={},
                    rationale=(
                        f"This decision is {current_status}, not pending; list current "
                        "pending matches instead."
                    ),
                    confidence="suggested",
                    idempotent=True,
                )
            raise UserError(
                f"Cannot set match {match_id!r} to {status!r}: it is {current_status!r}, "
                "not pending.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                recovery_actions=[action],
            )

        update_match_status(
            self._db,
            match_id,
            status=cast("MatchStatus", status),
            decided_by=decided_by,
        )
