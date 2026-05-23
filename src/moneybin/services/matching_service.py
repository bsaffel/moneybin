"""MatchingService — thin facade over the matching package's primitives.

Exposes ``run``, ``seed_priority``, ``undo``, ``get_log``, and
``count_pending`` so adapters and other services call
``MatchingService(db).method(...)`` uniformly instead of importing
:mod:`moneybin.matching.engine` / ``persistence`` / ``priority`` directly.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import duckdb

from moneybin import error_codes
from moneybin.config import MatchingSettings, get_settings
from moneybin.database import Database
from moneybin.errors import RecoveryAction, UserError
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.persistence import (
    VALID_MATCH_TYPES,
    get_match_decision,
    get_match_log,
    get_pending_matches,
)
from moneybin.matching.priority import seed_source_priority
from moneybin.tables import MATCH_DECISIONS

if TYPE_CHECKING:
    from moneybin.matching.engine import MatchResult
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

logger = logging.getLogger(__name__)

_SETTABLE_STATUSES: frozenset[str] = frozenset({"accepted", "rejected"})


def _non_pending_recovery(current_status: str) -> RecoveryAction:
    """Recovery action for a set_status call on a non-pending decision.

    An accepted decision is already merged into core; the inverse is the
    audit-log undo (``system_audit_undo``, the M2D undo consumer). Until it
    ships, the equivalent manual route is the CLI ``moneybin transactions
    matches undo`` — named in the rationale. ``arguments`` is empty because the
    ``operation_id`` isn't known at this error site (the agent finds it via the
    audit history). Any other terminal status routes back to the pending list.
    """
    if current_status == "accepted":
        return RecoveryAction(
            tool="system_audit_undo",
            arguments={},
            rationale=(
                "This match is already accepted and merged into core. Reverse it "
                "via the audit-log undo (system_audit_undo); until that MCP tool "
                "ships, run 'moneybin transactions matches undo <match_id>' (CLI)."
            ),
            confidence="suggested",
            idempotent=False,
        )
    # rejected / reversed: the row is no longer pending, so the pending list
    # would be a dead end — point at history, which has no status filter.
    return RecoveryAction(
        tool="transactions_matches_history",
        arguments={},
        rationale=(
            f"This decision is {current_status}, not pending; view it in the "
            "match history (the pending queue excludes it)."
        ),
        confidence="suggested",
        idempotent=True,
    )


class MatchingService:
    """Thin facade over :class:`TransactionMatcher` for uniform service-layer access."""

    def __init__(self, db: Database, settings: MatchingSettings | None = None) -> None:
        """Initialize MatchingService with a Database and optional MatchingSettings."""
        self._db = db
        self._settings = settings or get_settings().matching

    def _match_repo(self) -> MatchDecisionsRepo:
        """Build the audited match-decisions repo (deferred import breaks a cycle).

        This module is eagerly imported by ``services.__init__``, and the repo's
        ``base`` → ``services.audit_service`` chain re-enters that path; a
        module-top import would cycle.
        """
        from moneybin.repositories.match_decisions_repo import (  # noqa: PLC0415
            MatchDecisionsRepo,
        )

        return MatchDecisionsRepo(self._db)

    def count_pending(self, *, match_type: str | None = None) -> int:
        """Return the number of match decisions awaiting user review.

        ``match_type`` filters to a single type; None counts all pending. Used
        for the total_count an MCP read tool needs to report ``has_more``.
        """
        where = "WHERE match_status = 'pending' AND reversed_at IS NULL"
        params: list[Any] = []
        if match_type is not None:
            where += " AND match_type = ?"
            params.append(match_type)
        try:
            row = self._db.execute(
                f"""
                SELECT COUNT(*) FROM {MATCH_DECISIONS.full_name}
                {where}
                """,  # noqa: S608  # TableRef constant + literal where; values parameterized
                params,
            ).fetchone()
            return int(row[0]) if row else 0
        except duckdb.CatalogException:
            return 0  # table not created until the first matcher run

    def run(
        self, *, auto_accept_transfers: bool = False, actor: str = "system"
    ) -> MatchResult:
        """Run same-record dedup (Tier 2b/3) and transfer detection (Tier 4).

        ``auto_accept_transfers`` simulates automated review — used by the
        scenario runner so evaluations can read accepted transfers from
        ``core.bridge_transfers`` without an interactive step. ``actor`` is the
        audit actor for the decisions written this run (surfaces pass
        ``"cli"``/``"mcp"``; defaults to ``"system"`` for automated callers).
        """
        seed_source_priority(self._db, self._settings)
        return TransactionMatcher(self._db, self._settings, actor=actor).run(
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

    def undo(
        self, match_id: str, *, reversed_by: str = "user", actor: str = "system"
    ) -> None:
        """Reverse a match decision (audited via ``MatchDecisionsRepo``).

        ``reversed_by`` is the domain column (``user``/``system``); ``actor`` is
        the audit *surface* (``cli``/``mcp``/``system``), defaulting to
        ``"system"`` for automated callers — matching ``run``'s default and the
        actor taxonomy (``user`` is a ``decided_by`` value, not a surface).
        Surfaces pass their own (``cli``/``mcp``). Raises ``ValueError`` when no
        match with this id exists.
        """
        self._match_repo().reverse(match_id, reversed_by=reversed_by, actor=actor)

    def get_log(
        self, *, limit: int = 50, match_type: str | None = None
    ) -> list[dict[str, Any]]:
        """Return recent match decisions for display.

        Wraps :func:`moneybin.matching.persistence.get_match_log`.
        """
        return get_match_log(self._db, limit=limit, match_type=match_type)

    def set_status(
        self,
        match_id: str,
        *,
        status: str,
        decided_by: str = "user",
        actor: str = "system",
    ) -> None:
        """Accept or reject one pending match decision by id (audited via repo).

        Validates the transition: only a ``pending`` decision may move to
        ``accepted``/``rejected``. Re-asserting the current status is an
        idempotent no-op (shape 1b). Any other transition raises ``UserError``
        carrying ``recovery_actions``. ``decided_by`` is the domain column
        (``user``/``system``); ``actor`` is the audit surface (``cli``/``mcp``,
        default ``"system"``).
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

        # Read-validate-write in one transaction so a concurrent writer can't
        # slip between the guard read and the update (closes the TOCTOU window).
        self._db.begin()
        try:
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
            if current_status != status:
                if current_status != "pending":
                    raise UserError(
                        f"Cannot set match {match_id!r} to {status!r}: it is "
                        f"{current_status!r}, not pending.",
                        code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                        recovery_actions=[_non_pending_recovery(current_status)],
                    )
                self._match_repo().update_status(
                    match_id,
                    status=status,
                    decided_by=decided_by,
                    actor=actor,
                    in_outer_txn=True,
                )
            # current_status == status falls through as an idempotent no-op.
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise

    def get_pending(
        self, *, match_type: str | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Return pending match decisions awaiting review.

        Wraps :func:`moneybin.matching.persistence.get_pending_matches`.
        ``limit`` is pushed to SQL; None returns all pending.
        """
        return get_pending_matches(self._db, match_type=match_type, limit=limit)

    def accept_all_pending(
        self, *, match_type: str | None = None, actor: str = "system"
    ) -> int:
        """Accept every pending match decision in scope. Returns the count accepted.

        Routes through ``MatchDecisionsRepo.accept_pending`` so each acceptance
        emits a paired ``app.audit_log`` row (Invariant 10), all inside one
        transaction (all-or-nothing). ``actor`` is the audit surface
        (``cli``/``mcp``, default ``"system"``). ``match_type``, when given, is
        validated here (the repo's filter is parameterized but unguarded) so a
        bad value raises instead of silently accepting nothing.
        """
        if match_type is not None and match_type not in VALID_MATCH_TYPES:
            raise ValueError(f"Invalid match_type: {match_type!r}")
        return self._match_repo().accept_pending(
            match_type=match_type, decided_by="user", actor=actor
        )
