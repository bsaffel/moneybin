"""MatchingService — thin facade over the matching package's primitives.

Exposes ``run``, ``seed_priority``, ``undo``, ``get_log``, and
``count_pending`` so adapters and other services call
``MatchingService(db).method(...)`` uniformly instead of importing
:mod:`moneybin.matching.engine` / ``persistence`` / ``priority`` directly.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast

import duckdb

from moneybin import error_codes
from moneybin.config import MatchingSettings, get_settings
from moneybin.database import Database
from moneybin.errors import RecoveryAction, UserError
from moneybin.matching.assignment import NodeKey, connected_components
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.persistence import (
    MatchStatus,
    accept_pending_matches,
    get_active_dedup_edges,
    get_match_decision,
    get_match_log,
    get_pending_matches,
    undo_match,
    update_match_status,
)
from moneybin.matching.priority import seed_source_priority
from moneybin.tables import MATCH_DECISIONS

if TYPE_CHECKING:
    from moneybin.matching.engine import MatchResult

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

    def count_pending(self, *, match_type: str | None = None) -> int:
        """Return the number of match decisions awaiting user review.

        ``match_type`` filters to a single type; None counts all pending. Used
        for the total_count an MCP read tool needs to report ``has_more``.
        """
        where = "WHERE match_status = 'pending' AND reversed_at IS NULL"
        params: list[Any] = []
        if match_type:
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
                update_match_status(
                    self._db,
                    match_id,
                    status=cast("MatchStatus", status),
                    decided_by=decided_by,
                )
            # current_status == status falls through as an idempotent no-op.
            self._db.commit()
        except Exception:
            self._db.rollback()
            raise

    def _compute_component_keys(self) -> dict[tuple[str, str, str], str]:
        """Build a map from (account_id, source_type, stid) to component_key.

        Fetches all active+pending non-reversed dedup edges, builds connected
        components via UnionFind, then computes each component's key as
        MIN(f"{source_type}|{source_transaction_id}") over its members within
        the same account_id — matching the prep fold's group_id semantics.

        Returns a dict keyed on (account_id, source_type, stid).
        """
        edges: list[tuple[NodeKey, NodeKey]] = [
            (
                (e["source_type_a"], e["source_transaction_id_a"], e["account_id"]),
                (e["source_type_b"], e["source_transaction_id_b"], e["account_id"]),
            )
            for e in get_active_dedup_edges(self._db)
        ]
        # Dedup edges only ever connect same-account nodes, so each component is
        # account-scoped. component_key = MIN packed "stype|stid" over members,
        # matching the prep fold's group_id.
        result: dict[tuple[str, str, str], str] = {}
        for members in connected_components(edges):
            component_key = min(f"{st}|{stid}" for st, stid, _ in members)
            for st, stid, acct in members:
                result[(acct, st, stid)] = component_key
        return result

    def get_pending(
        self, *, match_type: str | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Return pending match decisions awaiting review, enriched with component_key.

        Dedup rows share a ``component_key`` when they belong to the same
        connected component of active+pending dedup edges — the same grouping
        the prep fold uses for ``match_group_id``. Transfer rows are not grouped;
        their ``component_key`` is the row's own ``match_id``.

        ``limit`` is pushed to SQL; None returns all pending.
        """
        rows = get_pending_matches(self._db, match_type=match_type, limit=limit)
        if not rows:
            return rows

        # Build component keys once for all pending dedup rows in this call
        comp_keys = self._compute_component_keys()

        enriched: list[dict[str, Any]] = []
        for row in rows:
            if row.get("match_type") == "dedup":
                acct = row["account_id"]
                stype_a = row["source_type_a"]
                stid_a = row["source_transaction_id_a"]
                lookup_key = (acct, stype_a, stid_a)
                # Fall back to match_id if the node isn't in our edge map
                # (should not happen for pending dedup, but be defensive)
                component_key = comp_keys.get(lookup_key, row["match_id"])
            else:
                # Transfers are ungrouped; each edge is its own cluster
                component_key = row["match_id"]
            enriched.append({**row, "component_key": component_key})

        return enriched

    def accept_all_pending(self, *, match_type: str | None = None) -> int:
        """Accept every pending match decision in scope. Returns the count accepted.

        Delegates to the atomic bulk UPDATE in persistence (single statement,
        all-or-nothing) rather than a per-row loop — so a mid-batch failure
        can't leave the queue half-accepted and the count reflects exactly what
        committed. ``WHERE match_status = 'pending'`` is itself the guard.
        """
        return accept_pending_matches(
            self._db, match_type=match_type, decided_by="user"
        )
