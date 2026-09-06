"""MatchingService — thin facade over the matching package's primitives.

Exposes ``run``, ``seed_priority``, ``undo``, ``get_log``, and
``count_pending`` so adapters and other services call
``MatchingService(db).method(...)`` uniformly instead of importing
:mod:`moneybin.matching.engine` / ``persistence`` / ``priority`` directly.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from moneybin import error_codes
from moneybin.config import MatchingSettings, get_settings
from moneybin.database import Database
from moneybin.errors import RecoveryAction, UserError
from moneybin.matching.application import (
    MatchDecisionApplication,
    MatchDecisionNotFoundError,
    MatchDecisionStateError,
    SettableMatchStatus,
    record_committed_match_effects,
)
from moneybin.matching.assignment import NodeKey, connected_components
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.persistence import (
    VALID_MATCH_TYPES,
    count_pending_matches,
    get_active_dedup_edges,
    get_match_log,
    get_pending_matches,
)
from moneybin.matching.priority import seed_source_priority

if TYPE_CHECKING:
    from moneybin.matching.engine import MatchResult
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

logger = logging.getLogger(__name__)

# Printed verbatim wherever a run leaves pending matches: `matches run`,
# `matches backfill`, and the refresh pipeline. One constant because all three
# once drifted into the same unrunnable invocation — `--type matches` is
# required, since `review`'s `--type` defaults to "all" and its decide-flags
# guard rejects every other value. `test_pending_matches_hint_is_runnable`
# executes what this string advertises.
PENDING_MATCHES_HINT = (
    "Run 'moneybin transactions matches pending' to see them, then "
    "'moneybin review --type matches --confirm <match-id>' to decide"
)

_SETTABLE_STATUSES: frozenset[str] = frozenset({"accepted", "rejected"})


@dataclass(frozen=True)
class MatchDecisionOutcome:
    """What :meth:`MatchingService.set_status` actually committed.

    ``match_status`` is re-read after the reconciliation rather than echoed from
    the request, because accepting a match runs a pass over *every* accepted
    transfer — including the row just written. When that row loses the
    earliest-decided-first tiebreak it is reversed inside the same transaction,
    so the requested status is the one value that cannot be trusted to describe
    the outcome. Surfaces report this field, never their own argument.
    """

    match_status: str
    transfers_retired: int


@dataclass(frozen=True)
class BulkAcceptOutcome:
    """What :meth:`MatchingService.accept_all_pending` actually committed.

    The bulk twin of :class:`MatchDecisionOutcome`, and it needs both counts for
    the same reason. A batch can hold a dedup edge and a transfer that edge
    invalidates; the reconciliation reverses the loser inside this transaction,
    so the number of rows flipped is not the number that stood.

    ``transfers_retired`` is the wider figure — it also counts transfers accepted
    long before this batch — so it never substitutes for
    ``reversed_by_reconciliation``, which is only ever this call's own rows.
    """

    accepted: int
    reversed_by_reconciliation: int
    transfers_retired: int


def _non_pending_recovery(
    current_status: str,
    *,
    accepted_operation_id: str | None = None,
) -> RecoveryAction:
    """Recovery action for a set_status call on a non-pending decision.

    Accepted decisions point directly at their audited operation so the agent
    can reverse the merge. Other terminal states route to normalized history.
    """
    if current_status == "accepted" and accepted_operation_id is not None:
        return RecoveryAction(
            tool="system_audit_undo",
            arguments={"operation_id": accepted_operation_id},
            rationale=(
                "This match is already accepted and merged into core. Reverse "
                "the audited acceptance operation before choosing another state."
            ),
            confidence="suggested",
            idempotent=False,
        )
    return RecoveryAction(
        tool="reviews",
        arguments={"kind": "matches", "status": "history"},
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
        return count_pending_matches(self._db, match_type=match_type)

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
        self, *, limit: int | None = 50, match_type: str | None = None
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
    ) -> MatchDecisionOutcome:
        """Accept or reject one pending match decision by id (audited via repo).

        Validates the transition: only a ``pending`` decision may move to
        ``accepted``/``rejected``. Re-asserting the current status is an
        idempotent no-op (shape 1b). Any other transition raises ``UserError``
        carrying ``recovery_actions``. ``decided_by`` is the domain column
        (``user``/``system``); ``actor`` is the audit surface (``cli``/``mcp``,
        default ``"system"``).

        ``transfers_retired`` counts the *standing* accepted transfers this
        acceptance invalidated and therefore reversed — see
        :func:`~moneybin.matching.reconciliation.retire_transfers_invalidated_by_dedup`.
        Zero on a rejection and on the idempotent no-op: neither collapses
        anything, so neither can invalidate a transfer. Also zero when the only
        row reversed is *this* one: a proposal accepted and reversed inside this
        transaction was never standing, ``match_status`` below is what reports
        it, and undo returns it to ``pending`` rather than to accepted. Surfaces
        are expected to report a non-zero count rather than swallow it.

        ``match_status`` is what committed, which is not always ``status``: that
        same reconciliation can reverse *this* row when it loses the
        earliest-decided-first tiebreak against a standing transfer.
        """
        if status not in _SETTABLE_STATUSES:
            raise UserError(
                f"status must be one of {sorted(_SETTABLE_STATUSES)}, got {status!r}",
                code=error_codes.MUTATION_INVALID_INPUT,
                recovery_actions=[
                    RecoveryAction(
                        tool="reviews",
                        arguments={"kind": "matches", "status": "pending"},
                        rationale="List pending matches to pick a valid match and status.",
                        confidence="suggested",
                        idempotent=True,
                    )
                ],
            )

        self._db.begin()
        try:
            application = MatchDecisionApplication(
                self._db,
                decisions=self._match_repo(),
                actor=actor,
                decided_by=decided_by,
            )
            application.set_status(match_id, status=cast(SettableMatchStatus, status))
            effects = application.finalize()
            self._db.commit()
        except MatchDecisionNotFoundError as exc:
            self._db.rollback()
            raise UserError(
                f"No match decision found for id {exc.match_id!r}.",
                code=error_codes.MUTATION_NOT_FOUND,
                recovery_actions=[
                    RecoveryAction(
                        tool="reviews",
                        arguments={"kind": "matches", "status": "pending"},
                        rationale="List current pending matches to find a valid match_id.",
                        confidence="suggested",
                        idempotent=True,
                    )
                ],
            ) from None
        except MatchDecisionStateError as exc:
            self._db.rollback()
            accepted_operation_id = None
            if exc.current_status == "accepted":
                from moneybin.services.audit_service import AuditService

                events = AuditService(self._db).list_events(
                    target_table="match_decisions",
                    target_id=match_id,
                    limit=1,
                )
                if events:
                    accepted_operation_id = events[0].operation_id
            raise UserError(
                f"Cannot set match {match_id!r} to {status!r}: it is "
                f"{exc.current_status!r}, not pending.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                recovery_actions=[
                    _non_pending_recovery(
                        exc.current_status,
                        accepted_operation_id=accepted_operation_id,
                    )
                ],
            ) from None
        except BaseException:
            self._db.rollback()
            raise
        record_committed_match_effects(effects)
        return MatchDecisionOutcome(
            match_status=effects.effective_statuses[match_id],
            transfers_retired=effects.standing_transfers_retired,
        )

    def _compute_component_keys(self) -> dict[tuple[str, str, str], str]:
        """Build a map from (account_id, source_type, stid) to component_key.

        Fetches all active (accepted + pending) non-reversed dedup edges, builds
        connected components via UnionFind, and keys each by
        MIN(f"{source_type}|{source_transaction_id}") over its members within the
        same account_id.

        This is a *prospective* grouping identifier, NOT the prep fold's
        ``match_group_id``: the fold groups accepted edges only, so a cluster
        that is still pending review has a non-null ``component_key`` here while
        its ``match_group_id`` in ``core``/``prep`` is NULL until the edges are
        accepted. Use it to cluster the review queue, not to join against core.

        Returns a dict keyed on (account_id, source_type, stid).
        """
        edges: list[tuple[NodeKey, NodeKey]] = [
            (
                (e["source_type_a"], e["source_transaction_id_a"], e["account_id"]),
                (e["source_type_b"], e["source_transaction_id_b"], e["account_id"]),
            )
            for e in get_active_dedup_edges(self._db, statuses=("accepted", "pending"))
        ]
        # Dedup edges only ever connect same-account nodes, so each component is
        # account-scoped. component_key = account_id prefixed onto the MIN packed
        # "stype|stid" over members — the account prefix makes it globally unique
        # (source_transaction_id only repeats within an account), matching the
        # prep fold's account-prefixed group_id and preventing two accounts'
        # clusters from colliding in the review queue.
        result: dict[tuple[str, str, str], str] = {}
        for members in connected_components(edges):
            acct = members[0][2]  # all members of a component share one account
            component_key = f"{acct}|" + min(f"{st}|{stid}" for st, stid, _ in members)
            for st, stid, member_acct in members:
                result[(member_acct, st, stid)] = component_key
        return result

    def _component_key_for_row(
        self, row: dict[str, Any], comp_keys: dict[tuple[str, str, str], str]
    ) -> str:
        """Resolve a pending dedup row's component_key, warning on the fallback.

        Every pending dedup edge's side-A node is, by construction, a member of
        the active-edge component map. If it ever isn't, the row would silently
        split into its own single-edge cluster (over-counting groups, fragmenting
        the review queue) — so emit a warning to make that anomaly observable
        rather than degrading without a signal.
        """
        lookup_key = (
            row["account_id"],
            row["source_type_a"],
            row["source_transaction_id_a"],
        )
        component_key = comp_keys.get(lookup_key)
        if component_key is None:
            logger.warning(
                f"Pending dedup match {row['match_id']} side-A node absent from the "
                f"active dedup edge map; treating it as its own cluster"
            )
            return row["match_id"]
        return component_key

    def get_pending(
        self, *, match_type: str | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Return pending match decisions awaiting review, enriched with component_key.

        Dedup rows share a ``component_key`` when they belong to the same
        connected component of active+pending dedup edges, so the reviewer can
        cluster copies of one transaction. This is a prospective grouping id, not
        the prep fold's ``match_group_id`` (which groups accepted edges only) —
        see ``_compute_component_keys``; don't use it to join against core.
        Transfer rows are not grouped; their ``component_key`` is their own
        ``match_id``.

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
                component_key = self._component_key_for_row(row, comp_keys)
            else:
                # Transfers are ungrouped; each edge is its own cluster
                component_key = row["match_id"]
            enriched.append({**row, "component_key": component_key})

        return enriched

    def count_pending_dedup_groups(self, *, match_type: str | None = None) -> int:
        """Distinct dedup components across the FULL pending queue (not one page).

        Counts over every pending dedup decision, so the figure is correct even
        when a caller paginates ``get_pending`` — a page-local count would
        undercount when ``has_more`` is true and mislead the reviewer about how
        many transactions still need review.

        ``match_type`` mirrors the caller's ``get_pending`` filter so the count
        stays consistent with the rows returned: a non-dedup scope (e.g.
        ``"transfer"``) has zero dedup groups, not the whole unfiltered queue.
        """
        if match_type is not None and match_type != "dedup":
            return 0
        pending = get_pending_matches(self._db, match_type="dedup", limit=None)
        if not pending:
            return 0
        comp_keys = self._compute_component_keys()
        return len({self._component_key_for_row(r, comp_keys) for r in pending})

    def accept_all_pending(
        self, *, match_type: str | None = None, actor: str = "system"
    ) -> BulkAcceptOutcome:
        """Accept every pending match decision in scope.

        Routes through ``MatchDecisionsRepo.accept_pending`` so each acceptance
        emits a paired ``app.audit_log`` row (Invariant 10), all inside one
        transaction (all-or-nothing) that the reconciliation joins — a bulk
        accept is the sharpest way to collide two transfers' legs, since it
        folds every queued edge at once. ``actor`` is the audit surface
        (``cli``/``mcp``, default ``"system"``). ``match_type``, when given, is
        validated here (the repo's filter is parameterized but unguarded) so a
        bad value raises instead of silently accepting nothing.

        The accepted count is re-read after the reconciliation for the same
        reason ``set_status`` re-reads one row: the batch can contain both a
        dedup edge and a transfer the edge invalidates, and the reconciliation
        reverses the loser inside this transaction. Those self-reversals are
        reported as ``reversed_by_reconciliation`` and excluded from
        ``transfers_retired``, which counts only *standing* transfers this batch
        undid.
        """
        if match_type is not None and match_type not in VALID_MATCH_TYPES:
            raise ValueError(f"Invalid match_type: {match_type!r}")
        self._db.begin()
        try:
            application = MatchDecisionApplication(
                self._db,
                decisions=self._match_repo(),
                actor=actor,
                decided_by="user",
            )
            application.accept_pending(
                match_type=match_type,
            )
            effects = application.finalize()
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        record_committed_match_effects(effects)
        return BulkAcceptOutcome(
            accepted=effects.accepted_count,
            reversed_by_reconciliation=effects.immediate_reversals,
            transfers_retired=effects.standing_transfers_retired,
        )
