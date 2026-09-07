"""Transaction-local application of match-decision requests."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from moneybin.database import Database
from moneybin.errors import exception_origin
from moneybin.matching.aliasing import (
    AliasForwardResult,
    forward_rekeyed_transaction_ids,
    record_committed_alias_forwarding,
)
from moneybin.matching.persistence import get_match_decision, get_match_statuses
from moneybin.matching.reconciliation import (
    record_dedup_retirements,
    retire_transfers_invalidated_by_dedup,
)

if TYPE_CHECKING:
    from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo

logger = logging.getLogger(__name__)

type SettableMatchStatus = Literal["accepted", "rejected"]


@dataclass(frozen=True, slots=True)
class MatchStatusChange:
    """One requested status and its effective transaction-local status."""

    match_id: str
    requested_status: SettableMatchStatus
    prior_status: str
    effective_status: str
    changed: bool


@dataclass(frozen=True, slots=True)
class MatchApplicationEffects:
    """Transaction-local facts from applying one group of match decisions."""

    changes: tuple[MatchStatusChange, ...]
    reconciliation_reversals: int | None
    # An accepted dedup merge can re-anchor the group and change its canonical
    # transaction_id (ADR-015). Carried here rather than counted at write time
    # for the same reason as the reversals: nothing is durable until the owner
    # commits.
    alias_forwarding: AliasForwardResult = AliasForwardResult()

    def __post_init__(self) -> None:
        """Reject inconsistent effects while the owner's transaction is active."""
        if (self.reconciliation_reversals or 0) < self.immediate_reversals:
            raise AssertionError(
                "reconciliation reversals cannot be fewer than immediate reversals"
            )

    @property
    def reconciliation_ran(self) -> bool:
        """Whether accepted decisions triggered transfer reconciliation."""
        return self.reconciliation_reversals is not None

    @property
    def immediate_reversals(self) -> int:
        """Accepted requests that reconciliation reversed in the same transaction."""
        return sum(
            change.changed
            and change.requested_status == "accepted"
            and change.effective_status == "reversed"
            for change in self.changes
        )

    @property
    def standing_transfers_retired(self) -> int:
        """Previously standing transfers retired by reconciliation."""
        return (self.reconciliation_reversals or 0) - self.immediate_reversals

    @property
    def accepted_count(self) -> int:
        """Changed requests that remained accepted after reconciliation."""
        return sum(
            change.changed and change.effective_status == "accepted"
            for change in self.changes
        )

    @property
    def effective_statuses(self) -> dict[str, str]:
        """Effective statuses keyed by the ids requested in this application."""
        return {change.match_id: change.effective_status for change in self.changes}


class MatchDecisionNotFoundError(ValueError):
    """Raised when a requested match decision no longer exists."""

    def __init__(self, match_id: str) -> None:
        """Store the unavailable decision id."""
        super().__init__(f"No match decision found for id {match_id!r}.")
        self.match_id = match_id


class MatchDecisionStateError(ValueError):
    """Raised when a terminal match decision is asked to change state."""

    def __init__(
        self,
        match_id: str,
        requested_status: SettableMatchStatus,
        current_status: str,
    ) -> None:
        """Store the rejected transition and its current state."""
        super().__init__(
            f"Cannot set match {match_id!r} to {requested_status!r}: it is "
            f"{current_status!r}, not pending."
        )
        self.match_id = match_id
        self.requested_status = requested_status
        self.current_status = current_status


class MatchDecisionApplication:
    """Apply matching decisions inside an outer transaction exactly once."""

    def __init__(
        self,
        db: Database,
        *,
        decisions: MatchDecisionsRepo,
        actor: str,
        decided_by: str = "user",
    ) -> None:
        """Bind the outer transaction, audited writer, and decision identity."""
        self._db = db
        self._decisions = decisions
        self._actor = actor
        self._decided_by = decided_by
        self._requested: list[tuple[str, SettableMatchStatus, str, bool]] = []
        self._seen: set[str] = set()
        self._bulk_called = False
        self._closed = False

    def set_status(self, match_id: str, *, status: SettableMatchStatus) -> None:
        """Request one idempotent or pending-to-terminal status change."""
        self._ensure_open()
        if self._bulk_called:
            raise ValueError("cannot mix bulk and explicit match decision requests")
        if match_id in self._seen:
            raise ValueError(f"match decision {match_id!r} appears more than once")
        row = get_match_decision(self._db, match_id)
        if row is None:
            raise MatchDecisionNotFoundError(match_id)
        prior = str(row["match_status"])
        if prior == status:
            changed = False
        elif prior != "pending":
            raise MatchDecisionStateError(match_id, status, prior)
        else:
            self._decisions.update_status(
                match_id,
                status=status,
                decided_by=self._decided_by,
                actor=self._actor,
                in_outer_txn=True,
            )
            changed = True
        self._seen.add(match_id)
        self._requested.append((match_id, status, prior, changed))

    def accept_pending(self, *, match_type: str | None = None) -> None:
        """Request acceptance of every currently pending match in an optional scope."""
        self._ensure_open()
        if self._requested:
            raise ValueError(
                "cannot accept pending after explicit match decision requests"
            )
        self._bulk_called = True
        match_ids = self._decisions.accept_pending(
            match_type=match_type,
            decided_by=self._decided_by,
            actor=self._actor,
            in_outer_txn=True,
        )
        for match_id in match_ids:
            self._seen.add(match_id)
            self._requested.append((match_id, "accepted", "pending", True))

    def finalize(self) -> MatchApplicationEffects:
        """Run reconciliation and alias forwarding; return transaction-local facts."""
        self._ensure_open()
        # An acceptance is what collapses rows together, so it is the trigger for
        # both follow-ups: reconciliation retires transfers the collapse
        # invalidated, and alias forwarding records the canonical ids it re-keyed.
        # A rejection changes neither.
        accepted_something = any(
            changed and status == "accepted"
            for _, status, _, changed in self._requested
        )
        reversals = (
            retire_transfers_invalidated_by_dedup(
                self._db,
                decisions=self._decisions,
                actor=self._actor,
                in_outer_txn=True,
            )
            if accepted_something
            else None
        )
        # After the reconciliation, not before: it can reverse a row this
        # application just accepted, and aliasing a merge that did not survive
        # the transaction would append a forwarding pointer to an id that never
        # went away — and the alias map is append-only, so it could not be taken
        # back.
        alias_forwarding = (
            forward_rekeyed_transaction_ids(
                self._db, actor=self._actor, in_outer_txn=True
            )
            if accepted_something
            else AliasForwardResult()
        )
        statuses = get_match_statuses(
            self._db, [match_id for match_id, _, _, _ in self._requested]
        )
        changes = tuple(
            MatchStatusChange(
                match_id=match_id,
                requested_status=status,
                prior_status=prior,
                effective_status=statuses[match_id],
                changed=changed,
            )
            for match_id, status, prior, changed in self._requested
        )
        self._closed = True
        return MatchApplicationEffects(
            changes=changes,
            reconciliation_reversals=reversals,
            alias_forwarding=alias_forwarding,
        )

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("match decision application is already finalized")


def record_committed_match_effects(effects: MatchApplicationEffects) -> None:
    """Record committed reconciliation metrics without risking committed work."""
    try:
        record_dedup_retirements(effects.reconciliation_reversals or 0)
        record_committed_alias_forwarding(effects.alias_forwarding)
    except Exception as exc:  # noqa: BLE001  # metrics must not escape post-commit
        logger.warning(
            f"Could not record committed matching metric at {exception_origin(exc)}"
        )
