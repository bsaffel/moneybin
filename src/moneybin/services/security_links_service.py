"""SecurityLinksService — merge decisions over security identity (M1G.4).

Mirrors :mod:`moneybin.services.merchant_links_service`: a thin service that
composes audited repos (``SecuritiesRepo``, ``SecurityLinksRepo``,
``SecurityLinkDecisionsRepo``, ``LotSelectionsRepo``,
``ManualInvestmentTransactionsRepo``) and coordinates their writes inside one
``db.begin()`` / ``db.commit()`` / ``db.rollback()`` transaction, each repo
called with ``in_outer_txn=True``.

``accept_merge`` is the app-state cascade for a provisional-security merge.
Within ONE transaction it re-points ``app.lot_selections`` at the survivor
(recomputing ``lot_id``, a content hash that includes ``security_id``),
re-points every accepted provider ref off the provisional security, re-points
every manual ledger row carrying the provisional's ``security_id`` directly
(``raw.manual_investment_transactions`` — user state resolved at entry, with no
link-table indirection), re-points the user's ``app.security_price_overrides``
marks, resolves the decision (auto-rejecting the ref's sibling candidates), and
deletes the provisional catalog row. A selection that cannot be deterministically
remapped BLOCKS the merge (``UserError``) rather than silently downgrading a
specific-ID election to FIFO on the next rebuild; so does a price mark that
would collide with one the survivor already holds.

The cascade's contract: after the merge, NOTHING still references the deleted
catalog row. That means every link-free reference — lot selections, manual
events, price marks — not only the ones a link table would carry. Atomicity is
the correctness bar: a half-applied merge (links re-pointed but lot selections
stranded, or the catalog row deleted while a manual event still points at it)
leaves cost basis silently wrong with no error raised and no doctor check to
catch it. A failed merge is retryable; a half-merge is not detectable.

``actor`` is the audit surface (``cli``/``mcp``); ``decided_by`` is the domain
column (``user``/``auto``). The caller supplies both.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

import duckdb

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.investments.cost_basis import compute_lot_id
from moneybin.metrics.registry import SECURITY_LINK_DECISION_OUTCOMES_TOTAL
from moneybin.repositories.lot_selections_repo import LotSelectionsRepo
from moneybin.repositories.manual_investment_transactions_repo import (
    ManualInvestmentTransactionsRepo,
)
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.repositories.security_link_decisions_repo import SecurityLinkDecisionsRepo
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.repositories.security_price_repo import SecurityPriceRepo
from moneybin.tables import (
    FCT_INVESTMENT_LOTS,
    FCT_INVESTMENT_TRANSACTIONS,
    LOT_SELECTIONS,
    SECURITIES,
    SECURITY_LINK_DECISIONS,
    SECURITY_LINKS,
    SECURITY_PRICE_OVERRIDES,
)

logger = logging.getLogger(__name__)

# (lot_id, quantity) pairs keyed by disposal — the shape LotSelectionsRepo takes.
_SelectionSet = dict[str, list[tuple[str, Decimal]]]

# Refs that name a market-data symbol rather than a second catalog row for one
# instrument. Accepting one BINDS the feed; accepting an identity ref MERGES two
# securities — opposite operations behind one reviewer intent, which is why
# `accept` routes on this set. Kept in step with what PriceService actually
# queues by test_price_service.py, which asserts every ref_kind the service files
# appears here: a new adapter whose ref_kind is missing would silently route its
# decisions into the merge path and destroy the security they were meant to price.
_FEED_KEY_REF_KINDS = frozenset({"tiingo_ticker", "coingecko_slug"})

# What an accepted decision actually did. A feed key BINDS (creates a link,
# touches nothing else); an identity ref MERGES (re-points every reference and
# deletes the provisional). Surfaces report the two differently.
AcceptOutcome = Literal["bound", "merged"]


@dataclass(frozen=True)
class PendingSecurityLinkCandidate:
    """One candidate merge-survivor proposal within a pending-review group."""

    decision_id: str
    candidate_security_id: str
    candidate_ticker: str | None
    candidate_name: str | None
    confidence: float | None
    match_reason: str | None


@dataclass(frozen=True)
class PendingSecurityLinkGroup:
    """One provider ref awaiting review + its candidate merge-survivor proposals."""

    ref_kind: str
    ref_value: str
    source_type: str
    provider_ticker: str | None
    provider_name: str | None
    candidates: tuple[PendingSecurityLinkCandidate, ...]


@dataclass(frozen=True)
class SecurityLinkAcceptImpact:
    """Stable identities and physical rows touched by a security merge."""

    provisional_security_id: str
    candidate_security_id: str
    lot_selection_disposal_ids: tuple[str, ...]
    blast_radius: dict[str, int]


class SecurityLinksService:
    """Accept/reject security merge proposals; count pending for review."""

    def __init__(self, db: Database, *, actor: str = "cli") -> None:
        """Initialize with a Database and the audit surface actor."""
        self._db = db
        self._actor = actor
        self._links = SecurityLinksRepo(db)
        self._decisions = SecurityLinkDecisionsRepo(db)
        self._securities = SecuritiesRepo(db)
        self._lot_selections = LotSelectionsRepo(db)
        self._manual_events = ManualInvestmentTransactionsRepo(db)

    # ------------------------------------------------------------------
    # Read-only methods
    # ------------------------------------------------------------------

    def count_pending(self) -> int:
        """Pending security-link decisions awaiting review (fresh DB -> 0)."""
        return self._decisions.count_pending()

    def list_pending(self) -> list[dict[str, Any]]:
        """Pending, non-reversed decisions ordered ``ref_value, decision_id``."""
        return self._decisions.list_pending()

    def pending(self) -> list[PendingSecurityLinkGroup]:
        """Pending decisions grouped by provider ref, candidates enriched with ticker/name.

        ``list_pending()`` rows carry only ``candidate_security_id`` — a bare
        id tells the reviewer nothing about whether the merge is right, so
        each candidate is enriched here with the catalog's ticker/name via a
        lookup against ``app.securities``. Grouped by ``(ref_kind, ref_value)``:
        the resolver files one decision per tied candidate for the same
        provider ref (an identifier tie), so a group — not the raw decision
        row — is the review unit, mirroring
        ``MerchantLinksService.pending()``. Read-only — no audit emitted.
        """
        rows = self._decisions.list_pending()
        if not rows:
            return []

        groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            key = (row["ref_kind"], row["ref_value"])
            groups.setdefault(key, []).append(row)

        result: list[PendingSecurityLinkGroup] = []
        for (ref_kind, ref_value), decisions in groups.items():
            first = decisions[0]
            candidates: list[PendingSecurityLinkCandidate] = []
            for d in decisions:
                ticker, name = self._security_display(d["candidate_security_id"])
                candidates.append(
                    PendingSecurityLinkCandidate(
                        decision_id=d["decision_id"],
                        candidate_security_id=d["candidate_security_id"],
                        candidate_ticker=ticker,
                        candidate_name=name,
                        confidence=d["confidence_score"],
                        match_reason=d.get("match_reason"),
                    )
                )
            result.append(
                PendingSecurityLinkGroup(
                    ref_kind=ref_kind,
                    ref_value=ref_value,
                    source_type=first["source_type"],
                    provider_ticker=first.get("provider_ticker"),
                    provider_name=first.get("provider_name"),
                    candidates=tuple(candidates),
                )
            )
        return result

    def history(self, *, limit: int | None = 50) -> list[dict[str, Any]]:
        """All decisions (any status) newest-first by ``decided_at``. Read-only."""
        return self._decisions.history(limit=limit)

    def decision_by_id(self, decision_id: str) -> dict[str, Any] | None:
        """Return one exact decision row by ID."""
        return self._decisions.fetch_by_id(decision_id)

    def accept_impact(
        self,
        decision_id: str,
        *,
        into: str,
    ) -> SecurityLinkAcceptImpact:
        """Preview stable identities and rows the security merge will mutate."""
        decision = self._require_pending(decision_id)
        if into != decision["candidate_security_id"]:
            raise UserError(
                "into does not match the candidate named in decision "
                f"{decision_id!r}; pass the decision's own "
                "candidate_security_id as a confirming safety check.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        survivor = str(decision["candidate_security_id"])
        provisional = self._links.lookup(
            ref_kind=decision["ref_kind"],
            ref_value=decision["ref_value"],
            source_type=decision["source_type"],
        )
        if provisional is None:
            raise UserError(
                "No accepted binding exists for the provider ref under review; "
                f"decision {decision_id!r} has nothing to merge away.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        if provisional == survivor:
            raise UserError(
                f"The ref in decision {decision_id!r} is already bound to the "
                "candidate security; there is nothing to merge.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        if self._security_created_by(provisional) != "plaid":
            raise UserError(
                f"The security bound to decision {decision_id!r} is "
                "user-authored; user-authored securities are never merged away.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        if not self._security_exists(survivor):
            raise UserError(
                f"No security found for id {survivor!r}.",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        plan = self._plan_lot_selections(provisional, survivor)
        lot_selection_count = sum(
            len(self._lot_selections.list_for_disposal(disposal_id))
            for disposal_id in plan
        )
        link_count_row = self._db.execute(
            f"SELECT COUNT(*) FROM {SECURITY_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized value
            "WHERE security_id = ? AND status = 'accepted'",
            [provisional],
        ).fetchone()
        sibling_count_row = self._db.execute(
            f"""
            SELECT COUNT(*) FROM {SECURITY_LINK_DECISIONS.full_name}
            WHERE source_type = ?
              AND ref_kind = ?
              AND ref_value = ?
              AND decision_id != ?
              AND status = 'pending'
              AND reversed_at IS NULL
            """,  # noqa: S608  # TableRef constants + parameterized values
            [
                decision["source_type"],
                decision["ref_kind"],
                decision["ref_value"],
                decision_id,
            ],
        ).fetchone()
        return SecurityLinkAcceptImpact(
            provisional_security_id=provisional,
            candidate_security_id=survivor,
            lot_selection_disposal_ids=tuple(sorted(plan)),
            blast_radius={
                "securities": 2,
                "security_links": int(link_count_row[0]) if link_count_row else 0,
                "security_link_decisions": (
                    1 + int(sibling_count_row[0]) if sibling_count_row else 1
                ),
                "lot_selections": lot_selection_count,
                "manual_investment_transactions": len(
                    self._manual_events.list_ids_for_security(provisional)
                ),
                "security_price_overrides": self._mark_count(provisional),
            },
        )

    def _mark_count(self, security_id: str) -> int:
        """User price marks the merge will move — part of its blast radius.

        Counted here because the confirm preview binds approval to this figure:
        a row the merge mutates but the preview omits is a write the user never
        agreed to.
        """
        row = self._db.execute(
            f"SELECT COUNT(*) FROM {SECURITY_PRICE_OVERRIDES.full_name} "  # noqa: S608  # TableRef + parameterized value
            "WHERE security_id = ?",
            [security_id],
        ).fetchone()
        return int(row[0]) if row is not None else 0

    def _security_display(self, security_id: str) -> tuple[str | None, str | None]:
        """(ticker, name) for ``security_id`` from ``app.securities``; ``(None, None)`` if absent."""
        row = self._db.execute(
            f"SELECT ticker, name FROM {SECURITIES.full_name} "  # noqa: S608  # TableRef + parameterized value
            "WHERE security_id = ? LIMIT 1",
            [security_id],
        ).fetchone()
        return (row[0], row[1]) if row is not None else (None, None)

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    @staticmethod
    def binds_a_feed_key(ref_kind: str) -> bool:
        """Whether accepting this ref binds a price feed rather than merging identities.

        Public because the coarse batch preflight has to route the same way
        ``accept`` does, one step earlier. Re-deriving the rule from
        ``_FEED_KEY_REF_KINDS`` at that second call site is exactly how the coarse
        path came to run every accept through the merge preflight while the
        fine-grained one routed correctly.
        """
        return ref_kind in _FEED_KEY_REF_KINDS

    def _require_bindable(
        self,
        decision: dict[str, Any],
        *,
        decision_id: str,
        into: str,
    ) -> None:
        """Refusals shared by the feed-key preflight and the bind that follows it."""
        if into != decision["candidate_security_id"]:
            raise UserError(
                "into does not match the candidate named in decision "
                f"{decision_id!r}; pass the decision's own "
                "candidate_security_id as a confirming safety check.",
                code=error_codes.MUTATION_INVALID_INPUT,
            )
        if not self._security_exists(into):
            raise UserError(
                f"Security {into!r} no longer exists, so a price feed cannot "
                "be bound to it.",
                code=error_codes.MUTATION_NOT_FOUND,
            )

    def bind_impact(self, decision_id: str, *, into: str) -> None:
        """Preflight an accepted feed-key decision before a batch's first write.

        The merge path exposes ``accept_impact`` so a batch discovers its refusals
        before mutating anything; this is that step for a bind. It previews no rows
        because a bind mutates none — it creates the link that did not exist — so it
        asserts only what ``accept_feed_key`` asserts, through the same helper, and
        the guard cannot drift from the write it guards.
        """
        decision = self._require_pending(decision_id)
        self._require_bindable(decision, decision_id=decision_id, into=into)

    def accept(
        self,
        decision_id: str,
        *,
        into: str,
        decided_by: str = "user",
        verify_accept: Callable[[SecurityLinkAcceptImpact], None] | None = None,
        in_outer_txn: bool = False,
    ) -> AcceptOutcome:
        """Accept one pending decision by whichever mechanism its ref_kind needs.

        The queue holds two kinds of question that look identical to a reviewer
        and are structurally opposite to resolve:

        - An **identity** ref (``plaid_security_id``, ``institution_security_id``)
          asks whether two catalog rows are one instrument. Accepting MERGES —
          re-points every reference onto the survivor and deletes the provisional.
        - A **feed-key** ref (``tiingo_ticker``, ``coingecko_slug``) asks whether
          a market-data symbol names this security. Accepting BINDS — it creates
          the link that did not exist, and touches nothing else.

        Routing every acceptance through the merge path made feed-key decisions
        unacceptable by construction: the merge requires an accepted binding to
        move, and a feed key has none — that absence is exactly why PriceService
        queued it. The decision stayed pending forever and the security stayed
        unpriced with no way to resolve it.

        One entry point rather than two commands: the reviewer's intent is the
        same ("yes, this pairing is right"), and the pending queue mixes both
        kinds, so asking the caller to pick the mechanism would leak an
        implementation detail into the surface.

        Returns which mechanism ran. The caller cannot see the routing and the
        two outcomes are opposite, so a surface that assumed one would tell the
        user it merged two securities when it only created a link. Re-deriving
        it from ``_FEED_KEY_REF_KINDS`` in each adapter would put the routing
        rule in two places instead.

        ``in_outer_txn`` lets the coarse batch enter through this same router
        rather than calling ``accept_merge`` directly. That call is what made the
        batch surface merge-only while this one routed correctly.
        """
        decision = self._require_pending(decision_id)
        if self.binds_a_feed_key(str(decision["ref_kind"])):
            self.accept_feed_key(
                decision_id,
                into=into,
                decided_by=decided_by,
                in_outer_txn=in_outer_txn,
            )
            return "bound"
        self.accept_merge(
            decision_id,
            into=into,
            decided_by=decided_by,
            verify_accept=verify_accept,
            in_outer_txn=in_outer_txn,
        )
        return "merged"

    def accept_feed_key(
        self,
        decision_id: str,
        *,
        into: str,
        decided_by: str = "user",
        in_outer_txn: bool = False,
    ) -> None:
        """Bind a reviewed market-data symbol to the security under review.

        Not a merge: no catalog row is deleted and no lot, manual event, or price
        mark moves. The binding simply did not exist — ``PriceService`` refused to
        create it silently because the symbol was ambiguous or the provider's
        metadata disagreed — and the reviewer is supplying the certainty the
        derivation lacked.

        In ONE transaction: mark the decision accepted (its audit id parents the
        binding, so the pair undoes together), insert the accepted link, and
        auto-reject the ref's sibling candidates — accepting one answers them all.

        Raises ``UserError`` when the decision is unknown or not pending, when
        ``into`` does not match the decision's own ``candidate_security_id`` (the
        same confirming safety check the merge path applies), or when that
        security no longer exists.
        """
        if not in_outer_txn:
            self._db.begin()
        try:
            decision = self._require_pending(decision_id)
            self._require_bindable(decision, decision_id=decision_id, into=into)
            event = self._decisions.update_status(
                decision_id,
                status="accepted",
                decided_by=decided_by,
                actor=self._actor,
                in_outer_txn=True,
            )
            self._links.insert(
                security_id=into,
                ref_kind=str(decision["ref_kind"]),
                ref_value=str(decision["ref_value"]),
                source_type=str(decision["source_type"]),
                decided_by=decided_by,
                actor=self._actor,
                parent_audit_id=event.audit_id,
                in_outer_txn=True,
            )
            self._reject_pending_siblings(
                decision,
                exclude=decision_id,
                decided_by=decided_by,
                parent_audit_id=event.audit_id,
            )
            if not in_outer_txn:
                self._db.commit()
        except BaseException:
            if not in_outer_txn:
                self._db.rollback()
            raise
        if in_outer_txn:
            return

        SECURITY_LINK_DECISION_OUTCOMES_TOTAL.labels(outcome="accepted").inc()
        logger.info(
            f"feed key bound: decision={decision_id} security={into} "
            f"ref_kind={decision['ref_kind']}"
        )

        from moneybin.services.security_resolver import (  # noqa: PLC0415
            refresh_security_link_pending_gauge,
        )

        refresh_security_link_pending_gauge(self._db)

    def reject_merge(
        self,
        decision_id: str,
        *,
        decided_by: str = "user",
        in_outer_txn: bool = False,
    ) -> None:
        """Reject one merge proposal; the provisional security is kept.

        The reviewer is asserting the provider security genuinely is a distinct
        instrument. The declined pairing lands in the resolver's rejected set
        (``list_rejected``), so it is never re-proposed — otherwise the review
        queue would never drain.

        Sibling candidates for the same ref stay **pending**: rejecting one
        candidate answers only that pairing, not the question of whether some
        other candidate is the same instrument. (This is where the merchant
        service's ``--new`` reject-all differs — there, rejecting means "mint a
        new merchant", which does answer every candidate at once.)

        Raises ``UserError`` when the decision is unknown or not pending.
        """
        if not in_outer_txn:
            self._db.begin()
        try:
            self._require_pending(decision_id)
            self._decisions.update_status(
                decision_id,
                status="rejected",
                decided_by=decided_by,
                actor=self._actor,
                in_outer_txn=True,
            )
            if not in_outer_txn:
                self._db.commit()
        except BaseException:
            if not in_outer_txn:
                self._db.rollback()
            raise
        if in_outer_txn:
            return
        SECURITY_LINK_DECISION_OUTCOMES_TOTAL.labels(outcome="rejected").inc()
        logger.info(f"security merge rejected: decision={decision_id}")

        # Rejecting changed the pending count — refresh the gauge.
        from moneybin.services.security_resolver import (  # noqa: PLC0415
            refresh_security_link_pending_gauge,
        )

        refresh_security_link_pending_gauge(self._db)

    def record_committed_outer_outcomes(self, outcomes: tuple[str, ...]) -> None:
        """Record metrics after an enclosing transaction commits."""
        for outcome in outcomes:
            SECURITY_LINK_DECISION_OUTCOMES_TOTAL.labels(outcome=outcome).inc()
        from moneybin.services.security_resolver import (  # noqa: PLC0415
            refresh_security_link_pending_gauge,
        )

        refresh_security_link_pending_gauge(self._db)

    def accept_merge(
        self,
        decision_id: str,
        *,
        into: str,
        decided_by: str = "user",
        verify_accept: Callable[[SecurityLinkAcceptImpact], None] | None = None,
        in_outer_txn: bool = False,
    ) -> None:
        """Merge the provisional security into the decision's candidate, atomically.

        ``into`` is a confirming safety check (mirrors
        :class:`~moneybin.services.merchant_links_service.MerchantLinksService.set`):
        it must equal the decision's own ``candidate_security_id``, so the
        caller cannot accidentally merge into a different security than the
        one it reviewed — this matters most on a tied group, where the
        resolver files one decision per candidate and a wrong pick both
        merges into the wrong security AND auto-rejects the right one.

        In ONE transaction:

        1. Resolve the provisional security = the ref's current accepted binding;
           the survivor = the decision's ``candidate_security_id``.
        2. Plan the lot-selection migration, and **block** (``UserError``) when
           any affected selection cannot be deterministically remapped.
        3. Mark the decision ``accepted`` — its audit id is the ``parent_audit_id``
           of every child write below, so the whole merge undoes as one chain.
        4. Re-point ``app.lot_selections`` at the survivor's re-hashed lots.
        5. Re-point EVERY accepted link on the provisional (the plaid ref and the
           institution ref both) onto the survivor.
        6. Re-point every ``raw.manual_investment_transactions`` row that carries
           the provisional's ``security_id`` — the ledger's other, link-free
           reference to the catalog (see :meth:`_repoint_manual_events`).
        7. Re-point the user's ``app.security_price_overrides`` marks — the
           fourth link-free reference (see :meth:`_repoint_price_marks`).
        8. Auto-reject the ref's sibling pending candidates — accepting one answers
           them all, so a tie resolves in a single review action.
        9. Delete the provisional ``created_by='plaid'`` catalog row.

        Steps 4-9 must succeed or fail together: a merge that re-points the link
        but strands a lot selection silently corrupts cost basis, one that
        deletes the catalog row but strands a manual event splits the
        instrument's position across a live security and a dead one, and one
        that strands a price mark silently stops the user's own valuation from
        applying.

        Raises ``UserError`` when:
        - ``decision_id`` is unknown (MUTATION_NOT_FOUND) or not ``pending``
          (MUTATION_CONSTRAINT_VIOLATION) — a decision never decides twice.
        - ``into`` does not match the decision's ``candidate_security_id``
          (MUTATION_INVALID_INPUT) — pass the decision's own candidate id.
        - The ref has no accepted binding to merge away
          (MUTATION_CONSTRAINT_VIOLATION).
        - The ref is already bound to the candidate — nothing to merge
          (MUTATION_CONSTRAINT_VIOLATION).
        - The bound security is user-authored (``created_by='user'``) —
          user-authored catalog rows are never merged away
          (MUTATION_CONSTRAINT_VIOLATION). Not reachable via
          ``SecurityResolver`` today (it only proposes plaid-minted
          provisionals), but ``SecuritiesRepo.delete`` enforces this too, so
          the merge must never depend on reaching that check.
        - The candidate security no longer exists (MUTATION_NOT_FOUND) — it must
          not become the ref's new binding.
        - A lot selection cannot be remapped, or ``core`` is not materialized and
          selections exist (MUTATION_CONSTRAINT_VIOLATION).
        """
        if not in_outer_txn:
            self._db.begin()
        try:
            decision = self._require_pending(decision_id)
            if into != decision["candidate_security_id"]:
                raise UserError(
                    "into does not match the candidate named in decision "
                    f"{decision_id!r}; pass the decision's own "
                    "candidate_security_id as a confirming safety check.",
                    code=error_codes.MUTATION_INVALID_INPUT,
                )
            survivor = str(decision["candidate_security_id"])
            provisional = self._links.lookup(
                ref_kind=decision["ref_kind"],
                ref_value=decision["ref_value"],
                source_type=decision["source_type"],
            )
            if provisional is None:
                raise UserError(
                    "No accepted binding exists for the provider ref under review; "
                    f"decision {decision_id!r} has nothing to merge away.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            if provisional == survivor:
                raise UserError(
                    f"The ref in decision {decision_id!r} is already bound to the "
                    "candidate security; there is nothing to merge.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            if self._security_created_by(provisional) != "plaid":
                # SecuritiesRepo.delete enforces the same rule at the LAST
                # write of the cascade (step 9); this pre-write check moves
                # the refusal ahead of the first write, per "Plan (and
                # validate) BEFORE the first write" below.
                raise UserError(
                    f"The security bound to decision {decision_id!r} is "
                    "user-authored; user-authored securities are never merged "
                    "away.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )
            if not self._security_exists(survivor):
                raise UserError(
                    f"No security found for id {survivor!r}.",
                    code=error_codes.MUTATION_NOT_FOUND,
                )

            # Plan (and validate) BEFORE the first write: a blocked merge should
            # not depend on rollback to leave the database untouched.
            plan = self._plan_lot_selections(provisional, survivor)
            if verify_accept is not None:
                verify_accept(self.accept_impact(decision_id, into=into))

            event = self._decisions.update_status(
                decision_id,
                status="accepted",
                decided_by=decided_by,
                actor=self._actor,
                in_outer_txn=True,
            )
            parent_audit_id = event.audit_id

            for disposal_id, selections in plan.items():
                self._lot_selections.set_for_disposal(
                    investment_transaction_id=disposal_id,
                    selections=selections,
                    actor=self._actor,
                    parent_audit_id=parent_audit_id,
                    in_outer_txn=True,
                )
            self._repoint_links(
                provisional,
                survivor,
                decided_by=decided_by,
                parent_audit_id=parent_audit_id,
            )
            manual_repointed = self._repoint_manual_events(
                provisional, survivor, parent_audit_id=parent_audit_id
            )
            marks_repointed = self._repoint_price_marks(
                provisional, survivor, parent_audit_id=parent_audit_id
            )
            self._reject_pending_siblings(
                decision,
                exclude=decision_id,
                decided_by=decided_by,
                parent_audit_id=parent_audit_id,
            )
            self._securities.delete(
                provisional,
                actor=self._actor,
                parent_audit_id=parent_audit_id,
                in_outer_txn=True,
            )
            if not in_outer_txn:
                self._db.commit()
        except BaseException:
            if not in_outer_txn:
                self._db.rollback()
            raise
        if in_outer_txn:
            return

        SECURITY_LINK_DECISION_OUTCOMES_TOTAL.labels(outcome="accepted").inc()
        logger.info(
            f"security merge accepted: decision={decision_id} "
            f"provisional={provisional} survivor={survivor} "
            f"disposals_remapped={len(plan)} "
            f"manual_events_repointed={manual_repointed} "
            f"price_marks_repointed={marks_repointed}"
        )

        # Accepting changed the pending count (the named decision plus its
        # auto-rejected siblings) — refresh the gauge.
        from moneybin.services.security_resolver import (  # noqa: PLC0415
            refresh_security_link_pending_gauge,
        )

        refresh_security_link_pending_gauge(self._db)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def decisions_for_ref(
        self, *, ref_kind: str, ref_value: str, source_type: str
    ) -> int:
        """Pending decisions competing for one provider ref.

        Accepting any one of them resolves the whole group — the winner is
        accepted and its siblings auto-rejected — so this is the decision-row
        count a confirmation's blast radius must state. Read-only.
        """
        row = self._db.execute(
            f"SELECT COUNT(*) FROM {SECURITY_LINK_DECISIONS.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE status = 'pending' AND ref_kind = ? AND ref_value = ? "
            "AND source_type = ?",
            [ref_kind, ref_value, source_type],
        ).fetchone()
        return int(row[0]) if row is not None else 0

    def _require_pending(self, decision_id: str) -> dict[str, Any]:
        """Fetch the decision, or raise ``UserError`` unless it is pending."""
        decision = self._decisions.fetch_by_id(decision_id)
        if decision is None:
            raise UserError(
                f"No security-link decision found for id {decision_id!r}.",
                code=error_codes.MUTATION_NOT_FOUND,
            )
        if decision["status"] != "pending":
            raise UserError(
                f"Decision {decision_id!r} is {decision['status']!r}, not pending.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        return decision

    def _security_exists(self, security_id: str) -> bool:
        """True when ``security_id`` is present in ``app.securities``."""
        row = self._db.execute(
            f"SELECT 1 FROM {SECURITIES.full_name} "  # noqa: S608  # TableRef + parameterized value
            "WHERE security_id = ? LIMIT 1",
            [security_id],
        ).fetchone()
        return row is not None

    def _security_created_by(self, security_id: str) -> str | None:
        """``created_by`` for ``security_id``, or ``None`` if it doesn't exist."""
        row = self._db.execute(
            f"SELECT created_by FROM {SECURITIES.full_name} "  # noqa: S608  # TableRef + parameterized value
            "WHERE security_id = ? LIMIT 1",
            [security_id],
        ).fetchone()
        return str(row[0]) if row is not None else None

    def _plan_lot_selections(self, provisional: str, survivor: str) -> _SelectionSet:
        """Compute the post-merge selection set for every disposal on the provisional.

        ``lot_id`` hashes ``security_id``, so the merge re-keys every lot of the
        provisional security. Each selection on one of its disposals therefore
        falls into exactly one of three cases:

        - its lot belongs to the provisional -> re-hash onto the survivor;
        - its lot already belongs to the survivor -> keep it (valid post-merge);
        - anything else (the ``lot_id`` resolves to no lot, or to a lot of some
          third security) -> **unremappable**: after the merge the disposal draws
          from the survivor's pool, which that lot will never join, so the
          engine would silently drop the election and fall back to FIFO. Block.

        Two selections on the same disposal can re-hash onto the SAME
        ``new_lot_id`` — e.g. the survivor already holds a lot at the exact
        ``(account_id, acquisition_date, source_transaction_id)`` a
        provisional lot remaps onto. Post-merge those genuinely ARE one lot,
        so their quantities are summed rather than written as two rows: a
        duplicate ``lot_id`` for one disposal would violate
        ``lot_selections``'s ``(investment_transaction_id, lot_id)`` primary
        key.

        Returns the full replacement set per touched disposal (unchanged disposals
        omitted) — ``set_for_disposal`` is a whole-set replace, so a partial set
        would delete the selections it left out.

        Deliberately exempt from the elected-method precondition
        ``InvestmentService.select_lots`` enforces (only 'specific' consumes lot
        selections). A merge is data repair, not a new election: refusing to
        carry selections across because the *survivor* resolves to FIFO would
        strand them on a security id that no longer exists. They land inert
        instead, which is recoverable — electing 'specific' on the survivor
        reactivates them.
        """
        try:
            rows = self._db.execute(
                f"""
                SELECT ls.investment_transaction_id, ls.lot_id, ls.quantity,
                       l.security_id, l.account_id, l.acquisition_date,
                       l.source_transaction_id
                FROM {LOT_SELECTIONS.full_name} AS ls
                JOIN {FCT_INVESTMENT_TRANSACTIONS.full_name} AS t
                  ON t.investment_transaction_id = ls.investment_transaction_id
                LEFT JOIN {FCT_INVESTMENT_LOTS.full_name} AS l
                  ON l.lot_id = ls.lot_id
                WHERE t.security_id = ?
                ORDER BY ls.investment_transaction_id, ls.lot_id
                """,  # noqa: S608  # TableRef constants + parameterized value
                [provisional],
            ).fetchall()
        except duckdb.CatalogException:
            # core is not materialized, so remappability cannot be verified. With
            # no selections at all there is nothing to migrate and the merge is
            # safe; otherwise refuse rather than guess.
            if self._lot_selection_count() == 0:
                return {}
            raise UserError(
                "Cannot accept this merge: the core investment models have not "
                "been materialized, so the lot selections that would have to "
                "migrate cannot be verified. Run a transform first.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            ) from None

        before: _SelectionSet = {}
        after_quantities: dict[str, dict[str, Decimal]] = {}
        unremappable = 0
        for row in rows:
            disposal_id = str(row[0])
            lot_id, quantity = str(row[1]), Decimal(row[2])
            lot_security = row[3]
            before.setdefault(disposal_id, []).append((lot_id, quantity))
            if lot_security == provisional:
                new_lot_id = compute_lot_id(str(row[4]), survivor, row[5], str(row[6]))
            elif lot_security == survivor:
                new_lot_id = lot_id
            else:
                unremappable += 1
                continue
            totals = after_quantities.setdefault(disposal_id, {})
            totals[new_lot_id] = totals.get(new_lot_id, Decimal("0")) + quantity

        if unremappable:
            raise UserError(
                f"Merge blocked: {unremappable} lot selection(s) cannot be "
                "deterministically remapped onto the surviving security. "
                "Accepting would silently downgrade a specific-identification "
                "sale to FIFO. Clear or correct those selections, then retry.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        after: _SelectionSet = {
            disposal_id: sorted(totals.items())
            for disposal_id, totals in after_quantities.items()
        }
        return {
            disposal_id: selections
            for disposal_id, selections in after.items()
            if selections != before[disposal_id]
        }

    def _lot_selection_count(self) -> int:
        """Whole-table count, deliberately over-broad.

        Used only as the "any selections exist at all" fallback when ``core``
        is absent (see the ``CatalogException`` handler above): with no
        ``core.fct_investment_lots``/``fct_investment_transactions``, there is
        no way to tell whether a given selection belongs to the disposal
        being merged, so this blocks a merge even when every selection is on
        an unrelated security. That's the safe direction — remappability
        genuinely can't be verified without ``core`` — not a bug to narrow.
        """
        row = self._db.execute(
            f"SELECT COUNT(*) FROM {LOT_SELECTIONS.full_name}"  # noqa: S608  # TableRef constant
        ).fetchone()
        return int(row[0]) if row else 0

    def _repoint_manual_events(
        self, provisional: str, survivor: str, *, parent_audit_id: str | None
    ) -> int:
        """Re-point every manual ledger row that references the provisional security.

        The ledger's OTHER reference to ``security_id``, alongside the provider
        refs ``_repoint_links`` moves. ``raw.manual_investment_transactions`` is
        user-entered state, not provider-owned raw: ``investments record``
        resolves the security at entry and stores the resolved id, and
        ``stg_manual__investment_transactions`` carries it verbatim — no link
        table sits in between, so the link repoint does not move it, and nothing
        restricts a manual entry to ``created_by='user'`` catalog rows in the
        first place.

        Left behind, those rows would point at the catalog id step 9 deletes:
        ``core.fct_investment_lots`` would keep building lots under a security
        that no longer exists while the Plaid side moved to the survivor, so the
        user's single real position is split across a live security and a dead
        one and BOTH cost bases are computed on a partial pool. There is no FK
        and no doctor check between the investment fact and the catalog, so
        ``moneybin doctor`` would report clean.

        Audited through a repo (Invariant 10's contract, applied to a table that
        is nominally ``raw`` but is really user state) and threaded onto the
        merge's ``parent_audit_id``, so the repoint undoes with the rest of the
        cascade rather than stranding the ledger on the survivor after an undo.
        Runs inside the caller's open transaction. Returns the row count.
        """
        source_ids = self._manual_events.list_ids_for_security(provisional)
        for source_transaction_id in source_ids:
            self._manual_events.repoint_security(
                source_transaction_id=source_transaction_id,
                new_security_id=survivor,
                actor=self._actor,
                parent_audit_id=parent_audit_id,
                in_outer_txn=True,
            )
        return len(source_ids)

    def _repoint_price_marks(
        self, provisional: str, survivor: str, *, parent_audit_id: str | None
    ) -> int:
        """Move the user's own price marks onto the survivor.

        ``app.security_price_overrides.security_id`` is the FOURTH link-free
        reference to ``app.securities``, beside lot selections, links, and manual
        events. No FK constrains it, so step 9's catalog delete leaves the mark
        behind rather than failing: ``core.fct_security_prices`` keeps unioning it
        under the dead id, ``core.dim_holdings`` joins it to nothing, and the
        surviving position quietly falls back to provider pricing — the user's
        explicit valuation stops applying with no warning on any surface.

        Refuses when a mark on each side shares a ``(price_date,
        quote_currency)``. The composite primary key admits only one, and choosing
        between two numbers the user typed is not a decision this merge can make
        silently — the same reasoning that makes an unremappable lot selection
        block rather than guess.

        Spelled as delete-then-set rather than a primary-key update so it reuses
        the repo's audited primitives and their full-row capture; the two events
        thread onto the merge's ``parent_audit_id`` and reverse in order, so an
        undo puts the mark back on the provisional.

        That spelling costs one thing an UPDATE would have kept for free, so the
        original ``created_at`` is carried across explicitly: the pair never hits
        ``set``'s ``ON CONFLICT`` branch, and letting the insert default stamp the
        merge time would rewrite when the user authored a number they typed months
        earlier — while the delete event sitting beside it still records the truth.
        """
        rows = self._db.execute(
            f"SELECT price_date, quote_currency, close, note, created_at "  # noqa: S608  # TableRef constant
            f"FROM {SECURITY_PRICE_OVERRIDES.full_name} WHERE security_id = ? "
            "ORDER BY price_date, quote_currency",
            [provisional],
        ).fetchall()
        if not rows:
            return 0
        clash = self._db.execute(
            f"SELECT COUNT(*) FROM {SECURITY_PRICE_OVERRIDES.full_name} AS a "  # noqa: S608  # TableRef constant
            f"JOIN {SECURITY_PRICE_OVERRIDES.full_name} AS b "
            "ON b.price_date = a.price_date AND b.quote_currency = a.quote_currency "
            "WHERE a.security_id = ? AND b.security_id = ?",
            [provisional, survivor],
        ).fetchone()
        if clash is not None and int(clash[0]) > 0:
            raise UserError(
                "Both securities carry a price mark for the same date and "
                "currency, and only one can survive the merge. Delete the mark "
                "you do not want with 'moneybin investments prices delete', then "
                "accept this decision again.",
                code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
            )
        marks = SecurityPriceRepo(self._db)
        for price_date, quote_currency, close, note, created_at in rows:
            marks.delete(
                provisional,
                price_date,
                str(quote_currency),
                actor=self._actor,
                parent_audit_id=parent_audit_id,
                in_outer_txn=True,
            )
            marks.set(
                survivor,
                price_date,
                str(quote_currency),
                close=close,
                note=note,
                actor=self._actor,
                created_at=created_at,
                parent_audit_id=parent_audit_id,
                in_outer_txn=True,
            )
        return len(rows)

    def _repoint_links(
        self,
        provisional: str,
        survivor: str,
        *,
        decided_by: str,
        parent_audit_id: str | None,
    ) -> None:
        """Re-point every accepted link on the provisional onto the survivor.

        Not only the ref under review: the provisional also holds the sibling
        institution-scoped ref (the resolver binds both). Leaving it behind
        would orphan it on a deleted security and mis-adopt the next sync's row.
        Runs inside the caller's open transaction.
        """
        link_ids = self._db.execute(
            f"SELECT link_id FROM {SECURITY_LINKS.full_name} "  # noqa: S608  # TableRef + parameterized value
            "WHERE security_id = ? AND status = 'accepted' ORDER BY link_id",
            [provisional],
        ).fetchall()
        for (link_id,) in link_ids:
            self._links.repoint(
                link_id=str(link_id),
                new_security_id=survivor,
                decided_by=decided_by,
                actor=self._actor,
                parent_audit_id=parent_audit_id,
                in_outer_txn=True,
            )

    def _reject_pending_siblings(
        self,
        decision: dict[str, Any],
        *,
        exclude: str,
        decided_by: str,
        parent_audit_id: str | None,
    ) -> None:
        """Auto-reject the other pending candidates proposed for the same ref.

        The resolver files one decision per tied candidate; accepting one answers
        all of them, so the human is asked once, not N times. Scoped to the full
        ref key ``(source_type, ref_kind, ref_value)`` so a ``ref_value`` shared
        across ref kinds or providers never cross-rejects. Runs inside the
        caller's open transaction.
        """
        sibling_ids = self._db.execute(
            f"""
            SELECT decision_id FROM {SECURITY_LINK_DECISIONS.full_name}
            WHERE source_type = ?
              AND ref_kind = ?
              AND ref_value = ?
              AND decision_id != ?
              AND status = 'pending'
              AND reversed_at IS NULL
            ORDER BY decision_id
            """,  # noqa: S608  # TableRef + parameterized values
            [
                decision["source_type"],
                decision["ref_kind"],
                decision["ref_value"],
                exclude,
            ],
        ).fetchall()
        for (sibling_id,) in sibling_ids:
            self._decisions.update_status(
                str(sibling_id),
                status="rejected",
                decided_by=decided_by,
                actor=self._actor,
                parent_audit_id=parent_audit_id,
                in_outer_txn=True,
            )
