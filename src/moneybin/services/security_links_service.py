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
link-table indirection), resolves the decision (auto-rejecting the ref's sibling
candidates), and deletes the provisional catalog row. A selection that cannot be
deterministically remapped BLOCKS the merge (``UserError``) rather than silently
downgrading a specific-ID election to FIFO on the next rebuild.

The cascade's contract: after the merge, NOTHING still references the deleted
catalog row. Atomicity is the correctness bar — a half-applied merge (links
re-pointed but lot selections stranded, or the catalog row deleted while a
manual event still points at it) leaves cost basis silently wrong with no error
raised and no doctor check to catch it. A failed merge is retryable; a
half-merge is not detectable.

``actor`` is the audit surface (``cli``/``mcp``); ``decided_by`` is the domain
column (``user``/``auto``). The caller supplies both.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

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
from moneybin.tables import (
    FCT_INVESTMENT_LOTS,
    FCT_INVESTMENT_TRANSACTIONS,
    LOT_SELECTIONS,
    SECURITIES,
    SECURITY_LINK_DECISIONS,
    SECURITY_LINKS,
)

logger = logging.getLogger(__name__)

# (lot_id, quantity) pairs keyed by disposal — the shape LotSelectionsRepo takes.
_SelectionSet = dict[str, list[tuple[str, Decimal]]]


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
            },
        )

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
        7. Auto-reject the ref's sibling pending candidates — accepting one answers
           them all, so a tie resolves in a single review action.
        8. Delete the provisional ``created_by='plaid'`` catalog row.

        Steps 4-8 must succeed or fail together: a merge that re-points the link
        but strands a lot selection silently corrupts cost basis, and one that
        deletes the catalog row but strands a manual event splits the
        instrument's position across a live security and a dead one.

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
                # write of the cascade (step 7); this pre-write check moves
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
            f"manual_events_repointed={manual_repointed}"
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

        Left behind, those rows would point at the catalog id step 7 deletes:
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
