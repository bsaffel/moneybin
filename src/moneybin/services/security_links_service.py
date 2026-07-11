"""SecurityLinksService — merge decisions over security identity (M1G.4).

Mirrors :mod:`moneybin.services.merchant_links_service`: a thin service that
composes Invariant-10 repos (``SecuritiesRepo``, ``SecurityLinksRepo``,
``SecurityLinkDecisionsRepo``, ``LotSelectionsRepo``) and coordinates their
writes inside one ``db.begin()`` / ``db.commit()`` / ``db.rollback()``
transaction, each repo called with ``in_outer_txn=True``.

``accept_merge`` is the app-state cascade for a provisional-security merge.
Within ONE transaction it re-points ``app.lot_selections`` at the survivor
(recomputing ``lot_id``, a content hash that includes ``security_id``),
re-points every accepted provider ref off the provisional security, resolves the
decision (auto-rejecting the ref's sibling candidates), and deletes the
provisional catalog row. A selection that cannot be deterministically remapped
BLOCKS the merge (``UserError``) rather than silently downgrading a specific-ID
election to FIFO on the next rebuild.

Atomicity is the correctness bar: a half-applied merge — links re-pointed but
lot selections stranded, or vice versa — leaves cost basis silently wrong with
no error raised. A failed merge is retryable; a half-merge is not detectable.

``actor`` is the audit surface (``cli``/``mcp``); ``decided_by`` is the domain
column (``user``/``auto``). The caller supplies both.
"""

from __future__ import annotations

import logging
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

    def history(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """All decisions (any status) newest-first by ``decided_at``. Read-only."""
        return self._decisions.history(limit=limit)

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

    def reject_merge(self, decision_id: str, *, decided_by: str = "user") -> None:
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
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        SECURITY_LINK_DECISION_OUTCOMES_TOTAL.labels(outcome="rejected").inc()
        logger.info(f"security merge rejected: decision={decision_id}")

        # Rejecting changed the pending count — refresh the gauge.
        from moneybin.services.security_resolver import (  # noqa: PLC0415
            refresh_security_link_pending_gauge,
        )

        refresh_security_link_pending_gauge(self._db)

    def accept_merge(self, decision_id: str, *, decided_by: str = "user") -> None:
        """Merge the provisional security into the decision's candidate, atomically.

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
        6. Auto-reject the ref's sibling pending candidates — accepting one answers
           them all, so a tie resolves in a single review action.
        7. Delete the provisional ``created_by='plaid'`` catalog row.

        Steps 4-7 must succeed or fail together: a merge that re-points the link
        but strands a lot selection silently corrupts cost basis.

        Raises ``UserError`` when:
        - ``decision_id`` is unknown (MUTATION_NOT_FOUND) or not ``pending``
          (MUTATION_CONSTRAINT_VIOLATION) — a decision never decides twice.
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
        self._db.begin()
        try:
            decision = self._require_pending(decision_id)
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
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise

        SECURITY_LINK_DECISION_OUTCOMES_TOTAL.labels(outcome="accepted").inc()
        logger.info(
            f"security merge accepted: decision={decision_id} "
            f"provisional={provisional} survivor={survivor} "
            f"disposals_remapped={len(plan)}"
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
