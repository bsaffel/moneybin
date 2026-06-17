"""AccountLinksService — review-queue facade for ``app.account_link_decisions``.

Mirrors :mod:`moneybin.services.matching_service`: a thin service that composes
two Invariant-10 repos (``AccountLinksRepo`` and ``AccountLinkDecisionsRepo``)
and coordinates multi-write atomic operations using the same
``db.begin() / db.commit() / db.rollback()`` pattern.

``actor`` is the audit surface (``cli``/``mcp``); ``decided_by`` is the domain
column (``user``/``system``/``auto``). The caller supplies both.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

import duckdb

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.account_link_decisions_repo import AccountLinkDecisionsRepo
from moneybin.repositories.account_links_repo import AccountLinksRepo
from moneybin.services.account_resolution_types import (
    PendingLinkCandidate,
    PendingLinkGroup,
)
from moneybin.tables import ACCOUNT_LINK_DECISIONS, ACCOUNT_LINKS, DIM_ACCOUNTS
from moneybin.utils.parsing import signal_from_match_signals

logger = logging.getLogger(__name__)


def _resolve_display_name(db: Database, account_id: str) -> str:
    """Return ``display_name`` from ``core.dim_accounts``; empty string when absent.

    Thin alias that localizes the sole ``account_resolver`` seam to one place
    rather than scattering the import across both call sites.
    """
    # Function-local for the same reason as run() below: a module-level import
    # of account_resolver cycles (AccountResolver <- AccountLinkDecisionsRepo
    # <- AccountLinksService).
    from moneybin.services.account_resolver import (  # noqa: PLC0415 — circular-import avoidance
        fetch_display_name,
    )

    return fetch_display_name(db, account_id)


class AccountLinksService:
    """Review-queue facade over ``app.account_link_decisions`` + ``app.account_links``.

    Composes ``AccountLinkDecisionsRepo`` and ``AccountLinksRepo`` for all
    mutations (Invariant 10). Multi-step atomic operations use
    ``db.begin()`` / ``db.commit()`` / ``db.rollback()`` with each repo method
    called via ``in_outer_txn=True`` — the same pattern as
    :class:`~moneybin.services.matching_service.MatchingService.set_status`.
    """

    def __init__(self, db: Database, *, actor: str = "cli") -> None:
        """Initialize with a Database and the audit surface actor."""
        self._db = db
        self._actor = actor
        self._links = AccountLinksRepo(db)
        self._decisions = AccountLinkDecisionsRepo(db)

    # ------------------------------------------------------------------
    # Read-only methods
    # ------------------------------------------------------------------

    def count_pending(self) -> int:
        """Number of DISTINCT provisional accounts with pending, non-reversed decisions.

        The review *unit* is the provisional account, not the raw decision row —
        one provisional with two candidate proposals counts as one item, not two.
        Returns 0 when the table does not yet exist.
        """
        try:
            row = self._db.execute(
                f"""
                SELECT COUNT(DISTINCT provisional_account_id)
                FROM {ACCOUNT_LINK_DECISIONS.full_name}
                WHERE status = 'pending' AND reversed_at IS NULL
                """,  # noqa: S608  # TableRef constant, no user values
            ).fetchone()
            return int(row[0]) if row else 0
        except duckdb.CatalogException:
            return 0

    def pending(self) -> list[PendingLinkGroup]:
        """Return pending decisions grouped by provisional account.

        Reads ``AccountLinkDecisionsRepo.list_pending()`` (already ordered
        ``provisional_account_id, decision_id``) and groups into
        ``PendingLinkGroup`` structs. Display names are resolved from
        ``core.dim_accounts``; empty string when the row is absent or the
        table is not yet materialized (``CatalogException`` guard).
        Read-only — no audit emitted.
        """
        rows = self._decisions.list_pending()
        if not rows:
            return []

        # list_pending() orders by provisional_account_id, decision_id — group in order.
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            pid = row["provisional_account_id"]
            groups.setdefault(pid, []).append(row)

        result: list[PendingLinkGroup] = []
        for provisional_id, decisions in groups.items():
            prov_display = _resolve_display_name(self._db, provisional_id)
            candidates = tuple(
                PendingLinkCandidate(
                    decision_id=d["decision_id"],
                    candidate_account_id=d["candidate_account_id"],
                    candidate_display_name=_resolve_display_name(
                        self._db, d["candidate_account_id"]
                    ),
                    confidence=d["confidence_score"],
                    # match_signals is already decoded by list_pending(); cast for pyright.
                    signal=signal_from_match_signals(d["match_signals"]),
                )
                for d in decisions
            )
            result.append(
                PendingLinkGroup(
                    provisional_account_id=provisional_id,
                    provisional_display_name=prov_display,
                    candidates=candidates,
                )
            )
        return result

    def history(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """All decisions (any status) newest-first by ``decided_at``. Read-only.

        Mirrors ``MatchingService.get_log``. Delegates to the repo (raw SQL +
        decode live in the repo layer); empty list when the table is absent.
        """
        return self._decisions.history(limit=limit)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fetch_decision(self, decision_id: str) -> dict[str, Any] | None:
        """Read one decision row by id. Returns None when not found."""
        return self._decisions.fetch_by_id(decision_id)

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def run(self, *, decided_by: str = "auto") -> int:
        """Backfill pending link proposals for all accounts in core.dim_accounts.

        For each account in core.dim_accounts, calls
        ``AccountResolver.propose_existing`` to find weak-signal candidates
        (institution+last4 or name fuzzy-match), then writes a ``pending``
        ``app.account_link_decisions`` row for each new unordered pair.

        Dedup rules (skips a candidate if either holds):
        - A decision already exists for that pair in either direction (any status).
        - The same unordered pair was already written in this run.

        Returns the count of new ``pending`` decisions written. Returns 0 when
        ``core.dim_accounts`` is not yet materialized.
        """
        # Import here to avoid a circular-import at module level
        # (AccountResolver ← AccountLinkDecisionsRepo ← AccountLinksService would cycle).
        from moneybin.services.account_resolver import (  # noqa: PLC0415
            AccountResolver,
            refresh_account_link_pending_gauge,
        )

        resolver = AccountResolver(self._db, actor=self._actor)
        try:
            account_ids = [
                str(r[0])
                for r in self._db.execute(
                    f"SELECT account_id FROM {DIM_ACCOUNTS.full_name}",  # noqa: S608  # TableRef constant
                ).fetchall()
            ]
        except duckdb.CatalogException:
            logger.debug("core.dim_accounts unavailable in run(); returning 0")
            return 0

        # Only a provisional with an accepted source_native link can be merged
        # (set() re-points that link; without it the merge can't collapse data
        # and set() refuses it). Don't write backfill proposals that would
        # dead-end at the merge step — they'd only be resolvable as standalone.
        mergeable = {
            str(r[0])
            for r in self._db.execute(
                f"SELECT DISTINCT account_id FROM {ACCOUNT_LINKS.full_name} "  # noqa: S608  # TableRef constant
                "WHERE ref_kind = 'source_native' AND status = 'accepted'",
            ).fetchall()
        }

        new_count = 0
        # Track unordered pairs written this run to avoid A→B + B→A double-writes.
        seen_pairs: set[frozenset[str]] = set()

        self._db.begin()
        try:
            for account_id in account_ids:
                if account_id not in mergeable:
                    continue
                proposal = resolver.propose_existing(account_id)
                if proposal is None:
                    continue
                for candidate in proposal.candidates:
                    pair: frozenset[str] = frozenset({account_id, candidate.account_id})
                    if pair in seen_pairs:
                        continue
                    # Skip if any decision (any status, either direction) already covers this pair.
                    existing = self._db.execute(
                        f"""
                        SELECT 1 FROM {ACCOUNT_LINK_DECISIONS.full_name}
                        WHERE (provisional_account_id = ? AND candidate_account_id = ?)
                           OR (provisional_account_id = ? AND candidate_account_id = ?)
                        LIMIT 1
                        """,  # noqa: S608  # TableRef constant + parameterized values
                        [
                            account_id,
                            candidate.account_id,
                            candidate.account_id,
                            account_id,
                        ],
                    ).fetchone()
                    seen_pairs.add(pair)
                    if existing is not None:
                        continue
                    decision_id = uuid.uuid4().hex[:12]
                    self._decisions.insert(
                        decision_id=decision_id,
                        provisional_account_id=account_id,
                        candidate_account_id=candidate.account_id,
                        confidence_score=candidate.confidence,
                        # value = matched candidate's display_name (schema intent:
                        # "which weak signal fired + its value"), mirroring import-time.
                        # Internal field; the queue surfaces signal/display_name, not this.
                        match_signals={
                            "signal": candidate.signal,
                            "value": candidate.display_name,
                        },
                        decided_by=decided_by,
                        actor=self._actor,
                        status="pending",
                        in_outer_txn=True,
                    )
                    new_count += 1
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise

        refresh_account_link_pending_gauge(self._db)
        logger.info(f"accounts_links_run: wrote {new_count} new pending decisions")
        return new_count

    def set(  # noqa: A003  # mirrors the existing set_status verb shape; "set" is the surface verb
        self,
        decision_id: str,
        *,
        target_account_id: str | None,
        decided_by: str = "user",
    ) -> None:
        """Accept (merge) or standalone-reject a pending link decision atomically.

        ``target_account_id`` is not None (accept → merge):
          1. Validates the arg matches the decision's own ``candidate_account_id``.
          2. Re-points every accepted ``source_native`` link for the provisional
             account onto ``target_account_id`` via ``AccountLinksRepo.repoint``.
          3. Marks the named decision ``accepted``.
          4. Auto-rejects every other pending, non-reversed decision for the
             same provisional account.

        ``target_account_id`` is None (standalone):
          Rejects every pending, non-reversed decision for the provisional
          account. No repoint — the provisional stays its own canonical account.

        All writes run inside one ``db.begin()`` / ``db.commit()`` transaction
        (matching :meth:`MatchingService.set_status`). Each repo method is
        called with ``in_outer_txn=True`` so it joins the enclosing transaction
        instead of opening a nested one.

        Raises ``UserError`` when:
        - ``decision_id`` is not found (MUTATION_NOT_FOUND).
        - The decision is not in ``pending`` status (MUTATION_CONSTRAINT_VIOLATION).
        - ``target_account_id`` does not match the decision's ``candidate_account_id``
          (MUTATION_INVALID_INPUT).
        """
        self._db.begin()
        try:
            decision = self._fetch_decision(decision_id)
            if decision is None:
                raise UserError(
                    f"No account-link decision found for id {decision_id!r}.",
                    code=error_codes.MUTATION_NOT_FOUND,
                )
            if decision["status"] != "pending":
                raise UserError(
                    f"Decision {decision_id!r} is {decision['status']!r}, not pending.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )

            provisional_id = decision["provisional_account_id"]

            if target_account_id is not None:
                # Accept path: validate confirming arg then repoint + accept + auto-reject.
                if target_account_id != decision["candidate_account_id"]:
                    raise UserError(
                        "target_account_id does not match the candidate named in "
                        f"decision {decision_id!r}; pass the decision's own "
                        "candidate_account_id as a confirming safety check.",
                        code=error_codes.MUTATION_INVALID_INPUT,
                    )
                # Re-point ALL accepted links for the provisional onto the
                # candidate — not only source_native (the staging-JOIN key) but
                # also strong refs (persistent_token / full_number, used for
                # adoption lookups). Leaving a strong ref on the merged-away
                # provisional would later mis-adopt a source carrying the same
                # token/number onto the dead id instead of the candidate.
                links = self._db.execute(
                    f"""
                    SELECT link_id, ref_kind FROM {ACCOUNT_LINKS.full_name}
                    WHERE account_id = ? AND status = 'accepted'
                    """,  # noqa: S608  # TableRef constant + parameterized values
                    [provisional_id],
                ).fetchall()
                if not any(ref_kind == "source_native" for _, ref_kind in links):
                    # No source_native mapping to re-point means the merge can't
                    # take effect: the staging JOIN translates raw rows via
                    # source_native links, so accepting here would record a
                    # "paper merge" that never collapses the data. Refuse (rolls
                    # back) rather than silently mark the decision accepted.
                    raise UserError(
                        f"Cannot apply merge for decision {decision_id!r}: the "
                        "provisional account has no source_native mapping to "
                        "re-point onto the candidate.",
                        code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                    )
                for link_id, _ref_kind in links:
                    self._links.repoint(
                        link_id=link_id,
                        new_account_id=target_account_id,
                        decided_by=decided_by,
                        actor=self._actor,
                        in_outer_txn=True,
                    )
                # Accept the named decision.
                self._decisions.update_status(
                    decision_id,
                    status="accepted",
                    decided_by=decided_by,
                    actor=self._actor,
                    in_outer_txn=True,
                )
                # Auto-reject every other pending decision that touches the
                # provisional — both where it is the provisional (other
                # candidates for it) AND where it is the *candidate* (some other
                # Q→P proposal). The provisional is merged away, so a later
                # accept of Q→P would re-point Q onto a dead account; a fresh
                # run() re-proposes Q→C if Q really is the same account.
                sibling_rows = self._db.execute(
                    f"""
                    SELECT decision_id FROM {ACCOUNT_LINK_DECISIONS.full_name}
                    WHERE (provisional_account_id = ? OR candidate_account_id = ?)
                      AND decision_id != ?
                      AND status = 'pending'
                      AND reversed_at IS NULL
                    """,  # noqa: S608  # TableRef constant + parameterized values
                    [provisional_id, provisional_id, decision_id],
                ).fetchall()
                for (sid,) in sibling_rows:
                    self._decisions.update_status(
                        sid,
                        status="rejected",
                        decided_by=decided_by,
                        actor=self._actor,
                        in_outer_txn=True,
                    )
            else:
                # Standalone path: reject every pending decision for this provisional.
                pending_rows = self._db.execute(
                    f"""
                    SELECT decision_id FROM {ACCOUNT_LINK_DECISIONS.full_name}
                    WHERE provisional_account_id = ?
                      AND status = 'pending'
                      AND reversed_at IS NULL
                    """,  # noqa: S608  # TableRef constant + parameterized values
                    [provisional_id],
                ).fetchall()
                for (did,) in pending_rows:
                    self._decisions.update_status(
                        did,
                        status="rejected",
                        decided_by=decided_by,
                        actor=self._actor,
                        in_outer_txn=True,
                    )

            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise
        # Accept/reject changed the pending count — refresh the gauge (only
        # reached on a successful commit; the except above re-raises).
        from moneybin.services.account_resolver import (  # noqa: PLC0415
            refresh_account_link_pending_gauge,
        )

        refresh_account_link_pending_gauge(self._db)
