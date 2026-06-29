"""MerchantLinksService — review-queue facade over ``app.merchant_link_decisions`` (M1T).

Mirrors :mod:`moneybin.services.account_links_service`: a thin service that composes
two Invariant-10 repos (``MerchantLinksRepo`` and ``MerchantLinkDecisionsRepo``)
and coordinates multi-write atomic operations using the same
``db.begin() / db.commit() / db.rollback()`` pattern.

``actor`` is the audit surface (``cli``/``mcp``); ``decided_by`` is the domain
column (``user``/``system``/``auto``). The caller supplies both.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

import duckdb

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.repositories.merchant_link_decisions_repo import MerchantLinkDecisionsRepo
from moneybin.repositories.merchant_links_repo import MerchantLinksRepo
from moneybin.tables import MERCHANT_LINK_DECISIONS, MERCHANTS

logger = logging.getLogger(__name__)


def _resolve_canonical_name(db: Database, merchant_id: str) -> str:
    """Return ``canonical_name`` from ``core.dim_merchants``; empty string when absent.

    Guards ``CatalogException`` so the service degrades gracefully on a fresh
    database where dim_merchants has not yet been materialized.
    """
    try:
        row = db.execute(
            f"SELECT canonical_name FROM {MERCHANTS.full_name} "  # noqa: S608  # TableRef constant + parameterized value
            "WHERE merchant_id = ? LIMIT 1",
            [merchant_id],
        ).fetchone()
        return str(row[0]) if row else ""
    except duckdb.CatalogException:
        return ""


@dataclass(frozen=True)
class PendingMerchantLinkCandidate:
    """One candidate merchant proposal within a pending-review group."""

    decision_id: str
    candidate_merchant_id: str
    candidate_canonical_name: str
    confidence: float | None


@dataclass(frozen=True)
class PendingMerchantLinkGroup:
    """One provider entity id awaiting review + its candidate merchant proposals."""

    ref_value: str
    source_type: str
    provider_merchant_name: str | None
    candidates: tuple[PendingMerchantLinkCandidate, ...]


class MerchantLinksService:
    """Review-queue facade over ``app.merchant_link_decisions`` + ``app.merchant_links``.

    Composes ``MerchantLinkDecisionsRepo`` and ``MerchantLinksRepo`` for all
    mutations (Invariant 10). Multi-step atomic operations use
    ``db.begin()`` / ``db.commit()`` / ``db.rollback()`` with each repo method
    called via ``in_outer_txn=True`` — the same pattern as
    :class:`~moneybin.services.account_links_service.AccountLinksService.set`.
    """

    def __init__(self, db: Database, *, actor: str = "cli") -> None:
        """Initialize with a Database and the audit surface actor."""
        self._db = db
        self._actor = actor
        self._links = MerchantLinksRepo(db)
        self._decisions = MerchantLinkDecisionsRepo(db)

    # ------------------------------------------------------------------
    # Read-only methods
    # ------------------------------------------------------------------

    def count_pending(self) -> int:
        """Number of DISTINCT provider entity ids with pending, non-reversed decisions.

        The review unit is the provider entity id, not the raw decision row —
        one entity id with two candidate proposals counts as one item, not two.
        Returns 0 when the table does not yet exist.
        """
        try:
            row = self._db.execute(
                f"""
                SELECT COUNT(DISTINCT ref_value)
                FROM {MERCHANT_LINK_DECISIONS.full_name}
                WHERE status = 'pending' AND reversed_at IS NULL
                """,  # noqa: S608  # TableRef constant, no user values
            ).fetchone()
            return int(row[0]) if row else 0
        except duckdb.CatalogException:
            return 0

    def pending(self) -> list[PendingMerchantLinkGroup]:
        """Return pending decisions grouped by provider entity id (ref_value).

        Reads ``MerchantLinkDecisionsRepo.list_pending()`` (already ordered by
        ``decided_at, decision_id``) and groups into
        ``PendingMerchantLinkGroup`` structs. Candidate display names are
        resolved from ``core.dim_merchants``; empty string when the row is
        absent or the table is not yet materialized (``CatalogException`` guard).
        Read-only — no audit emitted.
        """
        rows = self._decisions.list_pending()
        if not rows:
            return []

        # Group by ref_value preserving insertion order.
        groups: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            rv = row["ref_value"]
            groups.setdefault(rv, []).append(row)

        result: list[PendingMerchantLinkGroup] = []
        for ref_value, decisions in groups.items():
            first = decisions[0]
            candidates = tuple(
                PendingMerchantLinkCandidate(
                    decision_id=d["decision_id"],
                    candidate_merchant_id=d["candidate_merchant_id"],
                    candidate_canonical_name=_resolve_canonical_name(
                        self._db, d["candidate_merchant_id"]
                    ),
                    confidence=d["confidence_score"],
                )
                for d in decisions
            )
            result.append(
                PendingMerchantLinkGroup(
                    ref_value=ref_value,
                    source_type=first["source_type"],
                    provider_merchant_name=first.get("provider_merchant_name"),
                    candidates=candidates,
                )
            )
        return result

    def history(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """All decisions (any status) newest-first by ``decided_at``. Read-only.

        Delegates to the repo; empty list when the table is absent.
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
        """Harvest existing categorization facts into accepted merchant bindings.

        Delegates to ``MerchantResolver.harvest()``: binds provider entity ids
        that point unambiguously to a single canonical merchant, and routes
        conflicts to the review queue. Returns ``bound + conflicts``.
        """
        # Import here to avoid a circular-import at module level
        # (MerchantResolver <- MerchantLinkDecisionsRepo <- MerchantLinksService would cycle).
        from moneybin.services.merchant_resolver import (  # noqa: PLC0415
            MerchantResolver,
            refresh_merchant_link_pending_gauge,
        )

        result = MerchantResolver(self._db, actor=self._actor).harvest()
        refresh_merchant_link_pending_gauge(self._db)
        total = result.bound + result.conflicts
        logger.info(
            f"merchant_links_run: bound={result.bound} conflicts={result.conflicts}"
        )
        return total

    def set(  # noqa: A003  # mirrors the existing set_status verb shape; "set" is the surface verb
        self,
        decision_id: str,
        *,
        target_merchant_id: str | None,
        decided_by: str = "user",
    ) -> None:
        """Accept or reject a pending merchant-link decision atomically.

        ``target_merchant_id`` is not None (accept):
          1. Inserts an accepted binding for the decision's ``ref_value`` →
             ``target_merchant_id`` via ``MerchantLinksRepo.insert``.
          2. Marks the named decision ``accepted``.
          3. Auto-rejects every other pending, non-reversed decision for the
             same ``ref_value`` (sibling candidates).

        ``target_merchant_id`` is None (reject):
          Marks only the named decision ``rejected``. The resolver will mint a
          new proposal on the next ``run()``.

        All writes run inside one ``db.begin()`` / ``db.commit()`` transaction.
        Each repo method is called with ``in_outer_txn=True`` so it joins the
        enclosing transaction instead of opening a nested one.

        Raises ``UserError`` when:
        - ``decision_id`` is not found (MUTATION_NOT_FOUND).
        - The decision is not in ``pending`` status (MUTATION_CONSTRAINT_VIOLATION).
        """
        self._db.begin()
        try:
            decision = self._fetch_decision(decision_id)
            if decision is None:
                raise UserError(
                    f"No merchant-link decision found for id {decision_id!r}.",
                    code=error_codes.MUTATION_NOT_FOUND,
                )
            if decision["status"] != "pending":
                raise UserError(
                    f"Decision {decision_id!r} is {decision['status']!r}, not pending.",
                    code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                )

            ref_value = decision["ref_value"]

            if target_merchant_id is not None:
                # Accept path: bind ref_value → target_merchant_id, accept the
                # named decision, auto-reject sibling pending decisions.
                self._links.insert(
                    link_id=uuid.uuid4().hex[:12],
                    merchant_id=target_merchant_id,
                    ref_kind="merchant_entity_id",
                    ref_value=ref_value,
                    source_type=decision["source_type"],
                    decided_by=decided_by,
                    actor=self._actor,
                    status="accepted",
                    in_outer_txn=True,
                )
                self._decisions.update_status(
                    decision_id,
                    status="accepted",
                    decided_by=decided_by,
                    actor=self._actor,
                    in_outer_txn=True,
                )
                # Auto-reject sibling pending decisions for the same ref_value.
                sibling_rows = self._db.execute(
                    f"""
                    SELECT decision_id FROM {MERCHANT_LINK_DECISIONS.full_name}
                    WHERE ref_value = ?
                      AND decision_id != ?
                      AND status = 'pending'
                      AND reversed_at IS NULL
                    """,  # noqa: S608  # TableRef constant + parameterized values
                    [ref_value, decision_id],
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
                # Reject path: mark only the named decision rejected.
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

        # Accept/reject changed the pending count — refresh the gauge.
        from moneybin.services.merchant_resolver import (  # noqa: PLC0415
            refresh_merchant_link_pending_gauge,
        )

        refresh_merchant_link_pending_gauge(self._db)
