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
from typing import TYPE_CHECKING, Any

import duckdb

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.metrics.registry import MERCHANT_LINK_OUTCOMES_TOTAL
from moneybin.repositories.merchant_link_decisions_repo import MerchantLinkDecisionsRepo
from moneybin.repositories.merchant_links_repo import MerchantLinksRepo
from moneybin.tables import MERCHANT_LINK_DECISIONS, MERCHANTS

if TYPE_CHECKING:
    from moneybin.services.merchant_resolver import HarvestResult

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
        Returns 0 when the table does not yet exist. Single source of truth
        shared with the observability gauge via
        ``count_pending_merchant_link_decisions`` (imported lazily — the
        resolver must not import this service back).
        """
        from moneybin.services.merchant_resolver import (  # noqa: PLC0415
            count_pending_merchant_link_decisions,
        )

        return count_pending_merchant_link_decisions(self._db)

    def pending(self) -> list[PendingMerchantLinkGroup]:
        """Return pending decisions grouped by (source_type, ref_value).

        Reads ``MerchantLinkDecisionsRepo.list_pending()`` (already ordered by
        ``decided_at, decision_id``) and groups into
        ``PendingMerchantLinkGroup`` structs. Candidate display names are
        resolved from ``core.dim_merchants``; empty string when the row is
        absent or the table is not yet materialized (``CatalogException`` guard).
        Keyed on ``(source_type, ref_value)`` so two providers sharing one opaque
        ``ref_value`` form distinct review groups rather than folding into one.
        Read-only — no audit emitted.
        """
        rows = self._decisions.list_pending()
        if not rows:
            return []

        # Group by (source_type, ref_value) preserving insertion order.
        groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            key = (row["source_type"], row["ref_value"])
            groups.setdefault(key, []).append(row)

        result: list[PendingMerchantLinkGroup] = []
        for (source_type, ref_value), decisions in groups.items():
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
                    source_type=source_type,
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

    def _merchant_exists(self, merchant_id: str) -> bool:
        """True when ``merchant_id`` is present in ``core.dim_merchants``.

        Degrades to ``True`` (skip validation) when the dim is not yet
        materialized (``CatalogException`` guard) — a pre-transform DB has no
        merchant catalog to check against, so binding proceeds as before;
        once the view exists, an unknown id is a real dangling FK and rejects.
        """
        try:
            row = self._db.execute(
                f"SELECT 1 FROM {MERCHANTS.full_name} "  # noqa: S608  # TableRef constant + parameterized value
                "WHERE merchant_id = ? LIMIT 1",
                [merchant_id],
            ).fetchone()
        except duckdb.CatalogException:
            return True
        return row is not None

    def _reject_pending_siblings(
        self, ref_value: str, *, source_type: str, exclude: str, decided_by: str
    ) -> None:
        """Reject every pending, non-reversed decision for ``(source_type, ref_value)`` except ``exclude``.

        Shared by both the accept sweep (auto-reject losing candidates) and the
        reject sweep (``--new`` = reject ALL candidates for the entity). Scoped
        to ``source_type`` so two providers sharing one opaque ``ref_value`` do
        not cross-reject each other. Runs inside the caller's open transaction
        (``in_outer_txn=True``).
        """
        sibling_rows = self._db.execute(
            f"""
            SELECT decision_id FROM {MERCHANT_LINK_DECISIONS.full_name}
            WHERE ref_value = ?
              AND source_type = ?
              AND decision_id != ?
              AND status = 'pending'
              AND reversed_at IS NULL
            """,  # noqa: S608  # TableRef constant + parameterized values
            [ref_value, source_type, exclude],
        ).fetchall()
        for (sid,) in sibling_rows:
            self._decisions.update_status(
                sid,
                status="rejected",
                decided_by=decided_by,
                actor=self._actor,
                in_outer_txn=True,
            )

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def run(self) -> HarvestResult:
        """Harvest existing categorization facts into accepted merchant bindings.

        Delegates to ``MerchantResolver.harvest()``: binds provider entity ids
        that point unambiguously to a single canonical merchant, and routes
        conflicts to the review queue. Returns the ``HarvestResult`` so callers
        report silently-accepted bindings (``bound``) and queued-for-review
        conflicts (``conflicts``) distinctly — ``bound`` are NOT pending.
        """
        # Import here to avoid a circular-import at module level
        # (the resolver pulls in the categorization stack, which reaches back here).
        from moneybin.services.merchant_resolver import (  # noqa: PLC0415
            MerchantResolver,
            refresh_merchant_link_pending_gauge,
        )

        result = MerchantResolver(self._db, actor=self._actor).harvest()
        refresh_merchant_link_pending_gauge(self._db)
        logger.info(
            f"merchant_links_run: bound={result.bound} conflicts={result.conflicts}"
        )
        return result

    def set(  # noqa: A003  # mirrors the existing set_status verb shape; "set" is the surface verb
        self,
        decision_id: str,
        *,
        target_merchant_id: str | None,
        decided_by: str = "user",
    ) -> None:
        """Accept or reject a pending merchant-link decision atomically.

        ``target_merchant_id`` truthy (accept):
          1. Validates ``target_merchant_id`` equals the decision's
             ``candidate_merchant_id`` — a confirming safety check (mirrors
             :class:`~moneybin.services.account_links_service.AccountLinksService`);
             raises ``UserError`` when they diverge (MUTATION_INVALID_INPUT).
          2. Validates ``target_merchant_id`` exists in ``core.dim_merchants``;
             raises ``UserError`` when the catalog is present and the id is
             absent (no dangling FK).
          3. Inserts an accepted binding for the decision's ``ref_value`` →
             ``target_merchant_id`` via ``MerchantLinksRepo.insert``.
          4. Marks the named decision ``accepted``.
          5. Auto-rejects every other pending, non-reversed decision for the
             same ``(source_type, ref_value)`` (sibling candidates).

        ``target_merchant_id`` falsy — ``None`` or ``""`` (reject):
          Marks the named decision AND every sibling pending decision for the
          same ``(source_type, ref_value)`` ``rejected`` — ``--new`` means
          "reject ALL candidates". The declined pairing is not re-proposed; the
          resolver mints a new merchant for the id on its next categorization
          pass (spec Decision 6). An empty-string ``target_merchant_id`` rejects
          (it never binds).

        All writes run inside one ``db.begin()`` / ``db.commit()`` transaction.
        Each repo method is called with ``in_outer_txn=True`` so it joins the
        enclosing transaction instead of opening a nested one.

        Raises ``UserError`` when:
        - ``decision_id`` is not found (MUTATION_NOT_FOUND).
        - The decision is not in ``pending`` status (MUTATION_CONSTRAINT_VIOLATION).
        - ``target_merchant_id`` does not match the decision's
          ``candidate_merchant_id`` (MUTATION_INVALID_INPUT).
        - ``target_merchant_id`` is absent from ``core.dim_merchants``
          (MUTATION_NOT_FOUND).
        - The provider entity id is already bound to a different merchant
          (MUTATION_CONSTRAINT_VIOLATION).
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

            if target_merchant_id:
                # Accept path: confirming safety check, then validate the target,
                # bind ref_value → target_merchant_id, accept the named decision,
                # auto-reject sibling pending decisions.
                if target_merchant_id != decision["candidate_merchant_id"]:
                    raise UserError(
                        "target_merchant_id does not match the candidate named in "
                        f"decision {decision_id!r}; pass the decision's own "
                        "candidate_merchant_id as a confirming safety check.",
                        code=error_codes.MUTATION_INVALID_INPUT,
                    )
                if not self._merchant_exists(target_merchant_id):
                    raise UserError(
                        f"No merchant found for id {target_merchant_id!r}.",
                        code=error_codes.MUTATION_NOT_FOUND,
                    )
                try:
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
                except ValueError as exc:
                    # _guard_uniqueness: the entity id is already bound to a
                    # different merchant. Surface a clean UserError (no
                    # ref_value / PII in the message).
                    raise UserError(
                        "This provider entity id is already bound to a merchant.",
                        code=error_codes.MUTATION_CONSTRAINT_VIOLATION,
                    ) from exc
                self._decisions.update_status(
                    decision_id,
                    status="accepted",
                    decided_by=decided_by,
                    actor=self._actor,
                    in_outer_txn=True,
                )
                MERCHANT_LINK_OUTCOMES_TOTAL.labels(outcome="accepted").inc()
                self._reject_pending_siblings(
                    ref_value,
                    source_type=decision["source_type"],
                    exclude=decision_id,
                    decided_by=decided_by,
                )
            else:
                # Reject path (--new): reject the named decision AND all of its
                # pending siblings for the same (source_type, ref_value).
                # Per spec Decision 6: reject unbinds the durable id and routes
                # FUTURE transactions with that id to a fresh mint, but does NOT
                # rewrite categorizations already justified by an independent name
                # match. The historical-vs-future split is accepted in this
                # increment (Decision 7 — bindings do not retro-rewrite
                # categorizations).
                self._decisions.update_status(
                    decision_id,
                    status="rejected",
                    decided_by=decided_by,
                    actor=self._actor,
                    in_outer_txn=True,
                )
                MERCHANT_LINK_OUTCOMES_TOTAL.labels(outcome="rejected").inc()
                self._reject_pending_siblings(
                    ref_value,
                    source_type=decision["source_type"],
                    exclude=decision_id,
                    decided_by=decided_by,
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
