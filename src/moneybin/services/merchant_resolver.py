"""MerchantResolver — provider merchant id -> canonical merchant_id (M1T).

Adopt-or-mint ladder, mirroring AccountResolver:
  1 adopt bound id  2 auto-bind exact name  3 propose fuzzy (review)  4 mint plaid merchant.
Runs at categorization time; never writes app.transaction_categories (the orchestrator does).
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass

import duckdb

from moneybin.database import Database
from moneybin.metrics.registry import (
    MERCHANT_LINK_CONFIDENCE,
    MERCHANT_LINK_REVIEW_PENDING,
)
from moneybin.repositories.merchant_link_decisions_repo import MerchantLinkDecisionsRepo
from moneybin.repositories.merchant_links_repo import MerchantLinksRepo
from moneybin.services.categorization.applier import MatchApplier
from moneybin.tables import (
    INT_TRANSACTIONS_MERGED,
    MERCHANT_LINK_DECISIONS,
    TRANSACTION_CATEGORIES,
)

logger = logging.getLogger(__name__)
_FUZZY_CONFIDENCE = 0.5


def count_pending_merchant_link_decisions(db: Database) -> int:
    """Distinct (source_type, provider entity id) pairs with pending, non-reversed decisions.

    Single source of truth for the review-queue count, shared by
    ``refresh_merchant_link_pending_gauge`` (observability) and
    ``MerchantLinksService.count_pending`` (surface). Counts the composite
    ``(source_type, ref_value)`` pair — the same review unit as ``pending()``
    — so two providers sharing one opaque ``ref_value`` count as two. Returns
    0 when the table does not yet exist (``CatalogException`` guard) so a fresh
    DB reports an empty queue rather than raising.
    """
    try:
        row = db.execute(
            f"SELECT COUNT(*) FROM (SELECT DISTINCT source_type, ref_value "  # noqa: S608  # TableRef constant
            f"FROM {MERCHANT_LINK_DECISIONS.full_name} "
            "WHERE status = 'pending' AND reversed_at IS NULL)"
        ).fetchone()
    except duckdb.CatalogException:
        return 0
    return int(row[0]) if row else 0


def refresh_merchant_link_pending_gauge(db: Database) -> None:
    """Set MERCHANT_LINK_REVIEW_PENDING from the live queue depth (distinct provider ids)."""
    MERCHANT_LINK_REVIEW_PENDING.set(count_pending_merchant_link_decisions(db))


@dataclass(frozen=True)
class HarvestResult:
    """Result from MerchantResolver.harvest(): counts of bindings written and conflicts routed."""

    bound: int
    conflicts: int


@dataclass(frozen=True)
class MerchantResolution:
    """Resolution outcome from MerchantResolver.resolve()."""

    merchant_id: str | None
    outcome: str  # adopted | auto_bound | proposed | minted | none
    created: bool = False


class MerchantResolver:
    """Resolves a provider merchant entity id to a canonical merchant_id."""

    def __init__(self, db: Database, *, actor: str = "system") -> None:
        """Bind the resolver to a database and set the audit actor."""
        self._db = db
        self._actor = actor
        self._links = MerchantLinksRepo(db)
        self._decisions = MerchantLinkDecisionsRepo(db)

    def load_bindings(self) -> dict[tuple[str, str], str]:
        """(source_type, ref_value) -> merchant_id for all accepted bindings (batch cache)."""
        from moneybin.tables import MERCHANT_LINKS  # noqa: PLC0415

        rows = self._db.execute(
            f"SELECT source_type, ref_value, merchant_id FROM {MERCHANT_LINKS.full_name} "  # noqa: S608  # TableRef constant
            "WHERE status = 'accepted'"
        ).fetchall()
        return {(str(r[0]), str(r[1])): str(r[2]) for r in rows}

    def load_pending(self) -> set[tuple[str, str]]:
        """(source_type, ref_value) for all pending, non-reversed decisions.

        Used as a batch cache by ``harvest()``: entities with a pending decision
        are awaiting user review and must never be auto-bound — doing so would
        silently accept a match the user has been asked to confirm (a "magic
        stays visible" violation). Degrades to ``set()`` when the decisions table
        is absent (``CatalogException`` guard) so a fresh DB returns an empty set.
        """
        try:
            rows = self._db.execute(
                f"SELECT source_type, ref_value "  # noqa: S608  # TableRef constant
                f"FROM {MERCHANT_LINK_DECISIONS.full_name} "
                "WHERE status = 'pending' AND reversed_at IS NULL"
            ).fetchall()
        except duckdb.CatalogException:
            return set()
        return {(str(r[0]), str(r[1])) for r in rows}

    def load_rejected(self) -> set[tuple[str, str, str]]:
        """(source_type, ref_value, candidate_merchant_id) for all rejected, non-reversed decisions.

        Used as a batch cache by ``resolve()``: when a name-matched candidate appears
        in this set the resolver falls through to rung 4 (mint) instead of returning
        the rejected candidate, per spec Decision 6. Degrades to ``set()`` when the
        decisions table is absent (``CatalogException`` guard) so a fresh DB returns
        an empty rejected set rather than raising.
        """
        try:
            rows = self._db.execute(
                f"SELECT source_type, ref_value, candidate_merchant_id "  # noqa: S608  # TableRef constant
                f"FROM {MERCHANT_LINK_DECISIONS.full_name} "
                "WHERE status = 'rejected' AND reversed_at IS NULL"
            ).fetchall()
        except duckdb.CatalogException:
            return set()
        return {(str(r[0]), str(r[1]), str(r[2])) for r in rows}

    def resolve(
        self,
        *,
        merchant_entity_id: str | None,
        source_type: str,
        provider_merchant_name: str | None,
        name_match: Mapping[str, object] | None,
        bindings: dict[tuple[str, str], str],
        rejected: set[tuple[str, str, str]],
        pending: set[tuple[str, str]],
        applier: MatchApplier,
    ) -> MerchantResolution:
        """Run the adopt-or-mint ladder and return the resolution outcome."""
        if not merchant_entity_id:
            return MerchantResolution(merchant_id=None, outcome="none")

        # Rung 1 — adopt a bound id.
        bound = bindings.get((source_type, merchant_entity_id))
        if bound is not None:
            return MerchantResolution(merchant_id=bound, outcome="adopted")

        # Check whether this entity is awaiting user review. An entity under
        # review must never be silently auto-bound or minted — doing so would
        # bypass the user's pending decision ("magic stays visible" violation).
        # Rung-1 adopt is unaffected: a pending decision is not an accepted
        # binding, so bindings.get() already misses above.
        under_review = (source_type, merchant_entity_id) in pending

        # Rung 2/3 — there is a name match.
        if name_match is not None and name_match.get("merchant_id"):
            mid = str(name_match["merchant_id"])
            if (source_type, merchant_entity_id, mid) not in rejected:
                # Only auto-bind when the entity is NOT under review; if it is,
                # fall through to the _propose path (dedup-guarded) so the
                # existing pending decision retains control.
                if name_match.get("strength") == "exact" and not under_review:
                    self._bind(merchant_entity_id, source_type, mid, decided_by="auto")
                    bindings[(source_type, merchant_entity_id)] = mid
                    return MerchantResolution(merchant_id=mid, outcome="auto_bound")
                # Fuzzy / ambiguous, OR exact but under review → propose, do NOT bind.
                # Categorization still uses mid.
                self._propose(
                    merchant_entity_id, source_type, provider_merchant_name, mid
                )
                return MerchantResolution(merchant_id=mid, outcome="proposed")
            # else: the matched candidate was user-rejected for this entity id →
            # fall through to rung 4 (mint a new merchant), per spec Decision 6:
            # "reject → resolver mints a new merchant for the id on its next pass."

        # Rung 4 — no name match: mint a merchant from the provider's data, bind.
        # Guard: if the entity is under review, leave it unbound so the pending
        # decision retains control — minting here would silently create a new
        # accepted binding while the review is still open.
        if under_review:
            return MerchantResolution(merchant_id=None, outcome="none")
        canonical = (
            provider_merchant_name or merchant_entity_id
        ).strip() or merchant_entity_id
        new_id = applier.create_merchant_core(
            None,
            canonical,
            match_type="oneOf",
            created_by="plaid",
            exemplars=[],
            actor=self._actor,
        )
        self._bind(merchant_entity_id, source_type, new_id, decided_by="auto")
        bindings[(source_type, merchant_entity_id)] = new_id
        return MerchantResolution(merchant_id=new_id, outcome="minted", created=True)

    def _bind(
        self, ref_value: str, source_type: str, merchant_id: str, *, decided_by: str
    ) -> None:
        self._links.insert(
            link_id=uuid.uuid4().hex[:12],
            merchant_id=merchant_id,
            ref_kind="merchant_entity_id",
            ref_value=ref_value,
            source_type=source_type,
            decided_by=decided_by,
            actor=self._actor,
        )

    def _decision_blocks_propose(
        self, ref_value: str, source_type: str, candidate_merchant_id: str
    ) -> bool:
        """True when an existing decision should suppress re-proposing this binding.

        Dedups proposals: N uncategorized txns sharing one unbound fuzzy entity
        must not create N duplicate pending rows, and a re-run of ``run()`` must
        not re-propose an already-decided conflict. Blocks on a ``pending`` OR a
        ``rejected`` (non-reversed) decision for ``(source_type, ref_value,
        candidate_merchant_id)`` — re-proposing a user-rejected candidate every
        run would mean the queue never drains. A ``reversed`` decision is NOT a
        block (``reversed_at IS NULL``), so a reversal re-opens the proposal.
        Keyed on ``source_type`` so two providers sharing one opaque ``ref_value``
        do not cross-suppress each other.
        Degrades to ``False`` when the decisions table is absent
        (``CatalogException`` guard).
        """
        try:
            row = self._db.execute(
                f"SELECT 1 FROM {MERCHANT_LINK_DECISIONS.full_name} "  # noqa: S608  # TableRef constant + parameterized values
                "WHERE ref_value = ? AND source_type = ? AND candidate_merchant_id = ? "
                "AND status IN ('pending', 'rejected') AND reversed_at IS NULL LIMIT 1",
                [ref_value, source_type, candidate_merchant_id],
            ).fetchone()
        except duckdb.CatalogException:
            return False
        return row is not None

    def _propose(
        self,
        ref_value: str,
        source_type: str,
        provider_name: str | None,
        candidate_merchant_id: str,
    ) -> bool:
        """Insert a pending decision for this (source_type, ref_value, candidate).

        Returns True when the decision was newly inserted, False when an existing
        pending or rejected (non-reversed) decision already blocked re-proposal.
        Callers use the return value to distinguish "newly queued" from "already
        in queue" — harvest() counts only newly queued conflicts.
        """
        if self._decision_blocks_propose(ref_value, source_type, candidate_merchant_id):
            return False  # already proposed (pending) or user-rejected for this (source_type, ref_value, candidate)
        self._decisions.insert(
            decision_id=uuid.uuid4().hex[:12],
            ref_kind="merchant_entity_id",
            ref_value=ref_value,
            source_type=source_type,
            provider_merchant_name=provider_name,
            candidate_merchant_id=candidate_merchant_id,
            confidence_score=_FUZZY_CONFIDENCE,
            match_signals={"signal": "fuzzy_name", "value": provider_name},
            decided_by="auto",
            actor=self._actor,
            match_reason="fuzzy_name",
        )
        MERCHANT_LINK_CONFIDENCE.observe(_FUZZY_CONFIDENCE)
        refresh_merchant_link_pending_gauge(self._db)
        return True

    def harvest(self) -> HarvestResult:
        """Bind established (provider id -> assigned merchant) facts from existing categorizations.

        Routes one-id-many-merchants conflicts to the review queue without binding.
        Idempotent: the insert guard in _bind skips already-bound entity ids.
        Skips merchant_ids the user already rejected for an entity id — re-binding
        one would silently re-adopt a rejected merchant; leaving it unbound lets the
        next categorization pass mint a new merchant for the id (spec Decision 6).
        Also skips entities with a pending (under-review) decision — auto-binding
        one would silently accept a match the user has been asked to confirm, a
        "magic stays visible" violation.

        Keys on ``merchant_entity_source_type`` — the source_type of the merge
        member that issued the entity id — NOT the merge-winner
        ``canonical_source_type``, so a Plaid entity id riding an OFX+Plaid
        dedup binds under ``('plaid', E)`` like its Plaid-only siblings.

        Degrades to ``HarvestResult(0, 0)`` when ``prep.int_transactions__merged``
        is absent (never-transformed DB — CatalogException) or exists but predates
        the entity columns (stale-view upgrade — BinderException) so the MCP path
        doesn't raise raw.
        """
        try:
            rows = self._db.execute(
                f"""
                SELECT mt.merchant_entity_source_type AS source_type,
                       mt.merchant_entity_id, c.merchant_id, COUNT(*) AS n,
                       MAX(mt.merchant_name) AS provider_name
                FROM {INT_TRANSACTIONS_MERGED.full_name} AS mt
                JOIN {TRANSACTION_CATEGORIES.full_name} AS c
                    ON c.transaction_id = mt.transaction_id
                WHERE mt.merchant_entity_id IS NOT NULL AND c.merchant_id IS NOT NULL
                GROUP BY mt.merchant_entity_source_type, mt.merchant_entity_id,
                         c.merchant_id
                """  # noqa: S608  # TableRef constants, no user values
            ).fetchall()
        except (duckdb.CatalogException, duckdb.BinderException):
            return HarvestResult(bound=0, conflicts=0)
        by_id: dict[tuple[str, str], list[tuple[str, int]]] = {}
        names: dict[tuple[str, str], str | None] = {}
        for source_type, ent, mid, n, provider_name in rows:
            key = (str(source_type), str(ent))
            by_id.setdefault(key, []).append((str(mid), int(n)))
            names.setdefault(key, provider_name)  # consistent per entity id; first seen
        bound = conflicts = 0
        existing = self.load_bindings()
        rejected = self.load_rejected()
        pending = self.load_pending()
        for (source_type, ent), pairs in by_id.items():
            if (source_type, ent) in existing:
                continue  # already bound (idempotent)
            if (source_type, ent) in pending:
                continue  # under review — never auto-bind/propose an entity awaiting a user decision
            # Drop merchants the user already rejected for this entity id — re-binding
            # one would silently re-adopt a rejected merchant. harvest() must respect
            # rejections like resolve() does (spec Decision 6).
            live_pairs = [
                (mid, n) for mid, n in pairs if (source_type, ent, mid) not in rejected
            ]
            if not live_pairs:
                continue  # every observed merchant was rejected → leave unbound; the
                # next categorization pass mints a new merchant for the id
            merchants = {mid for mid, _ in live_pairs}
            if len(merchants) == 1:
                self._bind(ent, source_type, next(iter(merchants)), decided_by="system")
                bound += 1
            else:
                # Highest count wins; tie-break on merchant_id for stable choice
                # across runs (GROUP BY has no inherent order), or the _propose dedup
                # could pick a different candidate each run.
                dominant = max(live_pairs, key=lambda p: (p[1], p[0]))[0]
                # Count only conflicts actually queued — _propose returns False when an
                # existing pending/rejected decision already blocks re-proposal.
                if self._propose(
                    ent, source_type, names.get((source_type, ent)), dominant
                ):
                    conflicts += 1
        if conflicts:
            refresh_merchant_link_pending_gauge(self._db)
        return HarvestResult(bound=bound, conflicts=conflicts)
