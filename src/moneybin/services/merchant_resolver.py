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
from moneybin.tables import MERCHANT_LINK_DECISIONS

logger = logging.getLogger(__name__)
_FUZZY_CONFIDENCE = 0.5


def refresh_merchant_link_pending_gauge(db: Database) -> None:
    """Set MERCHANT_LINK_REVIEW_PENDING from the live queue depth (distinct provider ids)."""
    row = db.execute(
        f"SELECT COUNT(DISTINCT ref_value) FROM {MERCHANT_LINK_DECISIONS.full_name} "  # noqa: S608  # TableRef constant
        "WHERE status = 'pending' AND reversed_at IS NULL"
    ).fetchone()
    MERCHANT_LINK_REVIEW_PENDING.set(int(row[0]) if row else 0)


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

    def resolve(
        self,
        *,
        merchant_entity_id: str | None,
        source_type: str,
        provider_merchant_name: str | None,
        name_match: Mapping[str, object] | None,
        bindings: dict[tuple[str, str], str],
        applier: MatchApplier,
    ) -> MerchantResolution:
        """Run the adopt-or-mint ladder and return the resolution outcome."""
        if not merchant_entity_id:
            return MerchantResolution(merchant_id=None, outcome="none")

        # Rung 1 — adopt a bound id.
        bound = bindings.get((source_type, merchant_entity_id))
        if bound is not None:
            return MerchantResolution(merchant_id=bound, outcome="adopted")

        # Rung 2/3 — there is a name match.
        if name_match is not None and name_match.get("merchant_id"):
            mid = str(name_match["merchant_id"])
            if name_match.get("strength") == "exact":
                self._bind(merchant_entity_id, source_type, mid, decided_by="auto")
                bindings[(source_type, merchant_entity_id)] = mid
                return MerchantResolution(merchant_id=mid, outcome="auto_bound")
            # Fuzzy / ambiguous → propose, do NOT bind. Categorization still uses mid.
            self._propose(merchant_entity_id, source_type, provider_merchant_name, mid)
            return MerchantResolution(merchant_id=mid, outcome="proposed")

        # Rung 4 — no name match: mint a merchant from the provider's data, bind.
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

    def _pending_decision_exists(
        self, ref_value: str, candidate_merchant_id: str
    ) -> bool:
        """True when a pending, non-reversed decision already proposes this binding.

        Dedups proposals: N uncategorized txns sharing one unbound fuzzy entity
        must not create N duplicate pending rows, and a re-run of ``run()`` must
        not re-propose an already-pending conflict. Degrades to ``False`` when
        the decisions table is absent (``CatalogException`` guard).
        """
        try:
            row = self._db.execute(
                f"SELECT 1 FROM {MERCHANT_LINK_DECISIONS.full_name} "  # noqa: S608  # TableRef constant + parameterized values
                "WHERE ref_value = ? AND candidate_merchant_id = ? "
                "AND status = 'pending' AND reversed_at IS NULL LIMIT 1",
                [ref_value, candidate_merchant_id],
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
    ) -> None:
        if self._pending_decision_exists(ref_value, candidate_merchant_id):
            return  # already a pending proposal for this (ref_value, candidate)
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

    def harvest(self) -> HarvestResult:
        """Bind established (provider id -> assigned merchant) facts from existing categorizations.

        Routes one-id-many-merchants conflicts to the review queue without binding.
        Idempotent: the insert guard in _bind skips already-bound entity ids.

        Keys on ``merchant_entity_source_type`` — the source_type of the merge
        member that issued the entity id — NOT the merge-winner
        ``canonical_source_type``, so a Plaid entity id riding an OFX+Plaid
        dedup binds under ``('plaid', E)`` like its Plaid-only siblings.

        Degrades to ``HarvestResult(0, 0)`` when ``prep.int_transactions__merged``
        is absent (never-transformed DB) — mirrors the ``CatalogException`` guard
        in ``list_pending`` / ``count_pending`` so the MCP path doesn't raise raw.
        """
        try:
            rows = self._db.execute(
                """
                SELECT mt.merchant_entity_source_type AS source_type,
                       mt.merchant_entity_id, c.merchant_id, COUNT(*) AS n
                FROM prep.int_transactions__merged AS mt
                JOIN app.transaction_categories AS c
                    ON c.transaction_id = mt.transaction_id
                WHERE mt.merchant_entity_id IS NOT NULL AND c.merchant_id IS NOT NULL
                GROUP BY mt.merchant_entity_source_type, mt.merchant_entity_id,
                         c.merchant_id
                """  # noqa: S608  # static identifiers
            ).fetchall()
        except duckdb.CatalogException:
            return HarvestResult(bound=0, conflicts=0)
        by_id: dict[tuple[str, str], list[tuple[str, int]]] = {}
        for source_type, ent, mid, n in rows:
            by_id.setdefault((str(source_type), str(ent)), []).append((
                str(mid),
                int(n),
            ))
        bound = conflicts = 0
        existing = self.load_bindings()
        for (source_type, ent), pairs in by_id.items():
            if (source_type, ent) in existing:
                continue  # already bound (idempotent)
            merchants = {mid for mid, _ in pairs}
            if len(merchants) == 1:
                self._bind(ent, source_type, next(iter(merchants)), decided_by="system")
                bound += 1
            else:
                # Highest count wins; tie-break on merchant_id so the chosen
                # dominant is stable across runs (the GROUP BY has no inherent
                # row order). Without this, a tied conflict could propose a
                # different candidate each run and defeat the _propose dedup.
                dominant = max(pairs, key=lambda p: (p[1], p[0]))[0]
                self._propose(ent, source_type, None, dominant)
                conflicts += 1
        if conflicts:
            refresh_merchant_link_pending_gauge(self._db)
        return HarvestResult(bound=bound, conflicts=conflicts)
