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

    def _propose(
        self,
        ref_value: str,
        source_type: str,
        provider_name: str | None,
        candidate_merchant_id: str,
    ) -> None:
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
