"""Typed payload dataclasses for the merchants-links surface (M1T).

Each field carries ``Annotated[T, DataClass.X]`` metadata so the Phase 6
middleware can derive sensitivity via ``derive_tier`` without inspecting
tool source code directly.

``merchant_id``, ``decision_id``, and ``ref_value`` (an opaque provider
entity id, not a financial account number) are ``RECORD_ID`` (Tier.LOW).
``provider_merchant_name`` and ``candidate_canonical_name`` are
``MERCHANT_NAME`` (Tier.MEDIUM) — these may embed identifying brand text.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any

from moneybin.privacy.taxonomy import DataClass
from moneybin.utils.parsing import signal_from_match_signals

if TYPE_CHECKING:
    from collections.abc import Iterable

    from moneybin.services.merchant_links_service import (
        PendingMerchantLinkCandidate,
        PendingMerchantLinkGroup,
    )


# ---------------------------------------------------------------------------
# Pending group helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MerchantLinkCandidateRow:
    """One candidate merchant proposal in a pending-review group."""

    decision_id: Annotated[str, DataClass.RECORD_ID]
    candidate_merchant_id: Annotated[str, DataClass.RECORD_ID]
    candidate_canonical_name: Annotated[str, DataClass.MERCHANT_NAME]
    confidence: Annotated[float | None, DataClass.AGGREGATE]

    @classmethod
    def from_candidate(
        cls, c: PendingMerchantLinkCandidate
    ) -> MerchantLinkCandidateRow:
        """Map a service ``PendingMerchantLinkCandidate`` into the payload row."""
        return cls(
            decision_id=c.decision_id,
            candidate_merchant_id=c.candidate_merchant_id,
            candidate_canonical_name=c.candidate_canonical_name,
            confidence=float(c.confidence) if c.confidence is not None else None,
        )


@dataclass(frozen=True, slots=True)
class MerchantLinkPendingGroup:
    """One provider entity id awaiting review + its candidate merchant proposals."""

    ref_value: Annotated[
        str, DataClass.RECORD_ID
    ]  # provider entity id (opaque, non-PII)
    source_type: Annotated[str, DataClass.TXN_TYPE]
    provider_merchant_name: Annotated[str | None, DataClass.MERCHANT_NAME]
    candidates: list[MerchantLinkCandidateRow]

    @classmethod
    def from_domain(cls, g: PendingMerchantLinkGroup) -> MerchantLinkPendingGroup:
        """Map a service ``PendingMerchantLinkGroup`` into the payload group."""
        return cls(
            ref_value=g.ref_value,
            source_type=g.source_type,
            provider_merchant_name=g.provider_merchant_name,
            candidates=[
                MerchantLinkCandidateRow.from_candidate(c) for c in g.candidates
            ],
        )


# ---------------------------------------------------------------------------
# merchants_links_pending
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MerchantLinksPendingPayload:
    """Payload for merchants_links_pending — pending review queue grouped by provider entity id."""

    groups: list[MerchantLinkPendingGroup]
    n_pending: Annotated[int, DataClass.AGGREGATE]

    @classmethod
    def from_service(
        cls,
        groups: Iterable[PendingMerchantLinkGroup],
        n_pending: int,
    ) -> MerchantLinksPendingPayload:
        """Build the pending payload from ``MerchantLinksService.pending()`` output.

        Single mapper shared by the MCP tool and CLI command so the two surfaces
        cannot drift in shape.
        """
        return cls(
            groups=[MerchantLinkPendingGroup.from_domain(g) for g in groups],
            n_pending=n_pending,
        )


# ---------------------------------------------------------------------------
# merchants_links_set
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MerchantLinksSetPayload:
    """Payload for merchants_links_set — confirmation of the decision applied."""

    decision_id: Annotated[str, DataClass.RECORD_ID]
    status: Annotated[str, DataClass.TXN_TYPE]  # "accepted" or "rejected"


# ---------------------------------------------------------------------------
# merchants_links_history
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MerchantLinkHistoryRow:
    """One past merchant-link decision (merchants_links_history result)."""

    decision_id: Annotated[str, DataClass.RECORD_ID]
    ref_value: Annotated[str, DataClass.RECORD_ID]  # provider entity id
    source_type: Annotated[str, DataClass.TXN_TYPE]
    candidate_merchant_id: Annotated[str, DataClass.RECORD_ID]
    provider_merchant_name: Annotated[str | None, DataClass.MERCHANT_NAME]
    status: Annotated[str, DataClass.TXN_TYPE]
    decided_by: Annotated[str, DataClass.TXN_TYPE]
    decided_at: Annotated[str | None, DataClass.TIMESTAMP_OBSERVABILITY]
    confidence: Annotated[float | None, DataClass.AGGREGATE]
    signal: Annotated[str, DataClass.TXN_TYPE]

    @classmethod
    def from_decision_row(cls, r: dict[str, Any]) -> MerchantLinkHistoryRow:
        """Map a decoded ``merchant_link_decisions`` row into the history payload."""
        return cls(
            decision_id=r["decision_id"],
            ref_value=r["ref_value"],
            source_type=r["source_type"],
            candidate_merchant_id=r["candidate_merchant_id"],
            provider_merchant_name=r.get("provider_merchant_name"),
            status=r["status"],
            decided_by=r["decided_by"],
            decided_at=str(r["decided_at"])
            if r.get("decided_at") is not None
            else None,
            confidence=(
                float(r["confidence_score"])
                if r.get("confidence_score") is not None
                else None
            ),
            signal=signal_from_match_signals(r.get("match_signals")),
        )


@dataclass(frozen=True, slots=True)
class MerchantLinksHistoryPayload:
    """Payload for merchants_links_history — decision log, newest first."""

    decisions: list[MerchantLinkHistoryRow]

    @classmethod
    def from_rows(cls, rows: Iterable[dict[str, Any]]) -> MerchantLinksHistoryPayload:
        """Build the history payload from ``MerchantLinksService.history()`` rows."""
        return cls(
            decisions=[MerchantLinkHistoryRow.from_decision_row(r) for r in rows]
        )


# ---------------------------------------------------------------------------
# merchants_links_run
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class MerchantLinksRunPayload:
    """Payload for merchants_links_run — harvest outcome counts.

    ``bound`` are provider entity ids silently bound to a single unambiguous
    merchant (no review needed); ``conflicts`` are one-id-many-merchant cases
    queued as pending decisions for review. They are reported distinctly — a
    bound binding is NOT a pending proposal.
    """

    bound: Annotated[int, DataClass.AGGREGATE]
    conflicts: Annotated[int, DataClass.AGGREGATE]
