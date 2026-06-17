"""Input/result types for AccountResolver (keeps the resolve() signature stable)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypedDict


class AccountCandidateDict(TypedDict):
    """Serialized shape of one ``AccountCandidate`` carried across the envelope."""

    account_id: str
    display_name: str
    confidence: float
    signal: str


class AccountProposalDict(TypedDict):
    """Serialized shape of one ``AccountProposal`` (``account_proposals`` entry)."""

    source_account_key: str
    proposed_account_id: str | None
    is_new: bool
    adopted_via: str | None
    requires_confirm: bool
    candidates: list[AccountCandidateDict]


@dataclass(frozen=True)
class AccountCandidate:
    """One weak-signal merge candidate surfaced for confirmation."""

    account_id: str
    display_name: str
    confidence: float
    signal: str  # "institution_last4" | "name"


@dataclass(frozen=True)
class AccountProposal:
    """The resolver verdict for one detected source account, surfaced to confirm.

    ``requires_confirm`` encodes the surfacing rule structurally: a proposal with
    weak candidates ALWAYS surfaces; a strong-confirmer adoption (``adopted_via``
    set, no candidates) never does. A brand-new standalone account (is_new, no
    adoption, no candidates) also surfaces — minting a new account is a visible
    moment (spec Decision 7).
    """

    source_account_key: str
    proposed_account_id: str | None
    is_new: bool
    candidates: tuple[AccountCandidate, ...] = ()
    adopted_via: str | None = (
        None  # "source_native"|"persistent_token"|"full_number"|"explicit"
    )
    """``None`` for a declared-new (force_standalone) proposal — no preview id
    exists; ``resolve()`` mints the real id at commit time."""

    @property
    def requires_confirm(self) -> bool:
        """True when the proposal must be shown to the user before import proceeds."""
        return bool(self.candidates) or (self.is_new and self.adopted_via is None)

    def to_dict(self) -> AccountProposalDict:
        """Serialise to a typed dict for surface display.

        Includes opaque ids, display_name, confidence, and signal.
        Never exposes ref_value or other PII-bearing fields.
        """
        return {
            "source_account_key": self.source_account_key,
            "proposed_account_id": self.proposed_account_id,
            "is_new": self.is_new,
            "adopted_via": self.adopted_via,
            "requires_confirm": self.requires_confirm,
            "candidates": [
                {
                    "account_id": c.account_id,
                    "display_name": c.display_name,
                    "confidence": c.confidence,
                    "signal": c.signal,
                }
                for c in self.candidates
            ],
        }


@dataclass(frozen=True)
class SourceAccount:
    """One source account presented to the resolver.

    ``source_account_key`` is the source's own native account key (OFX number,
    CSV slug, Plaid token) — the ``source_native`` ref_value staging joins on.
    PII fields (``account_number``) are used as scoped confirmers and never logged.
    """

    source_type: str
    source_origin: str
    source_account_key: str
    account_name: str
    account_number: str | None = None
    last_four: str | None = None
    institution: str | None = None
    persistent_token: str | None = None
    explicit_account_id: str | None = None
    force_standalone: bool = False
    """User declared this a NEW standalone account: mint fresh, skip the
    weak-candidate merge pass. Set by an import-time ``account_bindings`` entry
    of ``"new"``. Still idempotent on re-import (adopts an existing
    source_native above)."""


@dataclass(frozen=True)
class ResolvedAccount:
    """The resolver's verdict for one source account."""

    account_id: str
    """Canonical, opaque uuid4[:12] (or the pinned/adopted existing id)."""

    is_new: bool
    """True when a fresh canonical account was minted this call."""

    pending_decision_ids: tuple[str, ...] = ()
    """Decision rows written for weak (institution+last4 / name) candidates."""

    outcome: str = "minted_new"
    """One of the ACCOUNT_LINK_OUTCOMES_TOTAL result labels."""


@dataclass(frozen=True)
class PendingLinkCandidate:
    """One candidate merge proposal within a pending-review group."""

    decision_id: str
    candidate_account_id: str
    candidate_display_name: str
    confidence: float | None
    signal: str  # e.g. "institution_last4" | "name"


@dataclass(frozen=True)
class PendingLinkGroup:
    """One provisional account awaiting review + its candidate proposals."""

    provisional_account_id: str
    provisional_display_name: str
    candidates: tuple[PendingLinkCandidate, ...]
