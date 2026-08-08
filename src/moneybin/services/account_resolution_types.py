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
    proposal_ref: str
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
    # "institution_last4" | "name" | "institution_reissue" | "institution" |
    # "fallback". The first three fired on real evidence and reach the persisted
    # decision log. The last two are the interactive import gate's last-resort
    # pick-list (existing accounts shown when nothing cleared); never emitted on
    # the backfill link queue.
    signal: str


@dataclass(frozen=True)
class AccountProposal:
    """The resolver verdict for one detected source account, surfaced to confirm.

    ``requires_confirm`` encodes the surfacing rule structurally: a proposal with
    weak candidates ALWAYS surfaces, and so does a source that stated no
    identity at all (``identity_unknown``); everything else proceeds. Both are
    forms of uncertainty. Candidates mean the import is about to adopt or merge
    onto an account that already exists, on a signal too weak to trust, and a
    wrong merge is hard to notice and hard to undo. ``identity_unknown`` means
    the file named no account, so the mint would be under a filename guess.
    A strong-confirmer adoption (``adopted_via`` set) has no ambiguity, and a
    mint of a stated identity has nothing to merge into and no other answer
    available.

    A mint is reported rather than gated. "Magic stays visible" calibrates to
    the cost of a wrong silent action: a surprise account is visible in the
    account list and cheap to rename or merge, unlike the silent merge this
    gate exists to prevent. Gating it made a first import of N files cost N
    confirms that each had exactly one legal answer.
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

    identity_unknown: bool = False
    """The source stated no account identity at all — a bare
    Date/Description/Amount CSV. Set by the caller that asked for a fallback
    pick-list, because only the caller knows the file carried no signal.

    Distinct from "new". Every other mint records an identity the file actually
    stated (an OFX ``<ACCTID>``, a statement's issuer + last four); this one
    would mint under the filename stem, which is a guess. Candidates cannot
    express it: on a first import there is nothing to offer, so the pick-list is
    empty and the proposal would be indistinguishable from a confident mint."""

    @property
    def requires_confirm(self) -> bool:
        """True when the proposal must be shown to the user before import proceeds."""
        return bool(self.candidates) or self.identity_unknown

    def to_dict(self, *, proposal_ref: str) -> AccountProposalDict:
        """Serialise to a typed dict for surface display.

        Includes opaque ids, display_name, confidence, and signal.
        Never exposes ref_value or other PII-bearing fields.

        ``proposal_ref`` is supplied by the caller rather than held on the
        proposal: it names this account's position in the file being imported,
        which is a fact about that import, not about the resolver's verdict.
        """
        return {
            "source_account_key": self.source_account_key,
            "proposal_ref": proposal_ref,
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

    unpinned_account_key: str | None = None
    """The key this source would derive on its own, when ``explicit_account_id``
    has replaced ``source_account_key`` with the canonical id.

    A pin that links only the canonical id teaches the resolver nothing about the
    document: the next import of the same source, without the pin, derives this
    key, finds no link, and mints a second account for the same thing. Recording
    it alongside the pin is what makes the pin stick. Never re-points a key
    already bound elsewhere — see ``AccountResolver._teach_unpinned_key``, which
    holds that guard; ``_run_ladder`` only calls it."""

    def __post_init__(self) -> None:
        """Canonicalize a blank last four to None — they mean the same thing.

        ``SyncAccount.mask`` declares only a maximum length, so the sync server
        can send ``""`` or ``"  "``; a source that writes an empty column
        produces the same. All answer the last4 rung with silence, but the
        resolver asks whether that answer is missing in two conventions — ``is
        None`` at the quarantine gates, falsy at the lookup and reissue passes —
        and neither ``"" is None`` nor ``bool("  ")`` agrees. Canonicalizing here
        is what keeps the two from disagreeing, rather than requiring every
        present and future consumer to pick the right one.

        Stripping, not just an empty-string test: a whitespace-only mask is
        truthy and non-None, so it would clear the quarantine gate that ``""``
        cannot. Padding around real digits is the same defect one step along —
        the last4 lookup matches exactly, so ``" 1234 "`` would mint a second
        account for a ledger that already has one.
        """
        if self.last_four is not None:
            stripped = self.last_four.strip()
            object.__setattr__(self, "last_four", stripped or None)


@dataclass(frozen=True)
class ResolvedAccount:
    """The resolver's verdict for one source account."""

    account_id: str
    """Canonical, opaque uuid4[:12] (or the pinned/adopted existing id)."""

    is_new: bool
    """True when a fresh canonical account was minted this call."""

    pending_decision_ids: tuple[str, ...] = ()
    """Decision rows for weak candidates (institution+last4, name, reissue)."""

    outcome: str = "minted_new"
    """One of the ACCOUNT_LINK_OUTCOMES_TOTAL result labels."""


@dataclass(frozen=True)
class PendingLinkCandidate:
    """One candidate merge proposal within a pending-review group."""

    decision_id: str
    candidate_account_id: str
    candidate_display_name: str
    confidence: float | None
    # "institution_last4" | "name" | "institution_reissue" — only these three.
    # Narrower than _Candidate.signal: this reads persisted decision rows, and
    # the single insert site passes fallback=False, so the gate's last-resort
    # pick-list ("institution" / "fallback") is never written to review.
    signal: str


@dataclass(frozen=True)
class PendingLinkGroup:
    """One provisional account awaiting review + its candidate proposals."""

    provisional_account_id: str
    provisional_display_name: str
    candidates: tuple[PendingLinkCandidate, ...]
