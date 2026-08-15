"""Input/result types for AccountResolver (keeps the resolve() signature stable)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import NotRequired, TypedDict

from moneybin.services.ledger_overlap import LedgerOverlap


class AccountCandidateDict(TypedDict):
    """Serialized shape of one ``AccountCandidate`` carried across the envelope."""

    account_id: str
    display_name: str
    confidence: float
    signal: str
    overlap_matched: NotRequired[int]
    overlap_comparable: NotRequired[int]
    overlap_window_start: NotRequired[str | None]
    overlap_window_end: NotRequired[str | None]


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
    # "legacy_pdf_identity" | "institution_last4" | "name" |
    # "institution_reissue" | "institution" | "fallback". The first four fired
    # on real evidence. The last two are the interactive import gate's
    # last-resort pick-list; never emitted on the backfill link queue.
    signal: str
    overlap: LedgerOverlap | None = None


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

    Distinct from "new". Every other mint records identity evidence from the
    file (an OFX ``<ACCTID>``, or captured PDF account traits alongside its
    exact-document digest); this one has only a placeholder display name and no
    evidence linking it across files. Candidates cannot
    express it: on a first import there is nothing to offer, so the pick-list is
    empty and the proposal would be indistinguishable from a confident mint."""

    @property
    def requires_confirm(self) -> bool:
        """True when the proposal must be shown to the user before import proceeds."""
        return bool(self.candidates) or self.identity_unknown

    def to_dict(self, *, proposal_ref: str) -> AccountProposalDict:
        """Serialise to a typed dict for surface display.

        Includes opaque ids, display_name, confidence, signal, and optional
        aggregate/date-window ledger-overlap evidence.
        Never exposes ref_value or other PII-bearing fields.

        ``proposal_ref`` is supplied by the caller rather than held on the
        proposal: it names this account's position in the file being imported,
        which is a fact about that import, not about the resolver's verdict.
        """
        candidates: list[AccountCandidateDict] = []
        for candidate in self.candidates:
            serialized: AccountCandidateDict = {
                "account_id": candidate.account_id,
                "display_name": candidate.display_name,
                "confidence": candidate.confidence,
                "signal": candidate.signal,
            }
            if candidate.overlap is not None:
                serialized.update({
                    "overlap_matched": candidate.overlap.matched,
                    "overlap_comparable": candidate.overlap.comparable,
                    "overlap_window_start": (
                        candidate.overlap.window_start.isoformat()
                        if candidate.overlap.window_start is not None
                        else None
                    ),
                    "overlap_window_end": (
                        candidate.overlap.window_end.isoformat()
                        if candidate.overlap.window_end is not None
                        else None
                    ),
                })
            candidates.append(serialized)
        return {
            "source_account_key": self.source_account_key,
            "proposal_ref": proposal_ref,
            "proposed_account_id": self.proposed_account_id,
            "is_new": self.is_new,
            "adopted_via": self.adopted_via,
            "requires_confirm": self.requires_confirm,
            "candidates": candidates,
        }


@dataclass(frozen=True)
class SourceAccount:
    """One source account presented to the resolver.

    ``source_account_key`` is the source's native key (OFX number, CSV slug,
    Plaid token, or PDF document digest) — the ``source_native`` ref_value
    staging joins on.
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
    legacy_source_account_key: str | None = None
    """A superseded source key that may nominate a review candidate, never adopt."""
    legacy_source_origin: str | None = None
    """The origin that scoped ``legacy_source_account_key`` before replacement."""

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
    """One candidate merge proposal within a pending-review group.

    Carries no confidence number. ``_weak_signal_candidates`` hardcoded 0.5 for
    the last-four signal and 0.4 for the name signal under the comment
    "confidence is informational only"; nothing accumulated evidence and nothing
    thresholded on it, so no input could ever move either one. Rendering a
    constant as a score invited a reviewer to read it as one. ``overlap`` is what
    replaces it — a measurement of the two ledgers that actually varies with the
    accounts in front of the reviewer.
    """

    decision_id: str
    candidate_account_id: str
    candidate_display_name: str
    # "institution_last4" | "name" | "institution_reissue" — only these three.
    # Narrower than _Candidate.signal: this reads persisted decision rows, and
    # the single insert site passes fallback=False, so the gate's last-resort
    # pick-list ("institution" / "fallback") is never written to review.
    signal: str
    overlap: LedgerOverlap


@dataclass(frozen=True)
class PendingLinkGroup:
    """One provisional account awaiting review + its candidate proposals.

    ``transactions`` is the magnitude at browse time. It sat only in the confirm
    binding before, computed inside the decide call, so a reviewer chose which
    proposals were worth opening while nothing had yet told them how much history
    each one moves.
    """

    provisional_account_id: str
    provisional_display_name: str
    candidates: tuple[PendingLinkCandidate, ...]
    transactions: int = 0
