"""Input/result types for AccountResolver (keeps the resolve() signature stable)."""

from __future__ import annotations

import string
from dataclasses import dataclass
from typing import TYPE_CHECKING, NotRequired, TypedDict, TypeGuard

from moneybin.services.entity_reference import normalize_reference
from moneybin.services.ledger_overlap import LedgerOverlap

if TYPE_CHECKING:  # import-time cycle: account_display_name reads the
    # UNNAMED_ACCOUNT_LABEL defined below, so the edge back is annotation-only.
    from moneybin.services.account_display_name import AccountNameFacts

_ACCOUNT_IDENTIFIER_CHARACTERS = frozenset(string.ascii_letters + string.digits)


def normalize_account_identifier(value: str) -> str:
    """Canonical cross-source form for a complete account identifier."""
    return "".join(
        character.upper()
        for character in value
        if character in _ACCOUNT_IDENTIFIER_CHARACTERS
    )


UNNAMED_ACCOUNT_LABEL = "Unnamed account"
"""What every surface calls an account nothing can name.

Duplicated as a literal in the terminal COALESCE arm of
``core.dim_accounts.display_name``, because SQL cannot import it. The two are
pinned together by ``test_dim_accounts_merge.py``, which asserts the model's
output against this constant after a real SQLMesh run -- so a drift in either
copy fails there rather than in front of a user.

One constant rather than a per-call-site literal because both spellings render
in the same table: ``core`` supplies this string for a row it could not name,
while the CLI and MCP substitute it for a name that is absent or was frozen as
``""``. Those are different states with one honest answer, and rendering them
as ``Unnamed account`` beside ``unnamed account`` reads as a bug.

Lives here rather than beside either consumer because both the merge matcher
and the free-text resolver must agree on it, and they import nothing from each
other.
"""

_RESERVED_ACCOUNT_NAME_FOLD = normalize_reference(UNNAMED_ACCOUNT_LABEL)


def is_reserved_account_name(value: object) -> bool:
    """Whether a proposed account name folds onto ``UNNAMED_ACCOUNT_LABEL``.

    Folded with the resolver's own ``normalize_reference``, not a weaker
    comparison. Its third rung matches on that fold, so anything it collapses
    onto the label -- a case variant, padding, a doubled space, an
    NFKC-equivalent character -- would otherwise be accepted as a name and then
    answer a request for the label another account displays: with generated
    placeholders filtered out of the candidate-name slot, such a row is the
    unique hit. Reserving on a narrower fold than the matcher uses leaves
    exactly that difference as a hole.

    One predicate rather than the comparison spelled at each site, because
    every write path that accepts an account name has to reject the same set,
    and a second spelling is how one of them ends up narrower. ``system
    doctor`` asks it too, of rows that were stored before a guard existed.
    """
    return (
        isinstance(value, str)
        and normalize_reference(value) == _RESERVED_ACCOUNT_NAME_FOLD
    )


def is_a_name(display_name: str | None) -> TypeGuard[str]:
    """Whether a dim label identifies an account, or only says nothing names it.

    ``UNNAMED_ACCOUNT_LABEL`` is one fixed string, so every account that reaches
    the dim's terminal arm carries a byte-identical label while agreeing with
    the others on nothing at all. Any code that treats ``display_name`` as an
    identifying string has to ask this first, because on that label a string
    comparison reports a perfect match between two unrelated accounts:

    - ``AccountResolver`` would file a merge proposal whose entire evidence is
      that neither account could be named.
    - ``AccountService.resolve`` would return both at confidence ``1.0`` to a
      user or agent who typed back the label MoneyBin itself displayed.

    Empty is refused for the same reason: it too compares equal to itself. The
    dim cannot currently emit one -- the COALESCE terminates on the sentinel --
    but a predicate that is right about the sentinel and wrong about its
    neighbour is not worth writing.

    This is deliberately not a check for "looks unhelpful". A real name that
    happens to be vague is still the user's name for the account.
    """
    return bool(display_name) and display_name != UNNAMED_ACCOUNT_LABEL


def matchable_account_name(display_name: str | None) -> str:
    """The name a reference may match an account by, or ``""`` when it has none.

    Four candidate builders project accounts into the shared
    ``resolve_entity_reference`` contract, which matches a reference against a
    candidate's name. An account MoneyBin could not name has no name to offer,
    and both spellings of that state -- the sentinel and empty -- compare equal
    to themselves across unrelated accounts, so neither may sit in the slot.

    **Never substitute the ``account_id``.** It is the obvious filler and it is
    wrong twice over. ``EntityCandidate`` matches ids off ``entity_id``, so the
    name slot buys no id resolution; and an account with no resolver link
    carries its source-native key as its ``account_id`` -- on OFX a real
    ``<ACCTID>``. ``AccountNotFoundError`` renders every candidate name into a
    message that ``handle_cli_errors`` logs, whose safety rests on that message
    being a fixed MoneyBin string, so an id here becomes an account number in
    ``cli_YYYY-MM-DD.log``. Dropping the id from the dim's terminal name is the
    whole point of the change this helper serves; putting it back one layer
    down would undo it.

    The candidate itself stays: ``entity_id`` still carries the id, so such an
    account remains addressable exactly as before.
    """
    return display_name if is_a_name(display_name) else ""


class AccountCandidateDict(TypedDict):
    """Serialized shape of one ``AccountCandidate`` carried across the envelope."""

    account_id: str
    display_name: str
    signal: str
    overlap_matched: NotRequired[int]
    overlap_comparable: NotRequired[int]
    overlap_window_days: NotRequired[int]
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
    """One weak-signal merge candidate surfaced for confirmation.

    ``confidence`` is a literal per rung that no input can move, so candidates
    found on one rung tie at one number however differently their ledgers
    overlap. It survives here for one job only: it is what gets written to
    ``app.account_link_decisions.confidence_score``, whose stored meaning is
    'the constant the resolver stamped'. ``to_dict`` deliberately drops it, for
    the same reason ``PendingLinkCandidate`` and ``LinkCandidateRow`` have no
    such field: a number named confidence invites whoever answers the gate — a
    human skimming, or an agent choosing a binding — to read a tie as a
    judgement about these two accounts. ``overlap`` is what the surface
    carries instead, because it varies with them.
    """

    account_id: str
    display_name: str
    confidence: float
    # "legacy_pdf_identity" | "institution_last4" | "last_four" | "name" |
    # "institution_reissue" | "institution" | "fallback". The first five fired
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

        Includes opaque ids, display_name, signal, and optional
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
                "signal": candidate.signal,
            }
            if candidate.overlap is not None:
                serialized.update({
                    "overlap_matched": candidate.overlap.matched,
                    "overlap_comparable": candidate.overlap.comparable,
                    "overlap_window_days": candidate.overlap.window_days,
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
    account_name_is_user_set: bool = False
    """Whether ``account_name`` is a person- or source-authored label rather
    than a generated fallback (institution + type, a bare filename, a raw
    token). Mirrors ``core.dim_accounts.display_name_is_user_set`` on the
    candidate side: the resolver's name rung requires this on the SOURCE side
    too, so a channel that has no authored name field (OFX has none at all)
    can't have its generated placeholder read back as name evidence. Default
    False is the safe reading for a channel that never sets it."""
    account_number: str | None = None
    last_four: str | None = None
    institution: str | None = None
    persistent_token: str | None = None
    legacy_source_account_key: str | None = None
    """A superseded source key that may nominate a review candidate, never adopt."""
    legacy_source_origin: str | None = None
    """The origin that scoped ``legacy_source_account_key`` before replacement."""
    legacy_source_account_key_is_filename_alias: bool = False
    """Whether the legacy key came from an anchorless PDF filename alias."""
    source_file: str | None = None
    """Canonical source path used only to recover a proven historical PDF tuple."""
    unpinned_account_key: str | None = None
    """The key this source derives on its own, when a pin made it use another.

    A pinned import borrows the key its account already answers to so the rows
    dedup, which leaves nothing on record identifying THIS file. Carried here so
    the resolver can also link the derived key, and an unpinned re-import of the
    same file still recognises the account instead of asking or minting."""

    name_facts: AccountNameFacts | None = None
    """What ``core.dim_accounts`` will name this account by, if it mints one.

    Never a resolution signal — the resolver ignores it. It rides here because
    the mint report (``accounts_created``) is built long after the channel that
    knows which institution spelling and which account-number column the model
    will read. Distinct from ``account_name`` beside it, which is the file's raw
    free-text label and feeds fuzzy matching: ``name_facts.source_label`` is the
    display-safe form of that label, and is the top rung the model names by.
    Left None only by callers that never report a mint (the sync path, the
    resolver's own probes)."""

    explicit_account_id: str | None = None
    force_standalone: bool = False
    """User declared this a NEW standalone account: mint fresh, skip the
    weak-candidate merge pass. Set by an import-time ``account_bindings`` entry
    of ``"new"``. Still idempotent on re-import (adopts an existing
    source_native above)."""

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
    # "institution_last4" | "last_four" | "name" | "institution_reissue" |
    # "manual" (a caller named the pair; no signal fired) — only these five.
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
