"""The sentence a human ratifies before an identity batch commits.

Sibling of ``import_confirmation``: the prose a confirm gate shows lives beside
the domain it describes rather than inside one surface, so the MCP elicitation
and the CLI prompt cannot drift into describing the same merge two different
ways. The MCP tool wrapper is where it used to live, which put the CLI a copy
away from a sentence it needs to render identically.

Presentation only: everything here takes already-resolved values and returns a
string. Gathering the facts costs a database read, which is why
``AccountLinksService.merge_facts`` owns that half — a caller that only needs to
*print* a confirmation should not load the repositories and sibling services that
applying one requires. The one import below that reaches outside this module is
``LedgerOverlap``, a value type; duplicating it here to preserve a purity claim
would let the rendered evidence disagree with the probe that measured it.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date

from moneybin.services.ledger_overlap import LedgerOverlap

#: Every blast-radius category an identity confirmation reports, named once.
#:
#: The confirm binding indexes ``affected_ids`` by exactly this set, so the two
#: lists have to agree: a preparer that omits a key raises at confirmation time,
#: and a category added to a preparer but not here is a mutation the prompt
#: silently drops. ``price_marks`` was the second kind — the merge moved every
#: override while the prompt counted only the first five.
#:
#: It lives beside the labels rather than in the decision service so that
#: rendering the sentence costs an importer nothing but this module: a CLI that
#: only needs to print a confirmation should not load the repositories and
#: sibling services that applying one requires.
IDENTITY_BLAST_RADIUS_CATEGORIES = (
    "accounts",
    "merchants",
    "securities",
    "transactions",
    "lots",
    "price_marks",
)

#: How each blast-radius category is named in the prompt a human ratifies.
#:
#: Deliberately a second list beside ``IDENTITY_BLAST_RADIUS_CATEGORIES`` rather
#: than a field on it: the categories index the digest an approval binds to, and
#: these name those categories in the sentence a human reads. The two must agree,
#: which ``test_every_blast_radius_category_has_a_prompt_label`` asserts by set
#: equality. A category counted in the digest but absent from this map would be
#: bound into the approval and left out of the sentence explaining it.
IDENTITY_BLAST_RADIUS_LABELS: dict[str, tuple[str, str]] = {
    "accounts": ("account", "accounts"),
    "merchants": ("merchant", "merchants"),
    "securities": ("security", "securities"),
    "transactions": ("transaction", "transactions"),
    "lots": ("tax lot", "tax lots"),
    "price_marks": ("price mark you set by hand", "price marks you set by hand"),
}

#: How each surface spells the command that reverses a committed merge.
#:
#: The one clause in this prompt that must differ by surface, and the exception
#: that proves the rest of the module: the sentence is shared so two surfaces
#: cannot describe one merge differently, but a recovery instruction is worth
#: nothing unless it runs where it is read. A CLI user told to call
#: ``system_audit_undo(operation_id=...)`` has no way to run it, and this prompt
#: accompanies a destructive merge — the one moment the recovery path has to be
#: reachable without a second lookup.
#:
#: ``surface`` is a required argument rather than a defaulted one so a third
#: surface has to answer this question instead of silently inheriting whichever
#: spelling happened to be the default.
UNDO_COMMANDS: dict[str, str] = {
    "cli": "moneybin system audit undo <operation_id>",
    "mcp": "system_audit_undo(operation_id=...)",
}

#: What an accepted link of each kind does, in the user's terms.
#:
#: Rendered only for the kinds a batch actually contains. The single paragraph
#: this replaces enumerated all three unconditionally, so a card-to-card account
#: link was told about security tax lots and hand-set price marks — consequences
#: that do not apply to the decision in hand, in the one place a human is
#: deciding whether the risk is acceptable.
_KIND_CLAUSES: dict[str, str] = {
    "account_link": (
        "move one account's whole transaction history onto the surviving account"
    ),
    "merchant_link": "merge merchant attribution onto the surviving merchant",
    "security_link": (
        "merge security tax lots, manual events, and any price marks you set by "
        "hand onto the surviving instrument, or bind a price-feed symbol without "
        "merging anything"
    ),
}

#: Fallback paragraph for a caller that names no kinds — every kind, as before.
_EVERY_KIND_PARAGRAPH = (
    "Confirm this complete identity-decision batch. Accepted links can merge "
    "account histories, merchant attribution, or security lots — including any "
    "price marks you set by hand, which move onto the survivor — or bind a "
    "price-feed symbol without merging anything; every decision in the ordered "
    "batch will commit together."
)

_BATCH_TAIL = "Every decision in this batch commits together."


@dataclass(frozen=True)
class AccountLedgerFacts:
    """One account, described by the things that distinguish it from its twin.

    Deliberately not built around the masked last four. Two accounts proposed
    for a merge routinely carry the *same* one — that is what the institution
    plus last-four signal fires on — so a description keyed to it renders both
    sides identically in exactly the case a human most needs them apart. What
    differs is the source that reported each one, how much history it holds,
    the period it covers, its subtype, and its currency.
    """

    account_id: str
    display_name: str = ""
    source_types: tuple[str, ...] = ()
    subtype: str | None = None
    currency_code: str | None = None
    transactions: int = 0
    first_date: date | None = None
    last_date: date | None = None
    #: Masked-form last four, carried as evidence rather than as a label. It
    #: cannot tell the two sides apart — the institution+last-four signal fires
    #: because they agree — but agreement is why the proposal exists and
    #: disagreement is evidence against it, and the prompt showed neither.
    last_four: str | None = None


@dataclass(frozen=True)
class AccountMergeFacts:
    """One proposed account merge, with the direction and the evidence for it."""

    absorbed: AccountLedgerFacts
    survivor: AccountLedgerFacts
    overlap: LedgerOverlap = field(
        default_factory=lambda: LedgerOverlap(
            comparable=0, matched=0, window_start=None, window_end=None
        )
    )


#: Source types that arrive through the mediated sync server.
#:
#: ``source_type`` records the provider slug the server happens to speak, and
#: the client's contract is that those providers are implementation details
#: hidden behind it (AGENTS.md, "Sync server is opaque"). This sentence is read
#: by a human in the CLI and by an agent over MCP, so the label names the
#: channel the user knows — ``moneybin sync pull`` — never the vendor. A format
#: like OFX or a store like GSheet is the user's own and stays named.
_MEDIATED_SYNC_SOURCES = frozenset({"plaid"})
_SYNC_SOURCE_LABEL = "SYNC"


def _source_label(facts: AccountLedgerFacts) -> str | None:
    """Uppercased source origins — "PDF", "OFX", "PDF+OFX" — or None when unknown."""
    labels = dict.fromkeys(
        _SYNC_SOURCE_LABEL if source in _MEDIATED_SYNC_SOURCES else source.upper()
        for source in facts.source_types
    )
    return "+".join(labels) or None


def _account_label(side: AccountLedgerFacts) -> str:
    """Name one account for a human, never as a bare id.

    Takes one side, not a pair. Deriving the label from the *difference*
    between two accounts is what made it collapse: a shared display name was
    discarded outright, so the case the institution+last-four signal fires on —
    two accounts that look alike — is exactly the case that rendered as "the
    account (a1b2c3d4e5f6)". A shared name is still the most useful text on the
    line; what separates the two sides is the traits printed beside it.

    Confined to this prompt. The list surfaces answer the same question from a
    resolved name string rather than ``AccountLedgerFacts``, so they carry their
    own fallbacks; sharing this would mean giving it a second signature, which
    buys less than it costs while there are exactly two shapes.
    """
    name = (side.display_name or "").strip()
    source = _source_label(side)
    if name and source:
        return f"{name}, {source}-derived"
    if name:
        return name
    if source:
        return f"the {source}-derived account"
    return "the account"


def _side_traits(side: AccountLedgerFacts, other: AccountLedgerFacts) -> list[str]:
    """The magnitude, the period, and whatever else separates the two sides."""
    traits = [f"{side.transactions:,} transactions"]
    if side.first_date and side.last_date:
        traits.append(f"{side.first_date.isoformat()} → {side.last_date.isoformat()}")
    if side.subtype != other.subtype:
        traits.append(side.subtype or "no subtype")
    if side.currency_code != other.currency_code:
        traits.append(side.currency_code or "no currency")
    return traits


def _side_phrase(side: AccountLedgerFacts, other: AccountLedgerFacts) -> str:
    """One side as name, then traits, then id — the id a reference, not the identity."""
    return (
        f"{_account_label(side)}, {', '.join(_side_traits(side, other))} "
        f"[{side.account_id}]"
    )


def _evidence_line(overlap: LedgerOverlap) -> str:
    """State how much of the absorbed ledger the survivor already holds.

    An unmeasurable probe says so in words. Rendering it as "0 of 0" would read
    as a merge with nothing in common, which is evidence against — the opposite
    of what no comparable period means.
    """
    if not overlap.measurable:
        return (
            "The two ledgers share no comparable period, so there is no overlap "
            "evidence either way."
        )
    window = ""
    if overlap.window_start and overlap.window_end:
        window = (
            f" over {overlap.window_start.isoformat()} → "
            f"{overlap.window_end.isoformat()}"
        )
    return (
        f"{overlap.matched:,} of {overlap.comparable:,} of the absorbed "
        f"account's transactions{window} already appear in the surviving "
        "account's ledger."
    )


#: Every fact the prompt holds about an account except its id. Set-compared in
#: ``test_the_compared_facts_cover_every_ledger_fact_but_the_id`` so a field added to
#: ``AccountLedgerFacts`` cannot silently drop out of the indistinguishable
#: check — which would let a pair the prompt calls distinguishable render as two
#: identical descriptions again.
COMPARED_LEDGER_FACTS = (
    "display_name",
    "source_types",
    "subtype",
    "currency_code",
    "transactions",
    "first_date",
    "last_date",
    "last_four",
)


#: How each compared fact is named in the sentence that reports a tie, in
#: reading order. The sentence is built from this map instead of retyping the
#: list, because ``COMPARED_LEDGER_FACTS`` is only set-compared: a ninth field
#: would satisfy that guard while the hand-written prose beside it silently
#: became a list that omits one. Two fields share a phrase — a date range is one
#: idea to a reader and two columns to the comparison.
FACT_PHRASES = {
    "display_name": "name",
    "source_types": "source",
    "subtype": "subtype",
    "currency_code": "currency",
    "first_date": "date range",
    "last_date": "date range",
    "transactions": "transaction count",
    "last_four": "last four",
}


def _fact_list(*, joiner: str) -> str:
    """The compared facts as prose, de-duplicated, in reading order."""
    phrases = list(dict.fromkeys(FACT_PHRASES.values()))
    return f"{', '.join(phrases[:-1])}, {joiner} {phrases[-1]}"


def _holds_any_fact(side: AccountLedgerFacts) -> bool:
    """Whether MoneyBin knows anything at all about this account beyond its id."""
    return any(getattr(side, field) for field in COMPARED_LEDGER_FACTS)


def _last_four_line(merge: AccountMergeFacts) -> str:
    """State the last-four evidence, in whichever of its four forms applies.

    Always rendered. Agreement cannot tell the two sides apart, which is an
    argument against using the last four as a *label*, not against showing it as
    *evidence*. The prompt named ledger overlap as its only evidence while the
    field most likely to have drawn the two together appeared nowhere, so a
    reviewer could not weigh it.

    Stated as evidence, never as cause. ``AccountMergeFacts`` carries no record
    of which signal produced the proposal, and ``account_name`` matching can
    pair two accounts whose last fours agree by coincidence -- so "the signal
    this fired on" was a claim this function has no way to check, in the one
    place an irreversible confirm must not overstate what it knows.
    """
    absorbed, survivor = merge.absorbed.last_four, merge.survivor.last_four
    if absorbed and survivor and absorbed == survivor:
        return (
            f"Both accounts state the same last four (…{absorbed}), which is "
            "evidence for the merge."
        )
    if absorbed and survivor:
        return (
            f"⚠️  These accounts state different last fours (…{absorbed} vs "
            f"…{survivor}), which is evidence against the merge."
        )
    if absorbed or survivor:
        side = "absorbed" if absorbed else "surviving"
        return (
            f"Only the {side} account states a last four "
            f"(…{absorbed or survivor}); the other states none."
        )
    return "Neither account states a last four, so it is no evidence either way."


def _indistinguishable(
    absorbed: AccountLedgerFacts, survivor: AccountLedgerFacts
) -> bool:
    """Whether the two sides agree on every fact the prompt holds but the id."""
    return all(
        getattr(absorbed, field) == getattr(survivor, field)
        for field in COMPARED_LEDGER_FACTS
    )


def _account_merge_block(merge: AccountMergeFacts, undo_command: str) -> str:
    """The question, the evidence, and what the answer does — in that order."""
    absorbed, survivor = merge.absorbed, merge.survivor
    lines = [
        f"Merge {_side_phrase(absorbed, survivor)} "
        f"into {_side_phrase(survivor, absorbed)}?",
        "",
        _last_four_line(merge),
        "",
        _evidence_line(merge.overlap),
        "",
        f"The absorbed account [{absorbed.account_id}] is folded into "
        f"[{survivor.account_id}]: its transactions move onto that account's "
        f"history and nothing is deleted. Reverse with {undo_command}.",
    ]
    if _indistinguishable(absorbed, survivor):
        # Naming the tie is the difference between a prompt that reads as a
        # rendering failure and one that reads as the finding it is. The merge
        # stays available: the reviewer may know something the ledger does not.
        #
        # Two sides agree trivially when nothing is known about either — a
        # profile with decisions but no materialized core describes both as
        # empty, and every field then matches. Calling that "identical on every
        # fact" tells the reviewer the evidence is overwhelming at the moment
        # there is none, which is the one reading an irreversible confirm must
        # never invite.
        if _holds_any_fact(absorbed):
            lines.append(
                "These two accounts are identical on every fact MoneyBin holds "
                f"— {_fact_list(joiner='and')}. Only the ids tell them apart."
            )
        else:
            lines.append(
                "MoneyBin holds no facts about either account — no "
                f"{_fact_list(joiner='or')} — so there is nothing here to "
                "support the merge or to tell the two apart."
            )
    if survivor.transactions == 0:
        # The reversed proposal is the expensive failure on this path: the live
        # queue offered a malformed placeholder as the survivor, where accepting
        # would have made it canonical. An empty surviving ledger is the one
        # cheap, legible tell for it.
        lines.append(
            "The surviving account has no transactions of its own — check the "
            "direction before accepting."
        )
    return "\n".join(lines)


def _batch_paragraph(kinds: Collection[str], *, accounts_described: bool) -> str:
    """Name what the batch's remaining accepted links do, and nothing else."""
    if not kinds:
        return _EVERY_KIND_PARAGRAPH
    remaining = [
        kind
        for kind in ("account_link", "merchant_link", "security_link")
        if kind in kinds and not (kind == "account_link" and accounts_described)
    ]
    if not remaining:
        return _BATCH_TAIL
    clauses = "; ".join(_KIND_CLAUSES[kind] for kind in remaining)
    return f"Accepted links {clauses}. {_BATCH_TAIL}"


def identity_confirm_message(
    blast_radius: Mapping[str, int],
    *,
    surface: str,
    merges: Sequence[AccountMergeFacts] = (),
    kinds: Collection[str] = (),
) -> str:
    """Prompt text naming what this batch moves, and how much of it.

    The elicitation shows this string and nothing else — the binding's counts are
    a digest the human never reads — so a category missing from here is a
    mutation nobody was told about. Zero-count categories are omitted rather than
    listed as zeros: a bind that touches one security should not read as a batch
    reaching into accounts and merchants it never opens.

    ``merges`` renders one clause-by-clause block per account merge in the batch:
    which account is absorbed, which survives, how much history moves, and how
    much of it the survivor already holds. ``kinds`` names the link kinds the
    batch actually accepts, so the closing paragraph describes those and not the
    other two. Both default to empty, which reproduces the counts-only prompt for
    a caller that has neither.

    ``surface`` keys ``UNDO_COMMANDS`` — the only clause that varies by caller,
    because the reversal has to be runnable where the sentence is read.
    """
    undo_command = UNDO_COMMANDS[surface]
    moved = [
        f"{count} {IDENTITY_BLAST_RADIUS_LABELS[key][0 if count == 1 else 1]}"
        for key in IDENTITY_BLAST_RADIUS_CATEGORIES
        if (count := blast_radius.get(key, 0))
    ]
    blocks = [_account_merge_block(merge, undo_command) for merge in merges]
    blocks.append(_batch_paragraph(kinds, accounts_described=bool(merges)))
    blocks.append(f"This batch touches: {', '.join(moved) if moved else 'no rows'}.")
    return "\n\n".join(blocks)
