"""The sentence a human ratifies before an identity batch commits.

Sibling of ``import_confirmation``: the prose a confirm gate shows lives beside
the domain it describes rather than inside one surface, so the MCP elicitation
and the CLI prompt cannot drift into describing the same merge two different
ways. The MCP tool wrapper is where it used to live, which put the CLI a copy
away from a sentence it needs to render identically.
"""

from __future__ import annotations

from collections.abc import Mapping

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


def identity_confirm_message(blast_radius: Mapping[str, int]) -> str:
    """Prompt text naming what this batch moves, and how much of it.

    The elicitation shows this string and nothing else — the binding's counts are
    a digest the human never reads — so a category missing from here is a
    mutation nobody was told about. Zero-count categories are omitted rather than
    listed as zeros: a bind that touches one security should not read as a batch
    reaching into accounts and merchants it never opens.
    """
    moved = [
        f"{count} {IDENTITY_BLAST_RADIUS_LABELS[key][0 if count == 1 else 1]}"
        for key in IDENTITY_BLAST_RADIUS_CATEGORIES
        if (count := blast_radius.get(key, 0))
    ]
    return (
        "Confirm this complete identity-decision batch. Accepted links can merge "
        "account histories, merchant attribution, or security lots — including "
        "any price marks you set by hand, which move onto the survivor — or bind "
        "a price-feed symbol without merging anything; every decision in the "
        "ordered batch will commit together.\n\n"
        f"This batch touches: {', '.join(moved) if moved else 'no rows'}."
    )
