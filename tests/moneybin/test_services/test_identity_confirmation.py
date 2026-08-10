"""Tests for the sentence a human ratifies before an identity batch commits."""

from __future__ import annotations

from moneybin.services.identity_confirmation import (
    IDENTITY_BLAST_RADIUS_CATEGORIES,
    IDENTITY_BLAST_RADIUS_LABELS,
    identity_confirm_message,
)


def test_every_blast_radius_category_has_a_prompt_label() -> None:
    """Set equality, because a missing label is silent in the direction that matters.

    ``identity_confirm_message`` reports only the categories it can name. A
    category added to ``IDENTITY_BLAST_RADIUS_CATEGORIES`` without a label here
    would be counted in the digest the approval binds to and then omitted from
    the sentence the human reads — which is the same failure as counting five
    categories while moving six, one layer further out.
    """
    assert set(IDENTITY_BLAST_RADIUS_LABELS) == set(IDENTITY_BLAST_RADIUS_CATEGORIES)


def test_the_identity_prompt_counts_every_category_it_moves() -> None:
    """The registered description promises the prompt counts what it moves.

    Until now it did not: the counts lived only in the confirmation digest, which
    the human never sees, while the elicited text was fixed prose naming
    "security lots" and nothing else. A merge could move a user's hand-set
    valuations onto another instrument with no sentence anywhere saying so.
    """
    message = identity_confirm_message({
        "accounts": 0,
        "merchants": 0,
        "securities": 2,
        "transactions": 7,
        "lots": 3,
        "price_marks": 4,
    })

    assert "4 price marks you set by hand" in message
    assert "7 transactions" in message
    assert "3 tax lots" in message
    assert "2 securities" in message


def test_the_identity_prompt_omits_categories_it_does_not_touch() -> None:
    """A radius padded with zeros reads as a bigger blast than it is.

    Paired with the test above: listing every category unconditionally satisfies
    that one, and would tell a user binding a price feed that the batch touches
    accounts and merchants it never opens.
    """
    message = identity_confirm_message({
        "accounts": 0,
        "merchants": 0,
        "securities": 1,
        "transactions": 0,
        "lots": 0,
        "price_marks": 0,
    })

    assert "1 security" in message
    assert "account" not in message.split("This batch touches:")[-1]
    assert "tax lot" not in message.split("This batch touches:")[-1]
