"""Service-layer validators for curation primitives.

The slug pattern is shared by tags (Req 14) and import labels (Req 23):
bare token or single optional namespace, lowercase ascii alnum + ``_``/``-``.

The length caps these enforce live in ``moneybin.limits``, because the request
contracts declare the same numbers as ``max_length``.
"""

from __future__ import annotations

import re

from moneybin.limits import CATEGORY_NAME_MAX_LEN, NOTE_MAX_LEN, SLUG_MAX_LEN

_SLUG_RE = re.compile(r"^[a-z0-9_-]+(:[a-z0-9_-]+)?$")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


class InvalidSlugError(ValueError):
    """Raised when a tag/label string fails the slug pattern."""


def validate_slug(value: str) -> None:
    """Enforce ^[a-z0-9_-]+(:[a-z0-9_-]+)?$ — bare or single-namespace slug."""
    if len(value) > SLUG_MAX_LEN or not _SLUG_RE.fullmatch(value):
        raise InvalidSlugError(f"invalid slug {value!r}: must match {_SLUG_RE.pattern}")


def validate_note_text(text: str) -> None:
    """Enforce non-empty note text within ``NOTE_MAX_LEN`` chars (Req 11)."""
    if not text.strip():
        raise ValueError("note text must be non-empty")
    if len(text) > NOTE_MAX_LEN:
        raise ValueError(f"note text exceeds {NOTE_MAX_LEN} chars")


def validate_category_text(value: str, field: str) -> None:
    """Enforce non-blank category text within ``CATEGORY_NAME_MAX_LEN`` chars.

    A category of spaces is not a category. The MCP write contracts already
    refuse one (``write_contracts._reject_whitespace_only``), so this is the
    same rule for the two split paths that reach the service without passing
    through a Pydantic model — ``add_split`` and the granular ``set_splits``.
    Storing the blank instead would put a third answer beside the two the
    pipeline already gives: ``core.uncategorized_queue`` selects
    ``category IS NULL``, and the staging models NULL a blank out, so a blank
    stored here counts under no category at all while claiming to have one.

    ``field`` names what is being refused, because both callers validate
    ``category`` and ``subcategory`` through this one function and the
    granular arm validates a whole list. Fixed to the word "category", the
    message sends a ``--subcategory`` caller to the flag they got right and
    never says which split failed. Callers pass the caller-facing name — the
    plain field for ``add_split``, ``splits[i].<field>`` for the batch arm —
    so the refusal matches the sibling type-check message beside it.
    """
    if not value.strip():
        raise ValueError(f"{field} must be non-empty")
    if len(value) > CATEGORY_NAME_MAX_LEN:
        raise ValueError(f"{field} exceeds {CATEGORY_NAME_MAX_LEN} chars")


def validate_category_hierarchy(
    category: str | None, subcategory: str | None, field: str
) -> None:
    """Refuse a subcategory with no category to hang off.

    A subcategory is a child of a category in this taxonomy, so the pair is
    invalid rather than partial: ``core.fct_transaction_lines`` coalesces the
    two fields independently, and a split carrying only a subcategory renders
    it beside the *parent transaction's* category — a combination nobody
    chose. ``write_contracts.SplitTarget`` has refused this on the MCP path all
    along; this is the same rule for the two split paths that reach the service
    without a Pydantic model, so the three surfaces stop disagreeing.

    ``field`` names the subcategory as the caller spells it, matching
    ``validate_category_text``.
    """
    if subcategory is not None and category is None:
        raise ValueError(f"{field} requires a category")


def validate_currency_code(value: str) -> None:
    """Enforce ISO 4217 shape: exactly 3 uppercase letters."""
    if not _CURRENCY_RE.fullmatch(value):
        raise ValueError("currency_code must be exactly 3 uppercase letters")
