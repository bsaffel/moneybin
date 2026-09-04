"""Service-layer validators for curation primitives.

The slug pattern is shared by tags (Req 14) and import labels (Req 23):
bare token or single optional namespace, lowercase ascii alnum + ``_``/``-``.
"""

from __future__ import annotations

import re

IDENTIFIER_MAX_LEN = 64
CATEGORY_NAME_MAX_LEN = 100
MERCHANT_NAME_MAX_LEN = 200
MERCHANT_PATTERN_MAX_LEN = 500
DESCRIPTION_MAX_LEN = 2000
#: Also bounds a report's stored reclassify ``reason`` — an audit annotation.
NOTE_MAX_LEN = 2000
SLUG_MAX_LEN = 100
#: A saved report's stored SELECT. Generous next to the others because a real
#: analytical query with CTEs legitimately runs to a few thousand characters —
#: but bounded, because DuckDB's VARCHAR is not, and every catalog read,
#: `reports explain`, and export receipt renders this text again.
REPORT_QUERY_MAX_LEN = 20_000
#: A saved report's serialized `params` declaration block, not one field of it:
#: a declared default, a help string, and the number of parameters all land in
#: the same JSON column, and the total is what the row stores, the catalog
#: republishes, and every later mutation copies into its audit images. One
#: parameter serializes to roughly 60-100 characters, so this admits dozens with
#: generous defaults while keeping all three bounded.
REPORT_PARAMS_MAX_LEN = 4_000
#: A saved report's serialized `class_downgrades` block, not one entry of it.
#: `reason` is already bounded per entry by `NOTE_MAX_LEN`, but the map grows one
#: entry per downgraded column and the whole of it is copied into the before/after
#: images every later mutation audits. An entry serializes to roughly 60
#: characters plus its reason, so this admits four maximum-length reasons or
#: around sixty ordinary one-sentence ones — well past any report that has had a
#: human confirm a downgrade per column.
REPORT_DOWNGRADES_MAX_LEN = 8_000

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


def validate_category_text(value: str, field: str = "category") -> None:
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


def validate_currency_code(value: str) -> None:
    """Enforce ISO 4217 shape: exactly 3 uppercase letters."""
    if not _CURRENCY_RE.fullmatch(value):
        raise ValueError("currency_code must be exactly 3 uppercase letters")
