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


def validate_currency_code(value: str) -> None:
    """Enforce ISO 4217 shape: exactly 3 uppercase letters."""
    if not _CURRENCY_RE.fullmatch(value):
        raise ValueError("currency_code must be exactly 3 uppercase letters")
