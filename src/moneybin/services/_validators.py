"""Service-layer validators for curation primitives.

The slug pattern is shared by tags (Req 14) and import labels (Req 23):
bare token or single optional namespace, lowercase ascii alnum + ``_``/``-``.
"""

from __future__ import annotations

import re

NOTE_MAX_LEN = 2000

_SLUG_RE = re.compile(r"^[a-z0-9_-]+(:[a-z0-9_-]+)?$")
_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


class InvalidSlugError(ValueError):
    """Raised when a tag/label string fails the slug pattern."""


def validate_slug(value: str) -> None:
    """Enforce ^[a-z0-9_-]+(:[a-z0-9_-]+)?$ — bare or single-namespace slug."""
    if not _SLUG_RE.fullmatch(value):
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
