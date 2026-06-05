"""Layout fingerprint computation and format lookup for PDF import.

A fingerprint is a small dict capturing the structural identity of a PDF
statement: the issuer (heuristic), the unique set of table column headers,
and a coarse page-count bucket.  Two statements with the same layout — even
from different months — produce identical fingerprints, which allows the
importer to replay a saved recipe instead of re-deriving one.

Limitations
-----------
- Issuer detection is a heuristic substring scan over the first ten text
  lines.  A known issuer name embedded in a transaction description near the
  top of the document (unlikely but possible) would cause a false positive.
- Headers are de-duplicated before sorting, so a layout with two identical
  tables (e.g. two pages of transactions) fingerprints the same as a layout
  with one such table.  This is intentional: the ``page_bucket`` field
  provides coarse volume-class differentiation.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from moneybin.extractors.pdf.ir import PdfDocument


def serialize_fingerprint(fp: dict[str, Any]) -> str:
    """Return the canonical JSON encoding used to hash / store a fingerprint.

    Three independent sites need byte-for-byte identical encodings or the
    saved ``layout_fingerprint`` JSON, the ``get_by_fingerprint`` lookup key,
    and the ``issuer_slug + fp_hash`` format name silently drift apart and
    break duplicate detection. ``sort_keys=True`` is the load-bearing
    invariant — dropping it (or adding ``separators=...`` at one site and
    not the others) is the foot-gun this helper exists to prevent.
    """
    return json.dumps(fp, sort_keys=True)


if TYPE_CHECKING:
    from moneybin.repositories.pdf_formats_repo import PdfFormat, PdfFormatsRepo

# Heuristic allowlist: checked as case-insensitive substrings against the
# first ten text lines of the document.  Order matters — earlier entries win
# on ambiguous overlap (e.g. "Citi" vs. "Citibank" would need ordering).
_KNOWN_ISSUERS: list[str] = [
    "Chase",
    "Bank of America",
    "Wells Fargo",
    "Capital One",
    "American Express",
    "Citi",
    "US Bank",
    "PNC",
    "TD Bank",
    "Discover",
]


def _detect_issuer(doc: PdfDocument) -> str:
    """Return the first known-issuer name found in the top 10 text lines.

    Searches case-insensitively; returns ``"unknown"`` when no match is found.
    """
    head = doc.text_lines[:10]
    for line in head:
        lower = line.lower()
        for issuer in _KNOWN_ISSUERS:
            if issuer.lower() in lower:
                return issuer
    return "unknown"


def _unique_table_headers(doc: PdfDocument) -> list[str]:
    """Return the unique column headers of the largest table in original order.

    Scoped to the single largest table (most rows) — typically the
    transaction-detail table — so a secondary table that changes month to
    month (e.g. a rewards summary whose columns drift) doesn't flip the
    fingerprint and break replay. Multi-page layouts with identical tables
    still fingerprint the same (volume differentiation is handled by
    ``page_bucket``).

    Order matters: ``execute_recipe`` zips PDF cells positionally against
    ``recipe.fields``, so two layouts with the same column names in a
    different order are NOT interchangeable. Sorting the header list would
    collapse those layouts onto a single fingerprint and let the recipe
    parse cells against the wrong fields (e.g. "Transaction Date" and
    "Posting Date" swap, both still match a date pattern, reconciliation
    passes, but every imported transaction has the columns reversed).
    Preserve the original column order using an ordered set built from
    a dict — Python dicts preserve insertion order since 3.7.
    """
    if not doc.tables:
        return []
    largest = max(doc.tables, key=lambda t: len(t.rows))
    return list(dict.fromkeys(largest.header))


def _page_bucket(n: int) -> str:
    """Map a page count to a coarse bucket string."""
    if n <= 1:
        return "1"
    if n <= 3:
        return "2-3"
    return "4+"


def compute_fingerprint(doc: PdfDocument) -> dict[str, Any]:
    """Return the structural fingerprint of *doc*.

    The returned dict has exactly three keys — ``issuer``, ``headers``,
    ``page_bucket`` — and is JSON-serializable (list of str, not tuple).
    It is suitable for direct insertion into ``app.pdf_formats.layout_fingerprint``
    and for passing to ``PdfFormatsRepo.get_by_fingerprint``.
    """
    page_count = max(t.page for t in doc.tables) if doc.tables else 1
    return {
        "issuer": _detect_issuer(doc),
        "headers": _unique_table_headers(doc),
        "page_bucket": _page_bucket(page_count),
    }


def match_format(fp: dict[str, Any], repo: PdfFormatsRepo) -> PdfFormat | None:
    """Look up a saved format by fingerprint; return None on miss."""
    return repo.get_by_fingerprint(fp)
