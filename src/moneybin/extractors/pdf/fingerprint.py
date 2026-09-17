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

import hashlib
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from moneybin.extractors.pdf.ir import PdfDocument
from moneybin.utils import slugify


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
    """Return the unique column headers of the transaction table in original order.

    Scoped to the **transaction table** — NOT just "the largest table" — so the
    fingerprint and the recipe always agree on which table to characterise.
    Picking by row count alone breaks recipe reuse on multi-table PDFs: an Amex
    statement with a larger rewards-summary grid, or an investment statement with
    a large positions table, would fingerprint the wrong table and either (a) flip
    the fingerprint as the incidental large table changes month-to-month, so
    replay never fires, or (b) keep the same fingerprint while the real
    transaction layout actually changes, so replay returns the wrong recipe.

    ``transaction_headers`` finds that table however it is visible — ruled,
    reconstructed from text, or (for an unruled debit/credit layout) only as raw
    text lines. Consulting it BEFORE giving up on an empty ``doc.tables`` is what
    keeps unruled statements — that is, every real one, since none draw ruling
    lines — off a degenerate ``headers: []`` fingerprint that two different
    institutions would collide on and replay each other's recipe against.

    Falls back to "largest table" only when the document is not a statement at
    all — routing seeds it in that case anyway, so fingerprint stability matters
    much less.

    Order matters: ``execute_recipe`` zips PDF cells positionally against
    ``recipe.fields``, so two layouts with the same column names in a
    different order are NOT interchangeable. Sorting the header list would
    collapse those layouts onto a single fingerprint and let the recipe
    parse cells against the wrong fields. Preserve original column order
    using an ordered set built from a dict — Python dicts preserve
    insertion order since 3.7.
    """
    # Deferred import to break a potential cycle and keep fingerprint leaf-leaf
    # even as auto_derive evolves.
    from moneybin.extractors.pdf.auto_derive import transaction_headers

    headers = transaction_headers(doc)
    if headers is not None:
        return list(dict.fromkeys(headers))
    if not doc.tables:
        return []
    return list(dict.fromkeys(max(doc.tables, key=lambda t: len(t.rows)).header))


# The complete, closed set of page-count buckets ``_page_bucket`` can emit, in
# ascending order. Public because consumers that validate a stored fingerprint
# against what ``compute_fingerprint`` could produce (e.g. the doctor
# ``app_pdf_formats_fingerprint_shape`` invariant) must check ``page_bucket``
# membership against this exact vocabulary — keep it the single source of truth.
PAGE_BUCKETS: tuple[str, str, str] = ("1", "2-3", "4+")


def _page_bucket(n: int) -> str:
    """Map a page count to a coarse bucket string from ``PAGE_BUCKETS``."""
    if n <= 1:
        return PAGE_BUCKETS[0]
    if n <= 3:
        return PAGE_BUCKETS[1]
    return PAGE_BUCKETS[2]


def compute_fingerprint(doc: PdfDocument) -> dict[str, Any]:
    """Return the structural fingerprint of *doc*.

    The returned dict has exactly three keys — ``issuer``, ``headers``,
    ``page_bucket`` — and is JSON-serializable (list of str, not tuple).
    It is suitable for direct insertion into ``app.pdf_formats.layout_fingerprint``
    and for passing to ``PdfFormatsRepo.get_by_fingerprint``.
    """
    # Both signals are lower bounds on the real page count, so take the larger.
    # max(table.page) alone reports 1 page for every unruled statement (they have
    # no tables at all), pinning page_bucket to "1"; doc.page_count alone would
    # trust a hand-built IR that never set it.
    highest_table_page = max((t.page for t in doc.tables), default=1)
    page_count = max(doc.page_count, highest_table_page)
    return {
        "issuer": _detect_issuer(doc),
        "headers": _unique_table_headers(doc),
        "page_bucket": _page_bucket(page_count),
    }


def match_format(fp: dict[str, Any], repo: PdfFormatsRepo) -> PdfFormat | None:
    """Look up a saved format by fingerprint; return None on miss."""
    return repo.get_by_fingerprint(fp)


def pdf_alias(file_path: Path) -> str:
    """Resolve the seed alias from the file stem.

    Returns a slug used in ``raw.pdf_<alias>`` view names. The ``pdf_``
    prefix is added by the view-name construction, so the alias itself can
    start with any character (including digits) — the view regex sees
    ``pdf_{alias}``, not just ``{alias}``.

    Capped at 59 chars so the ``pdf_{alias}`` view name fits the shared
    builder's 63-char limit. When truncation would silently merge distinct
    long filenames (two PDFs whose slugified stems share the first 59
    chars), a 4-char content-hash suffix preserves uniqueness within the
    same ceiling.
    """
    slug = slugify(file_path.stem).replace("-", "_")
    if not slug:
        slug = "import"
    if len(slug) > 59:
        suffix = hashlib.sha256(slug.encode()).hexdigest()[:4]
        slug = f"{slug[:54]}_{suffix}"
    return slug


def pdf_format_name(fp: dict[str, Any]) -> str:
    """Deterministic first-contact format name: issuer slug + fingerprint hash.

    Single source of truth for the ``app.pdf_formats.name`` of an auto-derived
    or bridge-authored recipe on first contact. Both
    ``ImportService._import_pdf_transactions`` (deterministic) and
    ``ImportService.apply_pdf_bridge_response`` (bridge) derive the name this
    way — the hash is built from ``serialize_fingerprint(fp)`` so it stays
    byte-for-byte identical to the JSON the repo stores and looks up by; any
    drift between call sites would silently break duplicate detection.
    """
    issuer_slug = slugify(fp.get("issuer", "unknown"))
    digest = hashlib.sha256(serialize_fingerprint(fp).encode()).hexdigest()[:12]
    return f"{issuer_slug}_{digest}"
