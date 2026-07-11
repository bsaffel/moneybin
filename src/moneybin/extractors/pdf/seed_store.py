"""Write a PdfDocument to raw.pdf_seeds as hashed JSON rows + regenerate view."""

from __future__ import annotations

import hashlib
import json
import logging
import re

import polars as pl

from moneybin.database import Database
from moneybin.extractors.pdf.ir import PdfDocument
from moneybin.sql.seed_view import generate_seed_view_sql
from moneybin.tables import PDF_SEEDS

logger = logging.getLogger(__name__)

# re.ASCII keeps \d ASCII-only so Arabic-Indic / Devanagari / other
# Unicode digit codepoints (which DuckDB cannot CAST to BIGINT/DECIMAL)
# don't get mis-inferred as numeric types and break view queries.
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$", re.ASCII)
# Used with .fullmatch() — match/search would accept partial-numeric
# strings like "1e5abc" by matching only the "1e5" prefix and break the
# CAST to DECIMAL/BIGINT downstream. re.ASCII protects against Unicode
# digits too (see _DATE_RE comment).
_PLAIN_NUMERIC_RE = re.compile(r"[+-]?(\d+\.\d+|\d+)", re.ASCII)


def _document_key(doc: PdfDocument) -> str:
    """Stable identity for a document's extracted content.

    An alias is only the filename stem, so `2024-01/chase.pdf` and
    `2024-02/chase.pdf` collapse to the same alias — and ``doc.source_file``
    is just the basename, so it can't tell them apart either. Without a
    document component in the row hash, a recurring charge that lands at the
    same row index in two months (an identical NETFLIX line) hashes
    identically across the two statements and the second is dropped by
    on_conflict='ignore'.

    Keyed on extracted *content*, not the file path, so re-importing the same
    statement from a different directory still deduplicates: identical content
    is the same statement.

    Hashes only ``doc.tables`` — the rows seeds are actually built from
    (``iter_rows`` reads nothing else). Folding in ``doc.text_lines`` would let a
    pdfplumber upgrade, or any whitespace change in how text is emitted, re-key
    every row of a document whose extracted cells did not change by one
    character — and ``on_conflict='ignore'`` would then insert the whole
    statement a second time instead of recognising it.
    """
    payload = json.dumps(
        [{"page": t.page, "header": t.header, "rows": t.rows} for t in doc.tables],
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def write_pdf_seed(
    db: Database, doc: PdfDocument, *, alias: str, import_id: str
) -> tuple[int, int]:
    """Insert the document's rows as a seed and regenerate raw.pdf_<alias>.

    Identity is SHA-256(alias|<doc_key>|p<page>r<row_idx>|json(row))[:16] with
    a pdf_ prefix. Two components keep distinct rows distinct:

    - The **page + row-index** preserves legitimate duplicate-cell rows *within*
      one statement (two same-day same-amount purchases).
    - The **document key** (``_document_key``) keeps two different statements
      that share an alias from colliding on a row identical in both content and
      position.

    Only the same content, at the same position, in the same document is a
    duplicate — which is exactly what makes re-importing one statement
    idempotent. Re-importing existing content is a no-op via
    on_conflict='ignore' — existing rows keep their original import_id so
    reverting a later import doesn't remove rows the first import's log claims
    as complete.

    Returns ``(extracted, inserted)`` where ``extracted`` is the number of
    rows produced by extraction (drives the zero-row gate so re-imports of
    valid content don't raise "no tables extracted") and ``inserted`` is the
    number of new rows actually persisted (≤ extracted; the difference is
    rows already present from a prior import). Audit log + metrics should
    report ``inserted`` so re-imports don't inflate counts.
    """
    rows: list[dict[str, object]] = []
    doc_key = _document_key(doc)
    # Per-page row index makes the hash position-aware: two rows with
    # identical cells but different positions (legitimate duplicate
    # transactions) get distinct hashes; the same row at the same position
    # across re-imports gets the same hash (idempotency).
    page_row_idx: dict[int, int] = {}
    for page, cells in doc.iter_rows():
        idx = page_row_idx.get(page, 0)
        page_row_idx[page] = idx + 1
        data_json = json.dumps(cells, sort_keys=True)
        # 16 hex chars = 64 bits; see identifiers.md for scale threshold.
        digest = hashlib.sha256(
            f"{alias}|{doc_key}|p{page}r{idx}|{data_json}".encode()
        ).hexdigest()[:16]
        row_hash = f"pdf_{digest}"
        rows.append({
            "alias": alias,
            "row_hash": row_hash,
            "data": data_json,
            "source_file": doc.source_file,
            "page": page,
            "import_id": import_id,
        })

    inserted = 0
    if rows:
        df = pl.DataFrame(rows)
        db.ingest_dataframe(PDF_SEEDS.full_name, df, on_conflict="ignore")
        # on_conflict='ignore' keeps existing rows under their original
        # import_id, so counting rows tagged with THIS import_id gives the
        # number actually inserted (excluding ignored duplicates).
        inserted_row = db.execute(
            f"SELECT COUNT(*) FROM {PDF_SEEDS.full_name} WHERE import_id = ?",  # noqa: S608 — compile-time TableRef constant, value parameterized
            [import_id],
        ).fetchone()
        inserted = int(inserted_row[0]) if inserted_row else 0

        # Sample ALL rows for this alias (incl. pre-existing) so the view's
        # column types accommodate every row, not just this import's. Same-alias
        # re-imports with different content could otherwise break old rows on
        # CAST at query time.
        existing = db.execute(
            f"SELECT data FROM {PDF_SEEDS.full_name} WHERE alias = ?",  # noqa: S608 — compile-time TableRef constant, value parameterized
            [alias],
        ).fetchall()
        all_rows_for_inference: list[dict[str, object]] = [
            {"data": str(row[0])} for row in existing
        ]

        # Re-collect union_keys across all rows (some keys may exist in
        # pre-existing rows but not the current import).
        full_union_keys: dict[str, None] = {}
        for r in all_rows_for_inference:
            cells = json.loads(str(r["data"]))
            for k in cells:
                full_union_keys.setdefault(k, None)

        # View creation is inside `if rows:` — avoids degenerate carry-only views
        # for zero-row imports (e.g. image-only PDFs that raised before reaching here).
        typed_columns = _infer_typed_columns(
            list(full_union_keys), all_rows_for_inference
        )
        view_sql = generate_seed_view_sql(
            source_table=PDF_SEEDS.full_name,
            view_name=f"pdf_{alias}",
            filter_column="alias",
            filter_value=alias,
            typed_columns=typed_columns,
            carry_columns=["page", "loaded_at"],
        )
        db.execute(view_sql)

    logger.info(
        f"pdf seed: alias={alias} import_id={import_id} "
        f"extracted={len(rows)} inserted={inserted}"
    )
    return len(rows), inserted


def _infer_typed_columns(
    keys: list[str], rows: list[dict[str, object]]
) -> dict[str, str]:
    """Infer a DuckDB type per JSON key from the written rows (numeric/date/varchar)."""
    typed: dict[str, str] = {}
    # Phase 1 scale (dozens of rows × dozens of columns): re-parsing each row's
    # JSON inside the key loop is O(rows × keys). Precompute parsed dicts if
    # the p95 import latency ever regresses.
    for key in keys:
        samples: list[str] = []
        for r in rows:
            cells = json.loads(str(r["data"]))
            v = cells.get(key)
            if v is None or str(v).strip() == "":
                continue
            samples.append(str(v).strip())
        typed[key] = _infer_type(samples)
    return typed


def _infer_type(samples: list[str]) -> str:
    if not samples:
        return "VARCHAR"
    if all(_PLAIN_NUMERIC_RE.fullmatch(v) for v in samples):
        if any("." in v for v in samples):
            return "DECIMAL(18,2)"
        # DuckDB BIGINT max = 9_223_372_036_854_775_807 (19 digits incl. sign).
        # >18 digits risks CAST overflow at query time — fall back to VARCHAR.
        if any(len(v.lstrip("+-")) > 18 for v in samples):
            return "VARCHAR"
        return "BIGINT"
    if all(_DATE_RE.fullmatch(v) for v in samples):
        return "DATE"
    return "VARCHAR"
