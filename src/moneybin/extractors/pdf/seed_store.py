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

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# Used with .fullmatch() — match/search would accept "inf"/"nan"/"1e5"
# and break the CAST to DECIMAL/BIGINT downstream.
_PLAIN_NUMERIC_RE = re.compile(r"[+-]?(\d+\.\d+|\d+)")


def write_pdf_seed(
    db: Database, doc: PdfDocument, *, alias: str, import_id: str
) -> tuple[int, int]:
    """Insert the document's rows as a seed and regenerate raw.pdf_<alias>.

    Identity is SHA-256(alias|p<page>r<row_idx>|json(row))[:16] with a pdf_
    prefix. The page+row-index component preserves legitimate duplicate-cell
    rows (e.g. two same-day same-amount purchases) — only the *same content
    at the same physical position* is treated as a duplicate, which keeps
    re-imports of the same statement idempotent. Re-importing existing
    content is a no-op via on_conflict='ignore' — existing rows keep their
    original import_id so reverting a later import doesn't remove rows the
    first import's log claims as complete.

    Returns ``(extracted, inserted)`` where ``extracted`` is the number of
    rows produced by extraction (drives the zero-row gate so re-imports of
    valid content don't raise "no tables extracted") and ``inserted`` is the
    number of new rows actually persisted (≤ extracted; the difference is
    rows already present from a prior import). Audit log + metrics should
    report ``inserted`` so re-imports don't inflate counts.
    """
    rows: list[dict[str, object]] = []
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
            f"{alias}|p{page}r{idx}|{data_json}".encode()
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
