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
) -> int:
    """Insert the document's rows as a seed and regenerate raw.pdf_<alias>.

    Identity is SHA-256(alias|json(row))[:16] with a pdf_ prefix. Re-importing
    the same content is a no-op via on_conflict='ignore' — existing rows keep
    their original import_id so reverting a later import that contained the
    same content doesn't remove rows the first import's log claims as complete.
    Returns the count of rows extracted from the document (used as the
    "newly written + already-existing" count for metrics + the zero-row gate;
    NOT the count of new rows actually persisted, which may be lower due to
    dedup against existing rows).
    """
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    for page, cells in doc.iter_rows():
        data_json = json.dumps(cells, sort_keys=True)
        # 16 hex chars = 64 bits; see identifiers.md for scale threshold.
        digest = hashlib.sha256(f"{alias}|{data_json}".encode()).hexdigest()[:16]
        row_hash = f"pdf_{digest}"
        if row_hash in seen:
            continue  # exact-duplicate row within one doc — keep first
        seen.add(row_hash)
        rows.append({
            "alias": alias,
            "row_hash": row_hash,
            "data": data_json,
            "source_file": doc.source_file,
            "page": page,
            "import_id": import_id,
        })

    if rows:
        df = pl.DataFrame(rows)
        db.ingest_dataframe(PDF_SEEDS.full_name, df, on_conflict="ignore")

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

    logger.info(f"pdf seed: alias={alias} import_id={import_id} rows={len(rows)}")
    return len(rows)


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
