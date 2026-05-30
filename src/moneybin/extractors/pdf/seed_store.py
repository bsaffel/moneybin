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

    Identity is SHA-256(alias|json(row))[:16] with a pdf_ prefix, so re-importing
    the same file is a no-op (PRIMARY KEY (alias, row_hash) conflict → upsert).
    Returns the number of rows written (pre-dedup row count).
    """
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    union_keys: dict[str, None] = {}  # ordered set of all header keys seen
    for page, cells in doc.iter_rows():
        data_json = json.dumps(cells, sort_keys=True)
        # 16 hex chars = 64 bits; see identifiers.md for scale threshold.
        digest = hashlib.sha256(f"{alias}|{data_json}".encode()).hexdigest()[:16]
        row_hash = f"pdf_{digest}"
        if row_hash in seen:
            continue  # exact-duplicate row within one doc — keep first
        seen.add(row_hash)
        for k in cells:
            union_keys.setdefault(k, None)
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
        db.ingest_dataframe(PDF_SEEDS.full_name, df, on_conflict="upsert")

        # View creation is inside `if rows:` — avoids degenerate carry-only views
        # for zero-row imports (e.g. image-only PDFs that raised before reaching here).
        typed_columns = _infer_typed_columns(list(union_keys), rows)
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
