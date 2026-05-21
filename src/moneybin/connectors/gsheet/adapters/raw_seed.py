"""RawSeedAdapter — catch-all escape hatch for arbitrary tabular gsheets.

Writes JSON-encoded rows to `raw.gsheet_seeds` with stable content-hash keys,
soft-deletes rows that disappear from the sheet, and regenerates a
per-connection typed view in `raw.gsheet_<alias>` so downstream consumers can
query the seed data with normal SQL.

Type inference for the view runs at detect-time; the inferred map is persisted
in `connection.column_mapping` (the seed adapter re-uses that field for typed
columns) and re-read on every load to rebuild the view.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re

import polars as pl

from moneybin.connectors.gsheet.adapters import ADAPTERS
from moneybin.connectors.gsheet.adapters.base import (
    DetectionResult,
    GSheetConnection,
    LoadResult,
)
from moneybin.connectors.gsheet.diff import compute_diff
from moneybin.connectors.gsheet.drift import DriftReport
from moneybin.connectors.gsheet.errors import GSheetError
from moneybin.connectors.gsheet.view_generator import generate_seed_view_sql
from moneybin.database import Database
from moneybin.tables import GSHEET_SEEDS

logger = logging.getLogger(__name__)

# Full-match anchored to both ends — otherwise an ISO datetime like
# "2024-01-15T12:00:00" would prefix-match and get typed as DATE, deferring the
# failure to view-query time instead of surfacing it at detection.
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class RawSeedAdapter:
    """Catch-all adapter: JSON rows + auto-generated typed view per connection."""

    name: str = "seed"

    def detect(
        self,
        df: pl.DataFrame,
        *,
        account_name: str | None,
    ) -> DetectionResult:
        """Infer DuckDB types per column from sample data."""
        _ = account_name  # accepted for Protocol parity; unused by seed adapter
        typed_columns: dict[str, str] = {
            col: _infer_type(df[col]) for col in df.columns
        }
        return DetectionResult(
            confidence="medium",  # seed is never "high" — that's the transactions adapter
            column_mapping={},  # unused for seed; typed_columns is the analogue
            header_signature=list(df.columns),
            typed_columns=typed_columns,
            notes=["Seed adapter: rows queryable via raw.gsheet_<alias> view."],
        )

    def check_drift(
        self,
        connection: GSheetConnection,
        current_df: pl.DataFrame,
    ) -> DriftReport:
        """Permissive drift — only structural unreadability triggers refusal.

        Added or renamed columns are NOT drift; the view is regenerated on
        every pull, absorbing schema changes naturally.
        """
        _ = connection  # unused; permissive drift doesn't compare against pinned state
        if current_df.shape[1] == 0:
            return DriftReport(
                is_drift=True,
                reason="Sheet has no headers",
            )
        return DriftReport(is_drift=False, reason="no drift")

    def transform(
        self,
        df: pl.DataFrame,
        connection: GSheetConnection,
    ) -> pl.DataFrame:
        """Build the `raw.gsheet_seeds` insert frame from a current pull.

        Each row gets a **content-only** ``row_hash`` derived from
        ``SHA-256(connection_id|json(row))[:16]``. The hash is position-
        independent — moving a row up or down in the sheet leaves its hash
        unchanged, so attached metadata (notes, tags, category assignments
        keyed on this row's transaction_id downstream) survives reorderings.

        Two rows with byte-identical content collide on the
        ``(connection_id, row_hash)`` primary key. Rather than silently
        keeping only one, we surface the duplicate explicitly so the user
        can disambiguate (typically by adding an ID/Note column in the
        sheet). Returns an empty frame with the correct schema when the
        input is empty.
        """
        records = df.to_dicts()
        rows: list[dict[str, object]] = []
        seen_hashes: dict[str, int] = {}
        duplicates: list[tuple[int, int]] = []
        for idx, rec in enumerate(records, start=1):
            data_json = json.dumps(rec, sort_keys=True, default=str)
            row_hash = hashlib.sha256(
                f"{connection.connection_id}|{data_json}".encode()
            ).hexdigest()[:16]
            if row_hash in seen_hashes:
                duplicates.append((seen_hashes[row_hash], idx))
                continue
            seen_hashes[row_hash] = idx
            rows.append({
                "connection_id": connection.connection_id,
                "spreadsheet_id": connection.spreadsheet_id,
                "sheet_gid": connection.sheet_gid,
                "row_number": idx,
                "row_hash": row_hash,
                "data": data_json,
            })

        if duplicates:
            # Surface the first collision pair so the user can find it. The
            # PRIMARY KEY (connection_id, row_hash) would otherwise raise
            # an opaque constraint error inside ingest_dataframe. Row content
            # is intentionally NOT included in the message — financial data
            # in error messages would violate security.md "No PII in errors".
            first, second = duplicates[0]
            raise GSheetError(
                f"Sheet has {len(duplicates)} duplicate row(s) by content. "
                f"Rows {first} and {second} have identical content. "
                "Add a disambiguating column (e.g. ID, Note) in the sheet, "
                "or remove the duplicates, before connecting."
            )

        if rows:
            return pl.DataFrame(rows)
        return pl.DataFrame(
            schema={
                "connection_id": pl.String,
                "spreadsheet_id": pl.String,
                "sheet_gid": pl.Int64,
                "row_number": pl.Int64,
                "row_hash": pl.String,
                "data": pl.String,
            }
        )

    def load(
        self,
        df: pl.DataFrame,
        connection: GSheetConnection,
        db: Database,
        import_id: str,
    ) -> LoadResult:
        """Diff, soft-delete missing, upsert present, undelete returning, regenerate view.

        The view is regenerated on EVERY load so a typed_columns change
        (e.g. user re-detected the sheet) propagates without extra plumbing.
        """
        if connection.alias is None:
            raise ValueError(
                f"RawSeedAdapter.load requires connection.alias (seed connection "
                f"{connection.connection_id} has none)"
            )

        # Active row hashes for this connection (soft-deletes excluded).
        active_rows = db.execute(
            f"SELECT row_hash FROM {GSHEET_SEEDS.full_name} "  # noqa: S608  # TableRef constant, no user input
            "WHERE connection_id = ? AND deleted_from_source_at IS NULL",
            [connection.connection_id],
        ).fetchall()
        active_hashes: set[str] = {r[0] for r in active_rows}

        current_hashes: set[str] = (
            set(df["row_hash"].to_list()) if df.height > 0 else set()
        )

        diff = compute_diff(current_ids=current_hashes, active_ids=active_hashes)

        # Soft-delete rows that were active but are no longer present.
        rows_soft_deleted = 0
        if diff.to_soft_delete:
            hashes = sorted(diff.to_soft_delete)
            placeholders = ",".join(["?"] * len(hashes))
            sql = (
                f"UPDATE {GSHEET_SEEDS.full_name} SET deleted_from_source_at = CURRENT_TIMESTAMP "  # noqa: S608  # TableRef + placeholders, no user input
                f"WHERE connection_id = ? AND row_hash IN ({placeholders})"
            )
            db.execute(sql, [connection.connection_id, *hashes])
            rows_soft_deleted = len(hashes)

        # Upsert current rows. Defensive NULL on deleted_from_source_at so that
        # INSERT OR REPLACE BY NAME clears any prior soft-delete state for
        # returning rows.
        if df.height > 0:
            df = df.with_columns(
                pl.lit(import_id).alias("import_id"),
                pl.lit(None, dtype=pl.Datetime("us")).alias("deleted_from_source_at"),
            )
            db.ingest_dataframe(GSHEET_SEEDS.full_name, df, on_conflict="upsert")

            # Belt-and-suspenders undelete: ensure any returning row is active
            # even if a future upsert path forgets to project the column.
            hashes = sorted(current_hashes)
            placeholders = ",".join(["?"] * len(hashes))
            sql = (
                f"UPDATE {GSHEET_SEEDS.full_name} SET deleted_from_source_at = NULL "  # noqa: S608  # TableRef + placeholders, no user input
                f"WHERE connection_id = ? AND row_hash IN ({placeholders})"
            )
            db.execute(sql, [connection.connection_id, *hashes])

        # Regenerate the per-connection typed view. Typed columns live in
        # connection.column_mapping for the seed adapter (the field is reused
        # because seed adapters have no source-header → dest-field mapping).
        view_sql = generate_seed_view_sql(
            alias=connection.alias,
            connection_id=connection.connection_id,
            typed_columns=connection.column_mapping,
        )
        db.execute(view_sql)

        # rows_upserted counts ONLY rows that already existed and were
        # updated this pull. Newly inserted rows live in diff.to_insert,
        # not in rows_upserted — otherwise CLI / import_log inflates by N
        # on first pull (matches the transactions adapter convention).
        rows_upserted = len(current_hashes) - len(diff.to_insert)
        logger.info(
            f"gsheet seed load: connection={connection.connection_id} "
            f"import_id={import_id} inserted={len(diff.to_insert)} "
            f"upserted={rows_upserted} soft_deleted={rows_soft_deleted}"
        )

        return LoadResult(
            rows_inserted=len(diff.to_insert),
            rows_soft_deleted=rows_soft_deleted,
            rows_upserted=rows_upserted,
        )


def _infer_type(col: pl.Series) -> str:
    """Heuristically infer a DuckDB type from a sample column.

    Strategy: collect non-null / non-empty / non-whitespace values, then try
    numeric → date → fallback VARCHAR. Numeric with any decimal point → DECIMAL;
    otherwise BIGINT.
    """
    sample: list[str] = []
    for v in col.to_list():
        if v is None:
            continue
        s = str(v).strip()
        if s == "":
            continue
        sample.append(s)

    if not sample:
        return "VARCHAR"

    try:
        for v in sample:
            float(v)
    except (ValueError, TypeError):
        pass
    else:
        if any("." in v for v in sample):
            return "DECIMAL(18,2)"
        return "BIGINT"

    if all(_DATE_RE.match(v) for v in sample):
        return "DATE"

    return "VARCHAR"


# Register the adapter exactly once at import time.
ADAPTERS.setdefault("seed", RawSeedAdapter())
