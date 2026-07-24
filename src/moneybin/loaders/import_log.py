"""Generic import-batch lifecycle for raw.import_log.

Both tabular and OFX import paths call these functions to create batches
(``begin_import``), finalize them with row counts (``finalize_import``),
query history (``get_import_history``), and check for prior imports of a
source file (``find_existing_import``).

The module is also the single source of truth for which raw tables a given
source_type populates — see ``REVERT_TABLES`` below. ``ImportService.revert``
(see ``moneybin/services/import_service.py``) consults this allowlist; the
revert operation itself lives on the service, not here.
"""

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Literal

from moneybin.database import Database
from moneybin.tables import (
    IMPORT_LOG,
    MANUAL_INVESTMENT_TRANSACTIONS,
    MANUAL_TRANSACTIONS,
    OFX_ACCOUNTS,
    OFX_BALANCES,
    OFX_INSTITUTIONS,
    OFX_TRANSACTIONS,
    PDF_SEEDS,
    TABULAR_ACCOUNTS,
    TABULAR_TRANSACTIONS,
    TableRef,
)

logger = logging.getLogger(__name__)


_SourceType = Literal[
    "csv", "tsv", "excel", "parquet", "feather", "pipe", "ofx", "manual", "pdf"
]

_IMPORT_HISTORY_COLUMNS = [
    "import_id",
    "source_file",
    "source_type",
    "source_origin",
    "format_name",
    "status",
    "rows_imported",
    "rows_rejected",
    "detection_confidence",
    "started_at",
    "completed_at",
]


# Allowlist mapping source_type → raw tables that carry rows for that type.
# ImportService.revert() consults this to know what to delete. Adding a new
# format means adding an entry here AND ensuring those tables have an
# import_id column.
_TABULAR_RAW_TABLES = [TABULAR_TRANSACTIONS, TABULAR_ACCOUNTS]
REVERT_TABLES: dict[str, list[TableRef]] = {
    "csv": _TABULAR_RAW_TABLES,
    "tsv": _TABULAR_RAW_TABLES,
    "excel": _TABULAR_RAW_TABLES,
    "parquet": _TABULAR_RAW_TABLES,
    "feather": _TABULAR_RAW_TABLES,
    "pipe": _TABULAR_RAW_TABLES,
    "ofx": [OFX_TRANSACTIONS, OFX_ACCOUNTS, OFX_BALANCES, OFX_INSTITUTIONS],
    # Manual cash entries and manual investment events share source_type
    # "manual" but write to different raw tables; revert is keyed on import_id
    # and each batch's rows live in exactly one table, so listing both is safe
    # (a cash-only batch deletes 0 investment rows and vice versa).
    "manual": [MANUAL_TRANSACTIONS, MANUAL_INVESTMENT_TRANSACTIONS],
    # Phase 2a: PDF imports can land in either pdf_seeds (Phase 1 fallback) or
    # tabular_transactions + tabular_accounts (deterministic path). All three
    # are listed so revert clears whichever tables the import wrote to. The
    # revert logic filters by import_id, so non-PDF rows in the tabular tables
    # are never touched.
    "pdf": [PDF_SEEDS, TABULAR_TRANSACTIONS, TABULAR_ACCOUNTS],
}


def begin_import(
    db: Database,
    *,
    source_file: str,
    source_type: _SourceType,
    source_origin: str,
    account_names: list[str],
    format_name: str | None = None,
    format_source: str | None = None,
) -> str:
    """Create an import_log row in 'importing' state. Returns the new import_id (UUID).

    Args:
        db: Database connection.
        source_file: Absolute path to the imported file.
        source_type: File format marker (csv, ofx, etc.). Must be a key of
            REVERT_TABLES — anything else cannot be reverted.
        source_origin: Format/institution identifier (e.g., 'wells_fargo', 'tiller').
        account_names: List of account names this import touches.
        format_name: Tabular format name if a format matched; None for OFX.
        format_source: How the format was resolved ('built-in', 'saved', 'detected').
            None for OFX.

    Returns:
        UUID import_id for this batch.

    Raises:
        ValueError: If source_type is not in REVERT_TABLES.
    """
    if source_type not in REVERT_TABLES:
        raise ValueError(
            f"Unknown source_type {source_type!r}; "
            f"must be one of {sorted(REVERT_TABLES)}"
        )
    import_id = str(uuid.uuid4())
    db.execute(
        f"""
        INSERT INTO {IMPORT_LOG.full_name} (
            import_id, source_file, source_type, source_origin,
            format_name, format_source, account_names, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'importing')
        """,
        [
            import_id,
            source_file,
            source_type,
            source_origin,
            format_name,
            format_source,
            json.dumps(account_names),
        ],
    )
    logger.info(f"Created import batch: {import_id[:8]}...")
    return import_id


def update_format(
    db: Database,
    import_id: str,
    *,
    format_name: str | None,
    format_source: str | None,
) -> None:
    """Backfill the format columns on an in-flight import_log row.

    Tabular imports know the format before calling ``begin_import`` and pass
    it in there. PDFs route AFTER ``begin_import`` so the format is unknown
    at that point — this helper closes the observability gap by stamping
    ``format_name`` and ``format_source`` once the routing decision is in.
    Without it every PDF entry in ``raw.import_log`` has NULL format
    columns and users can't tell whether a replay or auto-derive served
    that import.
    """
    db.execute(
        f"""
        UPDATE {IMPORT_LOG.full_name}
        SET format_name = ?, format_source = ?
        WHERE import_id = ?
        """,
        [format_name, format_source, import_id],
    )


def finalize_import(
    db: Database,
    import_id: str,
    *,
    status: Literal["complete", "partial", "failed"],
    rows_total: int,
    rows_imported: int,
    rows_rejected: int = 0,
    rows_skipped_trailing: int = 0,
    rejection_details: list[dict[str, str]] | None = None,
    detection_confidence: str | None = None,
    number_format: str | None = None,
    date_format: str | None = None,
    sign_convention: str | None = None,
    balance_validated: bool | None = None,
) -> None:
    """Finalize an import batch with status and counts.

    The trailing arguments after rows_skipped_trailing are tabular-specific
    metadata. OFX callers leave them at their defaults (all None / not supplied).
    """
    db.execute(
        f"""
        UPDATE {IMPORT_LOG.full_name} SET
            status = ?,
            rows_total = ?,
            rows_imported = ?,
            rows_rejected = ?,
            rows_skipped_trailing = ?,
            rejection_details = ?,
            detection_confidence = ?,
            number_format = ?,
            date_format = ?,
            sign_convention = ?,
            balance_validated = ?,
            completed_at = CURRENT_TIMESTAMP
        WHERE import_id = ?
        """,
        [
            status,
            rows_total,
            rows_imported,
            rows_rejected,
            rows_skipped_trailing,
            json.dumps(rejection_details) if rejection_details else None,
            detection_confidence,
            number_format,
            date_format,
            sign_convention,
            balance_validated,
            import_id,
        ],
    )
    logger.info(
        f"Import {import_id[:8]}... finalized: {status} "
        f"({rows_imported} imported, {rows_rejected} rejected)"
    )


def get_import_history(
    db: Database,
    *,
    limit: int = 20,
    import_id: str | None = None,
) -> list[dict[str, str | int | None]]:
    """Query the import_log. If import_id is given, returns at most one row."""
    if import_id:
        rows = db.execute(
            f"""
            SELECT import_id, source_file, source_type, source_origin,
                   format_name, status, rows_imported, rows_rejected,
                   detection_confidence, started_at, completed_at
            FROM {IMPORT_LOG.full_name}
            WHERE import_id = ?
            """,
            [import_id],
        ).fetchall()
    else:
        rows = db.execute(
            f"""
            SELECT import_id, source_file, source_type, source_origin,
                   format_name, status, rows_imported, rows_rejected,
                   detection_confidence, started_at, completed_at
            FROM {IMPORT_LOG.full_name}
            ORDER BY started_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()

    return [dict(zip(_IMPORT_HISTORY_COLUMNS, row, strict=True)) for row in rows]


@dataclass(frozen=True)
class ImportHistoryPage:
    """One keyset page bound to an immutable import-history high-water mark."""

    records: list[dict[str, str | int | None]]
    total_count: int
    snapshot_started_at: str | None
    snapshot_import_id: str | None
    has_more: bool


def get_import_history_page(
    db: Database,
    *,
    limit: int,
    snapshot_started_at: str | None = None,
    snapshot_import_id: str | None = None,
    after_started_at: str | None = None,
    after_import_id: str | None = None,
    snapshot_total: int | None = None,
) -> ImportHistoryPage:
    """Read one bounded ``started_at DESC, import_id DESC`` keyset page."""
    if limit < 0:
        raise ValueError("limit must be non-negative")
    if (snapshot_started_at is None) != (snapshot_import_id is None):
        raise ValueError("snapshot fields must be supplied together")
    if (after_started_at is None) != (after_import_id is None):
        raise ValueError("after fields must be supplied together")
    if snapshot_total is not None and snapshot_total < 0:
        raise ValueError("snapshot_total must be non-negative")

    if snapshot_started_at is None:
        head = db.execute(
            f"""
            SELECT started_at, import_id
            FROM {IMPORT_LOG.full_name}
            ORDER BY started_at DESC, import_id DESC
            LIMIT 1
            """  # noqa: S608  # TableRef constant
        ).fetchone()
        if head is None:
            return ImportHistoryPage(
                records=[],
                total_count=0,
                snapshot_started_at=None,
                snapshot_import_id=None,
                has_more=False,
            )
        snapshot_started_at = str(head[0])
        snapshot_import_id = str(head[1])

    if snapshot_total is None:
        count_row = db.execute(
            f"""
            SELECT COUNT(*)
            FROM {IMPORT_LOG.full_name}
            WHERE started_at < CAST(? AS TIMESTAMP)
               OR (started_at = CAST(? AS TIMESTAMP) AND import_id <= ?)
            """,  # noqa: S608  # TableRef constant + parameterized keyset bound
            [snapshot_started_at, snapshot_started_at, snapshot_import_id],
        ).fetchone()
        snapshot_total = int(count_row[0]) if count_row is not None else 0

    rows = db.execute(
        f"""
            SELECT import_id, source_file, source_type, source_origin,
                   format_name, status, rows_imported, rows_rejected,
                   detection_confidence, started_at, completed_at
            FROM {IMPORT_LOG.full_name}
            WHERE (
                   started_at < CAST(? AS TIMESTAMP)
               OR (started_at = CAST(? AS TIMESTAMP) AND import_id <= ?)
            )
              AND (
                   CAST(? AS TIMESTAMP) IS NULL
                OR started_at < CAST(? AS TIMESTAMP)
                OR (started_at = CAST(? AS TIMESTAMP) AND import_id < ?)
              )
            ORDER BY started_at DESC, import_id DESC
            LIMIT ?
            """,  # noqa: S608  # TableRef constant + parameterized snapshot head
        [
            snapshot_started_at,
            snapshot_started_at,
            snapshot_import_id,
            after_started_at,
            after_started_at,
            after_started_at,
            after_import_id,
            limit + 1,
        ],
    ).fetchall()
    page_rows = rows[:limit]
    return ImportHistoryPage(
        records=[
            dict(zip(_IMPORT_HISTORY_COLUMNS, row, strict=True)) for row in page_rows
        ],
        total_count=snapshot_total,
        snapshot_started_at=snapshot_started_at,
        snapshot_import_id=snapshot_import_id,
        has_more=len(rows) > limit,
    )


def find_existing_import(
    db: Database,
    source_file: str,
) -> tuple[str, str] | None:
    """Return (import_id, status) for the most recent live batch, or None.

    Excludes 'reverted' and 'failed' rows. Returns 'importing' batches too
    so callers can distinguish a successful prior import from a crashed
    in-progress one in their error messages.
    """
    row = db.execute(
        f"""
        SELECT import_id, status
        FROM {IMPORT_LOG.full_name}
        WHERE source_file = ?
          AND status NOT IN ('reverted', 'failed')
        ORDER BY started_at DESC
        LIMIT 1
        """,
        [source_file],
    ).fetchone()
    if row is None:
        return None
    return (row[0], row[1])
