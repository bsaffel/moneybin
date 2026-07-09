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

    columns = [
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
    return [dict(zip(columns, row, strict=True)) for row in rows]


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
