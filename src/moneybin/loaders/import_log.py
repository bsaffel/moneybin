"""Generic import-batch lifecycle for raw.import_log.

Both tabular and OFX import paths call these functions to create batches,
finalize them with row counts, query history, and revert by import_id.

The module is the single source of truth for which raw tables a given
source_type populates — see _REVERT_TABLES below.
"""

import json
import logging
import uuid
from typing import Literal

from moneybin.database import Database
from moneybin.tables import (
    IMPORT_LOG,
    MANUAL_TRANSACTIONS,
    OFX_ACCOUNTS,
    OFX_BALANCES,
    OFX_INSTITUTIONS,
    OFX_TRANSACTIONS,
    TABULAR_ACCOUNTS,
    TABULAR_TRANSACTIONS,
    TableRef,
)

logger = logging.getLogger(__name__)


_SourceType = Literal[
    "csv", "tsv", "excel", "parquet", "feather", "pipe", "ofx", "manual"
]


# Allowlist mapping source_type → raw tables that carry rows for that type.
# revert_import() uses this to know what to delete. Adding a new format means
# adding an entry here AND ensuring those tables have an import_id column.
_TABULAR_RAW_TABLES = [TABULAR_TRANSACTIONS, TABULAR_ACCOUNTS]
_REVERT_TABLES: dict[str, list[TableRef]] = {
    "csv": _TABULAR_RAW_TABLES,
    "tsv": _TABULAR_RAW_TABLES,
    "excel": _TABULAR_RAW_TABLES,
    "parquet": _TABULAR_RAW_TABLES,
    "feather": _TABULAR_RAW_TABLES,
    "pipe": _TABULAR_RAW_TABLES,
    "ofx": [OFX_TRANSACTIONS, OFX_ACCOUNTS, OFX_BALANCES, OFX_INSTITUTIONS],
    "manual": [MANUAL_TRANSACTIONS],
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
            _REVERT_TABLES — anything else cannot be reverted.
        source_origin: Format/institution identifier (e.g., 'wells_fargo', 'tiller').
        account_names: List of account names this import touches.
        format_name: Tabular format name if a format matched; None for OFX.
        format_source: How the format was resolved ('built-in', 'saved', 'detected').
            None for OFX.

    Returns:
        UUID import_id for this batch.

    Raises:
        ValueError: If source_type is not in _REVERT_TABLES.
    """
    if source_type not in _REVERT_TABLES:
        raise ValueError(
            f"Unknown source_type {source_type!r}; "
            f"must be one of {sorted(_REVERT_TABLES)}"
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


def revert_import(db: Database, import_id: str) -> dict[str, str | int]:
    """Revert an import batch by deleting all its rows from raw tables.

    Looks up source_type from raw.import_log to determine which tables to
    delete from (via the _REVERT_TABLES allowlist). Updates status to 'reverted'.

    Returns:
        {'status': 'reverted', 'rows_deleted': N} on success.
        {'status': 'not_found', ...} if import_id doesn't exist.
        {'status': 'already_reverted'} if already reverted.
        {'status': 'superseded', ...} if a later import overwrote the rows.
    """
    row = db.execute(
        f"SELECT source_type, status, source_file, started_at "
        f"FROM {IMPORT_LOG.full_name} WHERE import_id = ?",
        [import_id],
    ).fetchone()

    if row is None:
        return {"status": "not_found", "reason": f"No import with ID {import_id}"}

    src_type, status, source_file, started_at = row

    if status == "reverted":
        return {"status": "already_reverted"}

    if src_type not in _REVERT_TABLES:
        return {
            "status": "unsupported",
            "reason": f"Cannot revert source_type {src_type!r}",
        }

    tables = _REVERT_TABLES[src_type]

    # Sum across every table the source_type populates. OFX statements with
    # zero transactions but populated accounts/balances must still be
    # detectable as live (not superseded) and reportable in rows_deleted.
    rows_to_delete = 0
    for table in tables:
        result = db.execute(
            f"SELECT COUNT(*) FROM {table.full_name} WHERE import_id = ?",
            [import_id],
        ).fetchone()
        if result:
            rows_to_delete += result[0]

    if rows_to_delete == 0:
        # Same superseded check as the original tabular_loader: if a later
        # import upserted over this one's rows, surface that.
        reimport_row = db.execute(
            f"""
            SELECT import_id
            FROM {IMPORT_LOG.full_name}
            WHERE source_file = ?
              AND import_id != ?
              AND started_at > ?
              AND status NOT IN ('reverted', 'failed')
            ORDER BY started_at DESC
            LIMIT 1
            """,
            [source_file, import_id, started_at],
        ).fetchone()
        if reimport_row:
            newer_id = reimport_row[0]
            return {
                "status": "superseded",
                "reason": (
                    f"File was re-imported as {newer_id[:8]}...; "
                    f"revert that batch to remove the data."
                ),
            }

    db.begin()
    try:
        for table in tables:
            db.execute(
                f"DELETE FROM {table.full_name} WHERE import_id = ?",
                [import_id],
            )
        db.execute(
            f"""
            UPDATE {IMPORT_LOG.full_name} SET
                status = 'reverted',
                reverted_at = CURRENT_TIMESTAMP
            WHERE import_id = ?
            """,
            [import_id],
        )
        db.commit()
    except Exception:
        db.rollback()
        raise

    logger.info(f"Reverted import {import_id[:8]}...: {rows_to_delete} rows deleted")
    return {"status": "reverted", "rows_deleted": rows_to_delete}


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
