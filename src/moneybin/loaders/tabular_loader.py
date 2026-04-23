"""Stage 5: Tabular data loader.

Handles import batch tracking, raw table writes via Database.ingest_dataframe(),
format persistence, and import reverting.
"""

import json
import logging
import uuid

import polars as pl

from moneybin.database import Database
from moneybin.metrics.registry import TABULAR_IMPORT_BATCHES

logger = logging.getLogger(__name__)


class TabularLoader:
    """Load tabular data into DuckDB raw tables with batch tracking."""

    def __init__(self, db: Database) -> None:
        """Initialize the tabular loader.

        Args:
            db: Database instance for all database operations.
        """
        self.db = db

    def create_import_batch(
        self,
        *,
        source_file: str,
        source_type: str,
        source_origin: str,
        account_names: list[str],
        format_name: str | None = None,
        format_source: str | None = None,
    ) -> str:
        """Create an import batch record in raw.import_log.

        Args:
            source_file: Absolute path to the imported file.
            source_type: File format (csv, tsv, excel, etc.).
            source_origin: Format/institution identifier.
            account_names: List of account names in this import.
            format_name: Matched or saved format name (if any).
            format_source: How the format was resolved.

        Returns:
            UUID import_id for this batch.
        """
        import_id = str(uuid.uuid4())
        self.db.execute(
            """
            INSERT INTO raw.import_log (
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

    def load_transactions(self, df: pl.DataFrame) -> int:
        """Load transactions into raw.tabular_transactions.

        Args:
            df: Transformed transactions DataFrame.

        Returns:
            Number of rows loaded.
        """
        if len(df) == 0:
            return 0
        self.db.ingest_dataframe("raw.tabular_transactions", df, on_conflict="upsert")
        logger.info(f"Loaded {len(df)} transactions")
        return len(df)

    def load_accounts(self, df: pl.DataFrame) -> int:
        """Load accounts into raw.tabular_accounts.

        Args:
            df: Accounts DataFrame.

        Returns:
            Number of rows loaded.
        """
        if len(df) == 0:
            return 0
        self.db.ingest_dataframe("raw.tabular_accounts", df, on_conflict="upsert")
        logger.info(f"Loaded {len(df)} accounts")
        return len(df)

    def finalize_import_batch(
        self,
        *,
        import_id: str,
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
        """Finalize an import batch with results.

        Args:
            import_id: UUID of the import batch.
            rows_total: Total rows in source file.
            rows_imported: Rows successfully imported.
            rows_rejected: Rows that failed validation.
            rows_skipped_trailing: Trailing junk rows removed.
            rejection_details: Per-rejected-row details.
            detection_confidence: Confidence tier used.
            number_format: Number convention used.
            date_format: Date format string used.
            sign_convention: Sign convention applied.
            balance_validated: Whether balance validation passed.
        """
        if rows_imported == 0 and rows_rejected > 0:
            status = "failed"
        elif rows_rejected == 0:
            status = "complete"
        else:
            status = "partial"
        TABULAR_IMPORT_BATCHES.labels(status=status).inc()
        self.db.execute(
            """
            UPDATE raw.import_log SET
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

    def revert_import(self, import_id: str) -> dict[str, str | int]:
        """Revert an import batch by deleting all its rows.

        Args:
            import_id: UUID of the import to revert.

        Returns:
            Dict with status and details.
        """
        row = self.db.execute(
            "SELECT import_id, status FROM raw.import_log WHERE import_id = ?",
            [import_id],
        ).fetchone()

        if row is None:
            return {"status": "not_found", "reason": f"No import with ID {import_id}"}

        if row[1] == "reverted":
            return {"status": "already_reverted"}

        txn_count = self.db.execute(
            "SELECT COUNT(*) FROM raw.tabular_transactions WHERE import_id = ?",
            [import_id],
        ).fetchone()
        txn_deleted = txn_count[0] if txn_count else 0

        # If no rows found, check whether this file was re-imported under a
        # newer batch — the upsert replaces import_id on key collision.
        if txn_deleted == 0:
            reimport_row = self.db.execute(
                """
                SELECT il2.import_id
                FROM raw.import_log il1
                JOIN raw.import_log il2
                    ON il2.source_file = il1.source_file
                    AND il2.import_id != il1.import_id
                    AND il2.started_at > il1.started_at
                    AND il2.status NOT IN ('reverted', 'failed')
                WHERE il1.import_id = ?
                ORDER BY il2.started_at DESC
                LIMIT 1
                """,
                [import_id],
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

        self.db.begin()
        try:
            self.db.execute(
                "DELETE FROM raw.tabular_transactions WHERE import_id = ?",
                [import_id],
            )
            self.db.execute(
                "DELETE FROM raw.tabular_accounts WHERE import_id = ?",
                [import_id],
            )
            self.db.execute(
                """
                UPDATE raw.import_log SET
                    status = 'reverted',
                    reverted_at = CURRENT_TIMESTAMP
                WHERE import_id = ?
                """,
                [import_id],
            )
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

        logger.info(f"Reverted import {import_id[:8]}...: {txn_deleted} rows deleted")
        return {"status": "reverted", "rows_deleted": txn_deleted}

    def get_import_history(
        self,
        *,
        limit: int = 20,
        import_id: str | None = None,
    ) -> list[dict[str, str | int | None]]:
        """Query import history.

        Args:
            limit: Maximum number of records to return.
            import_id: Filter to a specific import ID.

        Returns:
            List of import log records.
        """
        if import_id:
            rows = self.db.execute(
                """
                SELECT import_id, source_file, source_type, source_origin,
                       format_name, status, rows_imported, rows_rejected,
                       detection_confidence, started_at, completed_at
                FROM raw.import_log
                WHERE import_id = ?
                """,
                [import_id],
            ).fetchall()
        else:
            rows = self.db.execute(
                """
                SELECT import_id, source_file, source_type, source_origin,
                       format_name, status, rows_imported, rows_rejected,
                       detection_confidence, started_at, completed_at
                FROM raw.import_log
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
