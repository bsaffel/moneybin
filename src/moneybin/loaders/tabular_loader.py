"""Stage 5: Tabular data loader.

Handles raw table writes via Database.ingest_dataframe(). Batch lifecycle
delegates to moneybin.loaders.import_log.
"""

import logging

import polars as pl

from moneybin.database import Database
from moneybin.loaders import import_log
from moneybin.metrics.registry import TABULAR_IMPORT_BATCHES
from moneybin.tables import TABULAR_ACCOUNTS, TABULAR_TRANSACTIONS

logger = logging.getLogger(__name__)


class TabularLoader:
    """Load tabular data into DuckDB raw tables with batch tracking."""

    def __init__(self, db: Database) -> None:
        """Initialize with an active Database connection."""
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
        """Create an import batch record. Delegates to import_log module."""
        return import_log.begin_import(
            self.db,
            source_file=source_file,
            source_type=source_type,  # type: ignore[arg-type]  # runtime-validated by begin_import
            source_origin=source_origin,
            account_names=account_names,
            format_name=format_name,
            format_source=format_source,
        )

    def load_transactions(self, df: pl.DataFrame) -> int:
        """Write transactions to raw.tabular_transactions; return count loaded."""
        if len(df) == 0:
            return 0
        self.db.ingest_dataframe(
            TABULAR_TRANSACTIONS.full_name, df, on_conflict="upsert"
        )
        logger.info(f"Loaded {len(df)} transactions")
        return len(df)

    def load_accounts(self, df: pl.DataFrame) -> int:
        """Write accounts to raw.tabular_accounts; return count loaded."""
        if len(df) == 0:
            return 0
        self.db.ingest_dataframe(TABULAR_ACCOUNTS.full_name, df, on_conflict="upsert")
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
        """Finalize an import batch. Delegates to import_log module + records metric."""
        if rows_imported == 0 and rows_rejected > 0:
            status = "failed"
        elif rows_rejected == 0:
            status = "complete"
        else:
            status = "partial"
        TABULAR_IMPORT_BATCHES.labels(status=status).inc()
        import_log.finalize_import(
            self.db,
            import_id,
            status=status,
            rows_total=rows_total,
            rows_imported=rows_imported,
            rows_rejected=rows_rejected,
            rows_skipped_trailing=rows_skipped_trailing,
            rejection_details=rejection_details,
            detection_confidence=detection_confidence,
            number_format=number_format,
            date_format=date_format,
            sign_convention=sign_convention,
            balance_validated=balance_validated,
        )

    def revert_import(self, import_id: str) -> dict[str, str | int]:
        """Delegate to import_log module."""
        return import_log.revert_import(self.db, import_id)

    def get_import_history(
        self,
        *,
        limit: int = 20,
        import_id: str | None = None,
    ) -> list[dict[str, str | int | None]]:
        """Delegate to import_log module."""
        return import_log.get_import_history(self.db, limit=limit, import_id=import_id)
