"""Stage 5: Tabular data extractor.

Handles raw table writes via Database.ingest_dataframe(). Batch lifecycle
delegates to moneybin.loaders.import_log.

This module is the Protocol-compliant entry point for the tabular provider;
it composes the format-detection, reading, and column-mapping primitives
in neighbor modules (``format_detector``, ``readers``, ``column_mapper``,
``transforms``) rather than reimplementing them.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from moneybin.database import Database
from moneybin.extractors._types import (
    ExtractionResult,
    FilePath,
    ProviderSource,
)
from moneybin.extractors.tabular.config import TabularProviderConfig
from moneybin.loaders import import_log
from moneybin.metrics.registry import TABULAR_IMPORT_BATCHES
from moneybin.tables import TABULAR_ACCOUNTS, TABULAR_TRANSACTIONS

logger = logging.getLogger(__name__)


class TabularExtractor:
    """Load tabular data into DuckDB raw tables with batch tracking.

    Caller manages the Database connection lifetime per ADR-010:

        with get_database(read_only=False) as db:
            extractor = TabularExtractor(db)
            extractor.load_transactions(df)
    """

    name = "tabular"
    """Provider name; matches raw.tabular_* table prefix."""

    source_type = "tabular"
    """Written into source_type column on every row produced by this provider.

    Note: per-row ``source_type`` for tabular imports records the concrete file
    format (``csv``, ``tsv``, ``excel``, ``parquet``, ``feather``) supplied by
    the caller; this class-level attribute is the provider-level identifier
    used by framework wiring (Task 5).
    """

    def __init__(
        self, db: Database, config: TabularProviderConfig | None = None
    ) -> None:
        """Initialize with an active Database connection.

        Args:
            db: An active Database connection (caller-managed per ADR-010).
            config: Provider configuration; defaults to empty
                ``TabularProviderConfig``. Tunables live on the same model
                and are surfaced via
                ``get_settings().providers.tabular`` at the service boundary.
        """
        self.db = db
        self.config = config or TabularProviderConfig()

    def extract(self, source: ProviderSource) -> ExtractionResult:
        """Provider Protocol entry point.

        Tabular accepts ``FilePath`` only. Framework decoration that supplies
        ``import_id`` and ``source_origin`` lands in Plan 2; existing callers
        continue to use ``load_transactions()`` / ``load_accounts()`` and the
        batch-lifecycle methods directly.
        """
        if not isinstance(source, FilePath):
            raise TypeError(
                f"TabularExtractor expects FilePath; got {type(source).__name__}"
            )
        raise NotImplementedError(
            "TabularExtractor.extract() will be wired in Plan 2 (framework "
            "decoration supplies import_id and source_origin). Use the "
            "existing load_transactions() / load_accounts() entry points "
            "for now."
        )

    def schema_files(self) -> list[Path]:
        """Return paths to raw.tabular_* DDL files bundled with this package."""
        schema_dir = Path(__file__).parent / "schema"
        return sorted(schema_dir.glob("raw_tabular_*.sql"))

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
        # Zero-row imports (whether all-rejected, all-trailing-skipped, or
        # an entirely empty file) must NOT report "complete" — that would
        # be a green signal for an import that wrote nothing. Map any
        # zero-imported outcome to "failed" so callers can detect it.
        if rows_imported == 0:
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

    def get_import_history(
        self,
        *,
        limit: int = 20,
        import_id: str | None = None,
    ) -> list[dict[str, str | int | None]]:
        """Delegate to import_log module."""
        return import_log.get_import_history(self.db, limit=limit, import_id=import_id)
