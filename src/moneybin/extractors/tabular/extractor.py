"""Stage 5: Tabular data extractor.

Handles raw table writes via Database.ingest_dataframe(). Batch lifecycle
(``ImportLogRepo``) is owned by the caller (``ImportService``) — this
extractor no longer touches it, so the extractors layer stays below
services/repositories (see `.claude/rules/design-principles.md`).

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
from moneybin.tables import TABULAR_ACCOUNTS, TABULAR_TRANSACTIONS

logger = logging.getLogger(__name__)


def build_account_dataframe(
    *,
    acct_id_to_name: dict[str, str],
    source_label_by_key: dict[str, str],
    label_parsed_by_key: dict[str, tuple[str, str | None]],
    number_last4_by_key: dict[str, str | None],
    institution_by_key: dict[str, str | None],
    file_path: Path,
    source_type: str,
    source_origin: str,
    import_id: str,
) -> pl.DataFrame:
    """One raw.tabular_accounts row per unique account this file presents.

    Every dict is keyed by native account key (``source_account_key``) and
    populated by the caller's source-account enumeration — this function only
    shapes the row, it does not decide identity, naming, or institution.

    ``account_label`` and ``institution_name`` are decided by the caller
    before any row is written, because the mint report has to state the same
    value this stage stores — two spellings of one expression is how the
    reported name and the stored one used to drift apart.

    ``account_number_masked`` reuses the caller's parsed last4
    (``label_parsed_by_key``), falling back to the mapped account-number
    column's last4 (``number_last4_by_key``) — never a second parse pass.
    """
    unique_ids = sorted(acct_id_to_name.keys())
    account_number_masked: dict[str, str | None] = {}
    for aid in unique_ids:
        l4 = label_parsed_by_key[aid][1] or number_last4_by_key.get(aid)
        account_number_masked[aid] = f"****{l4}" if l4 else None
    return pl.DataFrame({
        "account_id": unique_ids,
        "account_name": [acct_id_to_name[aid] for aid in unique_ids],
        "account_label": [source_label_by_key.get(aid) for aid in unique_ids],
        "account_number": [None] * len(unique_ids),
        "account_number_masked": [account_number_masked[aid] for aid in unique_ids],
        "account_type": [None] * len(unique_ids),
        "institution_name": [institution_by_key.get(aid) for aid in unique_ids],
        "currency": [None] * len(unique_ids),
        "source_file": [str(file_path)] * len(unique_ids),
        "source_type": [source_type] * len(unique_ids),
        "source_origin": [source_origin] * len(unique_ids),
        "import_id": [import_id] * len(unique_ids),
    })


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
        continue to use ``load_transactions()`` / ``load_accounts()`` directly,
        with the caller (``ImportService``) owning batch lifecycle via
        ``ImportLogRepo``.
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
