"""Load Plaid sync JSON into raw.plaid_* DuckDB tables.

Sign convention: raw tables preserve Plaid's native convention
(positive = expense). The -1 * amount flip is done EXCLUSIVELY in
prep.stg_plaid__transactions. See docs/specs/2026-05-13-plaid-sync-design.md
Section 5 — flipping anywhere else silently corrupts cross-source
aggregations.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

import polars as pl

from moneybin.connectors.sync_models import (
    SyncAccount,
    SyncDataResponse,
)
from moneybin.database import Database

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoadResult:
    """Per-table row counts returned by PlaidLoader.load()."""

    accounts: int
    transactions: int
    balances: int


_ACCOUNTS_SCHEMA = pl.Schema({
    "account_id": pl.Utf8,
    "account_type": pl.Utf8,
    "account_subtype": pl.Utf8,
    "institution_name": pl.Utf8,
    "official_name": pl.Utf8,
    "mask": pl.Utf8,
    "source_file": pl.Utf8,
    "source_type": pl.Utf8,
    "source_origin": pl.Utf8,
    "extracted_at": pl.Datetime(time_zone="UTC"),
    "loaded_at": pl.Datetime(time_zone="UTC"),
})


class PlaidLoader:
    """Load Plaid sync data into raw.plaid_* tables.

    Caller manages the Database connection lifetime per ADR-010:

        with get_database(read_only=False) as db:
            loader = PlaidLoader(db)
            result = loader.load(sync_data, job_id)
    """

    def __init__(self, db: Database) -> None:
        """Initialize with an active Database connection."""
        self.db = db

    def load(self, sync_data: SyncDataResponse, job_id: str) -> LoadResult:
        """Load accounts, transactions, balances from one sync response.

        Returns per-table counts. Does NOT handle removed_transactions —
        call handle_removed_transactions() separately.
        """
        source_file = f"sync_{job_id}"
        extracted_at = sync_data.metadata.synced_at
        loaded_at = datetime.now(UTC)
        item_by_account = self._build_account_to_item_map(sync_data)

        accounts_loaded = self._load_accounts(
            sync_data.accounts,
            item_by_account,
            source_file,
            extracted_at,
            loaded_at,
        )
        # Transactions and balances come in later tasks.
        return LoadResult(accounts=accounts_loaded, transactions=0, balances=0)

    def _build_account_to_item_map(self, sync_data: SyncDataResponse) -> dict[str, str]:
        """Each account belongs to exactly one institution (provider_item_id).

        The server's GET /sync/data response doesn't include item_id per account
        directly — accounts inherit it from their institution context. Phase 1
        assumes the metadata.institutions[] has exactly one entry per item, and
        all accounts in this sync belong to a single institution batch.
        For multi-institution syncs, the server should structure the response
        with per-institution account groupings (see follow-up).
        """
        # Phase 1: single-institution-per-sync assumption.
        # If sync_data.metadata.institutions has multiple entries, attribute
        # each account to its institution_name match. Otherwise, all accounts
        # get the single institution's provider_item_id.
        institutions = sync_data.metadata.institutions
        if len(institutions) == 1:
            single_item = institutions[0].provider_item_id
            return {acc.account_id: single_item for acc in sync_data.accounts}
        # Multi-institution: match by institution_name
        name_to_item = {i.institution_name: i.provider_item_id for i in institutions}
        return {
            acc.account_id: name_to_item.get(acc.institution_name, "")
            for acc in sync_data.accounts
        }

    def _load_accounts(
        self,
        accounts: list[SyncAccount],
        item_by_account: dict[str, str],
        source_file: str,
        extracted_at: datetime,
        loaded_at: datetime,
    ) -> int:
        if not accounts:
            return 0
        df = pl.DataFrame(
            [
                {
                    **acc.model_dump(),
                    "source_file": source_file,
                    "source_type": "plaid",
                    "source_origin": item_by_account[acc.account_id],
                    "extracted_at": extracted_at,
                    "loaded_at": loaded_at,
                }
                for acc in accounts
            ],
            schema=_ACCOUNTS_SCHEMA,
        )
        self.db.ingest_dataframe("raw.plaid_accounts", df, on_conflict="upsert")
        logger.info(f"Loaded {len(df)} Plaid accounts")
        return len(df)
