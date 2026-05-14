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
    SyncBalance,
    SyncDataResponse,
    SyncTransaction,
)
from moneybin.database import Database

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoadResult:
    """Per-table row counts returned by PlaidLoader.load()."""

    accounts_loaded: int
    transactions_loaded: int
    balances_loaded: int


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

_TRANSACTIONS_SCHEMA = pl.Schema({
    "transaction_id": pl.Utf8,
    "account_id": pl.Utf8,
    "transaction_date": pl.Date,
    "amount": pl.Decimal(18, 2),  # Plaid convention preserved; sign flip in staging
    "description": pl.Utf8,
    "merchant_name": pl.Utf8,
    "category": pl.Utf8,
    "pending": pl.Boolean,
    "source_file": pl.Utf8,
    "source_type": pl.Utf8,
    "source_origin": pl.Utf8,
    "extracted_at": pl.Datetime(time_zone="UTC"),
    "loaded_at": pl.Datetime(time_zone="UTC"),
})

_BALANCES_SCHEMA = pl.Schema({
    "account_id": pl.Utf8,
    "balance_date": pl.Date,
    "current_balance": pl.Decimal(18, 2),
    "available_balance": pl.Decimal(18, 2),
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
        transactions_loaded = self._load_transactions(
            sync_data.transactions,
            item_by_account,
            source_file,
            extracted_at,
            loaded_at,
        )
        balances_loaded = self._load_balances(
            sync_data.balances,
            item_by_account,
            source_file,
            extracted_at,
            loaded_at,
        )
        return LoadResult(
            accounts_loaded=accounts_loaded,
            transactions_loaded=transactions_loaded,
            balances_loaded=balances_loaded,
        )

    def _build_account_to_item_map(self, sync_data: SyncDataResponse) -> dict[str, str]:
        """Map each account_id to its provider_item_id.

        Single-institution sync: all accounts get the one item's id.

        Multi-institution sync: match each account to its institution by name.
        Raises if an account's institution_name doesn't appear in metadata, or
        if two institutions in metadata share the same name — silent fallback
        ("" source_origin) would cause dedup collapse on `(account_id, source_origin)`
        and break downstream joins. Server should structure responses with per-
        institution account groupings to make this unambiguous (followup).

        Also raises if `sync_data.transactions` or `sync_data.balances` reference
        an `account_id` not present in `sync_data.accounts` — eventual-consistency
        on Plaid's side surfaces this occasionally, and a KeyError during the
        per-row dict lookup leaves no useful context. Loud and explicit is better.
        """
        institutions = sync_data.metadata.institutions
        if len(institutions) == 1:
            single_item = institutions[0].provider_item_id
            mapping = {acc.account_id: single_item for acc in sync_data.accounts}
        else:
            name_to_item: dict[str | None, str] = {}
            for inst in institutions:
                if inst.institution_name in name_to_item:
                    raise ValueError(
                        f"multi-institution sync metadata has duplicate institution_name "
                        f"{inst.institution_name!r}; cannot attribute accounts unambiguously"
                    )
                name_to_item[inst.institution_name] = inst.provider_item_id

            mapping = {}
            for acc in sync_data.accounts:
                if acc.institution_name not in name_to_item:
                    raise ValueError(
                        f"account {acc.account_id} has institution_name "
                        f"{acc.institution_name!r} not present in sync metadata "
                        f"({sorted(str(n) for n in name_to_item)})"
                    )
                mapping[acc.account_id] = name_to_item[acc.institution_name]

        referenced = {txn.account_id for txn in sync_data.transactions} | {
            bal.account_id for bal in sync_data.balances
        }
        orphans = referenced - mapping.keys()
        if orphans:
            raise ValueError(
                f"transactions/balances reference account_id(s) not present in "
                f"sync_data.accounts: {sorted(orphans)}. This typically indicates "
                f"eventual-consistency drift on the server — retry the sync, and "
                f"if it persists, the server's account_id stream is out of sync "
                f"with its transaction stream."
            )
        return mapping

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

    def _load_transactions(
        self,
        transactions: list[SyncTransaction],
        item_by_account: dict[str, str],
        source_file: str,
        extracted_at: datetime,
        loaded_at: datetime,
    ) -> int:
        if not transactions:
            return 0
        # DO NOT NEGATE amount here. Plaid convention (positive = expense)
        # is preserved in raw. The sign flip lives in stg_plaid__transactions.
        df = pl.DataFrame(
            [
                {
                    **txn.model_dump(),
                    "source_file": source_file,
                    "source_type": "plaid",
                    "source_origin": item_by_account[txn.account_id],
                    "extracted_at": extracted_at,
                    "loaded_at": loaded_at,
                }
                for txn in transactions
            ],
            schema=_TRANSACTIONS_SCHEMA,
        )
        self.db.ingest_dataframe("raw.plaid_transactions", df, on_conflict="upsert")
        logger.info(f"Loaded {len(df)} Plaid transactions")
        return len(df)

    def _load_balances(
        self,
        balances: list[SyncBalance],
        item_by_account: dict[str, str],
        source_file: str,
        extracted_at: datetime,
        loaded_at: datetime,
    ) -> int:
        if not balances:
            return 0
        df = pl.DataFrame(
            [
                {
                    **bal.model_dump(),
                    "source_file": source_file,
                    "source_type": "plaid",
                    "source_origin": item_by_account[bal.account_id],
                    "extracted_at": extracted_at,
                    "loaded_at": loaded_at,
                }
                for bal in balances
            ],
            schema=_BALANCES_SCHEMA,
        )
        self.db.ingest_dataframe("raw.plaid_balances", df, on_conflict="upsert")
        logger.info(f"Loaded {len(df)} Plaid balance snapshots")
        return len(df)

    def handle_removed_transactions(self, removed_ids: list[str]) -> int:
        """Delete transactions Plaid has removed; return the rowcount actually deleted.

        Stale IDs (already removed in a prior sync, or never landed locally) don't
        inflate the count — the return value flows into `PullResult.transactions_removed`
        and the CLI's "Removed N stale transactions" message, so it must reflect
        reality rather than the request.
        """
        if not removed_ids:
            return 0
        placeholders = ", ".join("?" for _ in removed_ids)
        # Pre-count to report the rows DELETE will actually affect. DuckDB doesn't
        # surface affected-row counts through this connection API, so a separate
        # COUNT(*) is the simplest accurate signal; the two statements share the
        # write lock for the duration of the call.
        count_row = self.db.execute(
            f"SELECT COUNT(*) FROM raw.plaid_transactions WHERE transaction_id IN ({placeholders})",  # noqa: S608  # placeholders are ?, values parameterized
            removed_ids,
        ).fetchone()
        deleted = int(count_row[0]) if count_row else 0
        self.db.execute(
            f"DELETE FROM raw.plaid_transactions WHERE transaction_id IN ({placeholders})",  # noqa: S608  # placeholders are ?, values parameterized
            removed_ids,
        )
        return deleted
