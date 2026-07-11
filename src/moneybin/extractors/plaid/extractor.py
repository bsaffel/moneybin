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
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import polars as pl

from moneybin.connectors.sync_models import (
    SyncAccount,
    SyncBalance,
    SyncDataResponse,
    SyncHolding,
    SyncInvestmentTransaction,
    SyncSecurity,
    SyncTransaction,
)
from moneybin.database import Database
from moneybin.extractors._types import ExtractionResult, ProviderSource, SyncResponse
from moneybin.extractors.plaid.config import PlaidProviderConfig
from moneybin.metrics.registry import (
    INVESTMENT_AMOUNT_DRIFT_ROWS_TOTAL,
    SYNC_INVESTMENTS_RECORDS_LOADED,
)
from moneybin.tables import (
    PLAID_INVESTMENT_HOLDING_LOTS,
    PLAID_INVESTMENT_HOLDINGS,
    PLAID_INVESTMENT_TRANSACTIONS,
    PLAID_SECURITIES,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoadResult:
    """Per-table row counts returned by PlaidExtractor.load()."""

    accounts_loaded: int
    transactions_loaded: int
    balances_loaded: int
    securities_loaded: int = 0
    investment_transactions_loaded: int = 0
    holdings_loaded: int = 0
    holding_lots_loaded: int = 0


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
    "original_description": pl.Utf8,
    "iso_currency_code": pl.Utf8,
    "authorized_date": pl.Date,
    "pending_transaction_id": pl.Utf8,
    "payment_channel": pl.Utf8,
    "check_number": pl.Utf8,
    "merchant_entity_id": pl.Utf8,
    "location_address": pl.Utf8,
    "location_city": pl.Utf8,
    "location_region": pl.Utf8,
    "location_postal_code": pl.Utf8,
    "location_country": pl.Utf8,
    "location_latitude": pl.Float64,
    "location_longitude": pl.Float64,
    "category_detailed": pl.Utf8,
    "category_confidence": pl.Utf8,
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

_SECURITIES_SCHEMA = pl.Schema({
    "security_id": pl.Utf8,
    "institution_security_id": pl.Utf8,
    "institution_id": pl.Utf8,
    "ticker_symbol": pl.Utf8,
    "market_identifier_code": pl.Utf8,
    "security_name": pl.Utf8,
    "security_type": pl.Utf8,
    "close_price": pl.Decimal(28, 10),
    "close_price_as_of": pl.Date,
    "iso_currency_code": pl.Utf8,
    "unofficial_currency_code": pl.Utf8,
    "cusip": pl.Utf8,
    "isin": pl.Utf8,
    "is_cash_equivalent": pl.Boolean,
    "source_file": pl.Utf8,
    "source_type": pl.Utf8,
    "source_origin": pl.Utf8,
    "extracted_at": pl.Datetime(time_zone="UTC"),
    "loaded_at": pl.Datetime(time_zone="UTC"),
})

_INVESTMENT_TRANSACTIONS_SCHEMA = pl.Schema({
    "investment_transaction_id": pl.Utf8,
    "account_id": pl.Utf8,
    "security_id": pl.Utf8,
    "transaction_date": pl.Date,
    "transaction_datetime": pl.Datetime(time_zone="UTC"),
    "transaction_name": pl.Utf8,
    "quantity": pl.Decimal(28, 10),
    "amount": pl.Decimal(18, 2),
    "price": pl.Decimal(28, 10),
    "fees": pl.Decimal(18, 2),
    "iso_currency_code": pl.Utf8,
    "unofficial_currency_code": pl.Utf8,
    "investment_transaction_type": pl.Utf8,
    "investment_transaction_subtype": pl.Utf8,
    "source_file": pl.Utf8,
    "source_type": pl.Utf8,
    "source_origin": pl.Utf8,
    "extracted_at": pl.Datetime(time_zone="UTC"),
    "loaded_at": pl.Datetime(time_zone="UTC"),
})

_INVESTMENT_HOLDINGS_SCHEMA = pl.Schema({
    "account_id": pl.Utf8,
    "security_id": pl.Utf8,
    "holdings_date": pl.Date,
    "institution_price": pl.Decimal(28, 10),
    "institution_price_as_of": pl.Date,
    "institution_value": pl.Decimal(18, 2),
    "cost_basis": pl.Decimal(18, 2),
    "quantity": pl.Decimal(28, 10),
    "iso_currency_code": pl.Utf8,
    "unofficial_currency_code": pl.Utf8,
    "vested_quantity": pl.Decimal(28, 10),
    "vested_value": pl.Decimal(18, 2),
    "transactions_window_start": pl.Date,
    "source_file": pl.Utf8,
    "source_type": pl.Utf8,
    "source_origin": pl.Utf8,
    "extracted_at": pl.Datetime(time_zone="UTC"),
    "loaded_at": pl.Datetime(time_zone="UTC"),
})

_INVESTMENT_HOLDING_LOTS_SCHEMA = pl.Schema({
    "account_id": pl.Utf8,
    "security_id": pl.Utf8,
    "lot_index": pl.Int32,
    "institution_lot_id": pl.Utf8,
    "original_purchase_datetime": pl.Datetime(time_zone="UTC"),
    "quantity": pl.Decimal(28, 10),
    "purchase_price": pl.Decimal(28, 10),
    "cost_basis": pl.Decimal(18, 2),
    "current_value": pl.Decimal(18, 2),
    "position_type": pl.Utf8,
    "source_file": pl.Utf8,
    "source_type": pl.Utf8,
    "source_origin": pl.Utf8,
    "extracted_at": pl.Datetime(time_zone="UTC"),
    "loaded_at": pl.Datetime(time_zone="UTC"),
})


class PlaidExtractor:
    """Load Plaid sync data into raw.plaid_* tables.

    Caller manages the Database connection lifetime per ADR-010:

        with get_database(read_only=False) as db:
            extractor = PlaidExtractor(db)
            result = extractor.load(sync_data, job_id)
    """

    name = "plaid"
    """Provider name; matches raw.plaid_* table prefix."""

    source_type = "plaid"
    """Written into source_type column on every row produced by this provider."""

    def __init__(self, db: Database, config: PlaidProviderConfig | None = None) -> None:
        """Initialize with an active Database connection.

        Args:
            db: An active Database connection (caller-managed per ADR-010).
            config: Provider configuration; defaults to empty PlaidProviderConfig.
        """
        self.db = db
        self.config = config or PlaidProviderConfig()

    def extract(self, source: ProviderSource) -> ExtractionResult:
        """Provider Protocol entry point.

        Plaid accepts ``SyncResponse`` only. Framework decoration that
        supplies ``import_id`` and ``source_origin`` lands in Plan 2;
        existing callers continue to use ``load()`` directly.
        """
        if not isinstance(source, SyncResponse):
            raise TypeError(
                f"PlaidExtractor expects SyncResponse; got {type(source).__name__}"
            )
        raise NotImplementedError(
            "PlaidExtractor.extract() will be wired in Plan 2 (framework "
            "decoration). Use load() for now."
        )

    def schema_files(self) -> list[Path]:
        """Return paths to raw.plaid_* DDL files bundled with this package."""
        schema_dir = Path(__file__).parent / "schema"
        return sorted(schema_dir.glob("raw_plaid_*.sql"))

    def load(self, sync_data: SyncDataResponse, job_id: str) -> LoadResult:
        """Load accounts, transactions, balances from one sync response.

        Returns per-table counts. Does NOT handle removed_transactions —
        call handle_removed_transactions() separately.
        """
        source_file = f"sync_{job_id}"
        extracted_at = sync_data.metadata.synced_at
        loaded_at = datetime.now(UTC)
        item_by_account = self.build_account_to_item_map(sync_data)
        window_by_item = {
            inst.provider_item_id: inst.transactions_window_start
            for inst in sync_data.metadata.institutions
        }
        self._validate_holdings_windows(sync_data.investment_holdings, window_by_item)

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
        securities_loaded = self._load_securities(
            sync_data.securities, source_file, extracted_at, loaded_at
        )
        investment_transactions_loaded = self._load_investment_transactions(
            sync_data.investment_transactions, source_file, extracted_at, loaded_at
        )
        holdings_loaded, holding_lots_loaded = self._load_investment_holdings(
            sync_data.investment_holdings,
            window_by_item,
            source_file,
            extracted_at,
            loaded_at,
        )
        return LoadResult(
            accounts_loaded=accounts_loaded,
            transactions_loaded=transactions_loaded,
            balances_loaded=balances_loaded,
            securities_loaded=securities_loaded,
            investment_transactions_loaded=investment_transactions_loaded,
            holdings_loaded=holdings_loaded,
            holding_lots_loaded=holding_lots_loaded,
        )

    def _validate_holdings_windows(
        self,
        holdings: list[SyncHolding],
        window_by_item: dict[str, date | None],
    ) -> None:
        """Raise before any table is ingested if a holdings item lacks its window.

        Called at the top of load(), before the first ingest, so a bad
        payload fails all-or-nothing — no partial raw write and no inflated
        records-loaded counter from tables ingested ahead of holdings.
        """
        for holding in holdings:
            if window_by_item.get(holding.provider_item_id) is None:
                raise ValueError(
                    "metadata institution result for item "
                    f"{holding.provider_item_id} is missing transactions_window_start; "
                    "the opening-lot bootstrap cannot classify lots without it"
                )

    def build_account_to_item_map(self, sync_data: SyncDataResponse) -> dict[str, str]:
        """Map each account_id to its provider_item_id (its ``source_origin``).

        Public so the sync service can attribute each account to the same
        ``source_origin`` this loader stamps on ``raw.plaid_*`` when it
        populates ``app.account_links`` (the staging JOIN keys on that scope).

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

    def _load_securities(
        self,
        securities: list[SyncSecurity],
        source_file: str,
        extracted_at: datetime,
        loaded_at: datetime,
    ) -> int:
        if not securities:
            return 0
        df = pl.DataFrame(
            [
                {
                    **sec.model_dump(exclude={"provider_item_id"}),
                    "source_file": source_file,
                    "source_type": "plaid",
                    "source_origin": sec.provider_item_id,
                    "extracted_at": extracted_at,
                    "loaded_at": loaded_at,
                }
                for sec in securities
            ],
            schema=_SECURITIES_SCHEMA,
        )
        self.db.ingest_dataframe(PLAID_SECURITIES.full_name, df, on_conflict="upsert")
        SYNC_INVESTMENTS_RECORDS_LOADED.labels(table="plaid_securities").inc(len(df))
        logger.info(f"Loaded {len(df)} Plaid securities")
        return len(df)

    def _load_investment_transactions(
        self,
        transactions: list[SyncInvestmentTransaction],
        source_file: str,
        extracted_at: datetime,
        loaded_at: datetime,
    ) -> int:
        if not transactions:
            return 0
        self._warn_amount_drift(transactions)
        # DO NOT NEGATE amount here. Plaid convention (positive = cash out)
        # is preserved in raw; the flip lives in stg_plaid__investment_transactions.
        df = pl.DataFrame(
            [
                {
                    **txn.model_dump(exclude={"provider_item_id"}),
                    "source_file": source_file,
                    "source_type": "plaid",
                    "source_origin": txn.provider_item_id,
                    "extracted_at": extracted_at,
                    "loaded_at": loaded_at,
                }
                for txn in transactions
            ],
            schema=_INVESTMENT_TRANSACTIONS_SCHEMA,
        )
        self.db.ingest_dataframe(
            PLAID_INVESTMENT_TRANSACTIONS.full_name, df, on_conflict="upsert"
        )
        SYNC_INVESTMENTS_RECORDS_LOADED.labels(
            table="plaid_investment_transactions"
        ).inc(len(df))
        logger.info(f"Loaded {len(df)} Plaid investment transactions")
        return len(df)

    def _load_investment_holdings(
        self,
        holdings: list[SyncHolding],
        window_by_item: dict[str, date | None],
        source_file: str,
        extracted_at: datetime,
        loaded_at: datetime,
    ) -> tuple[int, int]:
        if not holdings:
            return (0, 0)
        holding_rows: list[dict[str, object]] = []
        lot_rows: list[dict[str, object]] = []
        for holding in holdings:
            # _validate_holdings_windows() already confirmed every item here
            # has a window before load() ingested anything.
            window_start = window_by_item[holding.provider_item_id]
            holding_rows.append({
                **holding.model_dump(exclude={"provider_item_id", "tax_lots"}),
                "holdings_date": extracted_at.date(),
                "transactions_window_start": window_start,
                "source_file": source_file,
                "source_type": "plaid",
                "source_origin": holding.provider_item_id,
                "extracted_at": extracted_at,
                "loaded_at": loaded_at,
            })
            for lot_index, lot in enumerate(holding.tax_lots):
                lot_rows.append({
                    "account_id": holding.account_id,
                    "security_id": holding.security_id,
                    "lot_index": lot_index,
                    **lot.model_dump(),
                    "source_file": source_file,
                    "source_type": "plaid",
                    "source_origin": holding.provider_item_id,
                    "extracted_at": extracted_at,
                    "loaded_at": loaded_at,
                })
        df_holdings = pl.DataFrame(holding_rows, schema=_INVESTMENT_HOLDINGS_SCHEMA)
        self.db.ingest_dataframe(
            PLAID_INVESTMENT_HOLDINGS.full_name, df_holdings, on_conflict="upsert"
        )
        SYNC_INVESTMENTS_RECORDS_LOADED.labels(table="plaid_investment_holdings").inc(
            len(df_holdings)
        )
        lots_loaded = 0
        if lot_rows:
            df_lots = pl.DataFrame(lot_rows, schema=_INVESTMENT_HOLDING_LOTS_SCHEMA)
            self.db.ingest_dataframe(
                PLAID_INVESTMENT_HOLDING_LOTS.full_name, df_lots, on_conflict="upsert"
            )
            SYNC_INVESTMENTS_RECORDS_LOADED.labels(
                table="plaid_investment_holding_lots"
            ).inc(len(df_lots))
            lots_loaded = len(df_lots)
        logger.info(
            f"Loaded {len(df_holdings)} Plaid holdings rows, {lots_loaded} tax lots"
        )
        return len(df_holdings), lots_loaded

    def _warn_amount_drift(self, transactions: list[SyncInvestmentTransaction]) -> None:
        """Count buy/sell rows failing |amount| ~ |q*p| under BOTH fee conventions.

        GOLDEN-GATED: Sandbox goldens settle whether Plaid amount is
        fee-inclusive; until then the staging flip assumes inclusive and this
        guard makes violations visible (log + metric, never a load failure).

        Per-row tolerance is a cent plus half the unit-in-last-place of
        Plaid's reported `price`, scaled by `quantity` — the largest
        rounding error the wire price's own precision can hide. A price
        rounded to 2dp on a 10,000-share position can hide up to ~$50 of
        true gross; an absolute-cent tolerance would flag that row as
        drifted even though the reconciliation is sound. Whoever settles
        the fee-convention question from Sandbox goldens should read this
        as "reconciles within reported-price rounding," not "reconciles
        exactly."

        `INVESTMENT_AMOUNT_DRIFT_ROWS_TOTAL` counts drift OCCURRENCES per
        load() call, not distinct drifted rows — replaying the same job
        re-counts the same row every time.
        """
        drifted = 0
        for txn in transactions:
            if txn.investment_transaction_type not in ("buy", "sell"):
                continue
            if txn.quantity is None or txn.price is None:
                continue
            gross = abs(txn.quantity * txn.price)
            exponent = txn.price.as_tuple().exponent
            if not isinstance(exponent, int):
                # Non-int exponent means a non-finite Decimal (NaN/Inf).
                # SyncInvestmentTransaction.price has no allow_inf_nan=False,
                # so a malformed payload could reach here; skip it from
                # drift-checking only -- the row itself still loads.
                continue
            ulp = Decimal(1).scaleb(exponent)
            tolerance = Decimal("0.01") + abs(txn.quantity) * ulp / 2
            fees = txn.fees or Decimal("0")
            amount = abs(txn.amount)
            candidates = (gross, gross + fees, gross - fees)
            if all(abs(amount - c) > tolerance for c in candidates):
                drifted += 1
        if drifted:
            INVESTMENT_AMOUNT_DRIFT_ROWS_TOTAL.inc(drifted)
            logger.warning(
                f"{drifted} Plaid investment transaction(s) failed amount "
                "reconciliation under both fee conventions"
            )

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
