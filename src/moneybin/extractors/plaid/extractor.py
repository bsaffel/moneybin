"""Load Plaid sync JSON into raw.plaid_* DuckDB tables.

Sign convention: raw tables preserve Plaid's native convention
(positive = expense). The -1 * amount flip is done EXCLUSIVELY in
prep.stg_plaid__transactions. See docs/specs/2026-05-13-plaid-sync-design.md
Section 5 — flipping anywhere else silently corrupts cross-source
aggregations.
"""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from itertools import chain
from pathlib import Path

import polars as pl

from moneybin.connectors.sync_models import (
    InstitutionResult,
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
    PRICE_ROWS_WRITTEN_TOTAL,
    SYNC_INVESTMENTS_RECORDS_LOADED,
)
from moneybin.tables import (
    PLAID_INVESTMENT_HOLDING_LOTS,
    PLAID_INVESTMENT_HOLDINGS,
    PLAID_INVESTMENT_HOLDINGS_SNAPSHOTS,
    PLAID_INVESTMENT_TRANSACTIONS,
    PLAID_SECURITIES,
    SECURITY_PRICES,
)

logger = logging.getLogger(__name__)


def _utc_naive(value: datetime | None) -> datetime | None:
    """Rebase an instant onto UTC and drop the tzinfo.

    The raw.plaid_* datetime columns are naive ``TIMESTAMP``, and DuckDB
    rebases a tz-AWARE value into the machine's session zone on insert — so a
    tz-aware column would store the LOCAL wall clock, and every ``::DATE``
    staging derives from it (``trade_date``, ``acquisition_date``) would be the
    local calendar date, off by a day outside UTC. Storing the UTC wall clock
    makes those casts the UTC calendar date on any machine, and keeps them on
    the same calendar as ``holdings_date`` (also a UTC date, below) — a
    mismatch between the two synthesizes phantom opening lots.

    A naive input is taken as already-UTC: the sync server's contract is UTC.
    """
    if value is None or value.tzinfo is None:
        return value
    return value.astimezone(UTC).replace(tzinfo=None)


def _utc_date(value: datetime) -> date:
    """The instant's UTC calendar date — never the machine's or the offset's."""
    if value.tzinfo is None:
        return value.date()
    return value.astimezone(UTC).date()


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
    security_prices_loaded: int = 0
    # A receipt is a durable raw write like any other, and on a liquidated
    # broker's pull it is the ONLY one — so callers gating refresh on "did
    # anything change" must be able to see it.
    holdings_snapshots_loaded: int = 0


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
    "iso_currency_code": pl.Utf8,
    "unofficial_currency_code": pl.Utf8,
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

_SECURITY_PRICES_SCHEMA = pl.Schema({
    "provider_security_key": pl.Utf8,
    "price_date": pl.Date,
    "quote_currency": pl.Utf8,
    "source_type": pl.Utf8,
    "source_origin": pl.Utf8,
    "close": pl.Decimal(28, 10),
    "price_basis": pl.Utf8,
    "extracted_at": pl.Datetime(time_zone="UTC"),
    "loaded_at": pl.Datetime(time_zone="UTC"),
})

_INVESTMENT_TRANSACTIONS_SCHEMA = pl.Schema({
    "investment_transaction_id": pl.Utf8,
    "account_id": pl.Utf8,
    "security_id": pl.Utf8,
    "transaction_date": pl.Date,
    # Naive on purpose — carries a UTC wall clock via _utc_naive(). A tz-aware
    # dtype here would make DuckDB rebase the instant into the machine's local
    # zone on insert (the raw column is naive TIMESTAMP), misdating trade_date.
    "transaction_datetime": pl.Datetime("us"),
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

_INVESTMENT_HOLDINGS_SNAPSHOTS_SCHEMA = pl.Schema({
    "source_origin": pl.Utf8,
    "source_file": pl.Utf8,
    "holdings_date": pl.Date,
    "holdings_count": pl.Int32,
    "transactions_window_start": pl.Date,
    "source_type": pl.Utf8,
    "extracted_at": pl.Datetime(time_zone="UTC"),
    "loaded_at": pl.Datetime(time_zone="UTC"),
})

_INVESTMENT_HOLDING_LOTS_SCHEMA = pl.Schema({
    "account_id": pl.Utf8,
    "security_id": pl.Utf8,
    "lot_index": pl.Int32,
    "institution_lot_id": pl.Utf8,
    # Naive on purpose — see _utc_naive() and _INVESTMENT_TRANSACTIONS_SCHEMA.
    "original_purchase_datetime": pl.Datetime("us"),
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
        security_prices_loaded = self._load_security_prices(
            sync_data.securities, extracted_at, loaded_at
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
        # Deliberately NOT inside _load_investment_holdings' `if not holdings`
        # early return: the receipt's entire purpose is the pull where holdings
        # is EMPTY (see below).
        holdings_snapshots_loaded = self._load_holdings_snapshots(
            sync_data.investment_holdings,
            sync_data.metadata.institutions,
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
            holdings_snapshots_loaded=holdings_snapshots_loaded,
            security_prices_loaded=security_prices_loaded,
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
        # Only the null-window case is reachable: build_account_to_item_map runs
        # first and rejects any holding whose item disagrees with its account's,
        # so by here every provider_item_id is one metadata reported.
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

        Also raises if any data array — `transactions`, `balances`,
        `investment_transactions` or `investment_holdings` — references an
        `account_id` not present in `sync_data.accounts`. Eventual-consistency on
        Plaid's side surfaces this occasionally, and a KeyError during the per-row
        dict lookup leaves no useful context. Loud and explicit is better.

        The investment arrays need the guard even though they stamp `source_origin`
        from their own `provider_item_id` (never this mapping): an account missing
        from `sync_data.accounts` gets no `app.account_links` row, so staging's
        `COALESCE(al.account_id, r.account_id)` falls back to the raw Plaid id and
        `core.fct_investment_transactions` / `core.dim_holdings` carry an
        `account_id` with no `core.dim_accounts` row.
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

        referenced = (
            {txn.account_id for txn in sync_data.transactions}
            | {bal.account_id for bal in sync_data.balances}
            | {itx.account_id for itx in sync_data.investment_transactions}
            | {hold.account_id for hold in sync_data.investment_holdings}
        )
        orphans = referenced - mapping.keys()
        if orphans:
            raise ValueError(
                f"sync payload references account_id(s) not present in "
                f"sync_data.accounts: {sorted(orphans)}. This typically indicates "
                f"eventual-consistency drift on the server — retry the sync, and "
                f"if it persists, the server's account_id stream is out of sync "
                f"with its transaction stream."
            )

        # Investment rows carry their OWN provider_item_id and stamp source_origin
        # from it, while app.account_links is stamped from this mapping. Staging
        # joins the two on (source_origin, account_id), so a row whose item
        # disagrees with its account's item misses that join, COALESCE falls back
        # to the raw Plaid account_id, and the fact/holding lands under an
        # account_id that has no core.dim_accounts row when that account was
        # cross-source merged. The mismatch is silent in core, so catch it here.
        for row, kind in chain(
            (
                (itx, "investment transaction")
                for itx in sync_data.investment_transactions
            ),
            ((hold, "investment holding") for hold in sync_data.investment_holdings),
        ):
            expected = mapping[row.account_id]
            if row.provider_item_id != expected:
                raise ValueError(
                    f"{kind} for account {row.account_id} carries provider_item_id "
                    f"{row.provider_item_id!r}, but that account belongs to item "
                    f"{expected!r} per sync_data.accounts. Attributing the row to "
                    f"its stated item would orphan it from its account — retry the "
                    f"sync, and if it persists the server's item attribution is "
                    f"inconsistent across arrays."
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

    def _load_security_prices(
        self,
        securities: list[SyncSecurity],
        extracted_at: datetime,
        loaded_at: datetime,
    ) -> int:
        """Append the security-level close to the durable price history.

        Runs beside the securities upsert rather than reading raw.plaid_securities
        later: that table is keyed (security_id, source_origin) and upserted, so each
        pull overwrites the previous close_price in place. Only the extractor sits
        between the payload and that overwrite.
        """
        rows = [
            {
                "provider_security_key": sec.security_id,
                "price_date": sec.close_price_as_of,
                "quote_currency": sec.iso_currency_code or sec.unofficial_currency_code,
                "source_type": "plaid",
                "source_origin": sec.provider_item_id,
                "close": sec.close_price,
                "price_basis": "raw",
                "extracted_at": extracted_at,
                "loaded_at": loaded_at,
            }
            for sec in securities
            # Silent by design: a null, zero, negative, dateless, or currency-less quote
            # means Plaid never served a usable price for this security (it sends null,
            # not 0, for "no price"), so there is nothing to lose. Contrast the
            # rounds-to-zero warning below, which drops a valid price we DID receive.
            if sec.close_price is not None
            and sec.close_price > 0
            and sec.close_price_as_of is not None
            and (sec.iso_currency_code or sec.unofficial_currency_code)
        ]
        if not rows:
            return 0
        df = pl.DataFrame(rows, schema=_SECURITY_PRICES_SCHEMA)
        # The pre-conversion guard above sees pydantic's unbounded Decimal; the
        # column is DECIMAL(28,10), so a quote below 1e-10 rounds to zero HERE,
        # silently and without error. Writing it would be permanent: the table
        # is append-only with on_conflict='ignore', so a zero row squats on its
        # primary key and every later corrected close for that date is dropped.
        priced = df.filter(pl.col("close") > 0)
        rounded_away = df.height - priced.height
        if rounded_away:
            logger.warning(
                f"Skipped {rounded_away} security price observation(s) whose "
                "quote rounds to zero at the stored scale (10 decimal places); "
                "those positions stay unpriced"
            )
        if priced.is_empty():
            return 0
        # Append-only: keep the observation already stored for this key.
        written = self.db.ingest_dataframe(
            SECURITY_PRICES.full_name, priced, on_conflict="ignore"
        )
        # Rows WRITTEN, never rows offered: Plaid re-reports the same
        # (security_id, close_price_as_of) on every pull until the close date
        # advances, so counting the batch would make this counter climb
        # steadily through a fully stalled upstream feed — the one condition it
        # exists to expose.
        PRICE_ROWS_WRITTEN_TOTAL.labels(source_type="plaid").inc(written)
        logger.info(f"Loaded {written} security price observations")
        return written

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
                    "transaction_datetime": _utc_naive(txn.transaction_datetime),
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
                # UTC calendar date of the snapshot instant — never the
                # machine's local date. int_plaid__opening_positions compares
                # this against trade_date, which SQL derives as a UTC date from
                # the UTC-wall-clock columns; different calendars there push a
                # snapshot-day buy outside the in-window net and synthesize a
                # phantom opening lot.
                "holdings_date": _utc_date(extracted_at),
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
                    "original_purchase_datetime": _utc_naive(
                        lot.original_purchase_datetime
                    ),
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

    def _load_holdings_snapshots(
        self,
        holdings: list[SyncHolding],
        institutions: list[InstitutionResult],
        source_file: str,
        extracted_at: datetime,
        loaded_at: datetime,
    ) -> int:
        """Record that each item's holdings were fetched — EVEN WHEN it reported none.

        raw.plaid_investment_holdings stores holding ROWS, so it cannot tell
        "this item reported nothing (every position sold)" from "this item never
        reported." An item whose pull returns an empty holdings array writes no
        rows at all, so a newest-snapshot join keyed on those rows silently
        keeps the last NON-EMPTY snapshot from an earlier pull — and the
        fully-liquidated broker, the largest possible net-worth overstatement,
        reads as "still holding the old positions." This receipt is the missing
        evidence, and it is why the write sits outside the holdings loop and
        outside any `if holdings:` guard.

        An item is treated as having reported iff the server declared an
        investments window for it (`transactions_window_start`, its
        /investments/transactions/get start boundary) on a `completed` result —
        the only signal on the wire that the server ran the item's investments
        flow at all. A cash-only item (no window) never reported; neither did a
        FAILED item, and a receipt for either would falsely claim the broker
        was asked and answered "nothing," flagging every lot there as a phantom.
        Items that DID deliver holdings are unioned in unconditionally, which
        holds the invariant every newest-snapshot consumer depends on: no
        holdings row exists whose (source_origin, source_file) has no receipt.
        """
        reported_items = {
            inst.provider_item_id
            for inst in institutions
            if inst.status == "completed" and inst.transactions_window_start is not None
        } | {holding.provider_item_id for holding in holdings}
        if not reported_items:
            return 0
        # Every reported item has a window: the first set is filtered on it, and
        # _validate_holdings_windows already raised for any item in the second
        # set that lacks one. That is what lets the column be NOT NULL.
        window_by_item = {
            inst.provider_item_id: inst.transactions_window_start
            for inst in institutions
            if inst.transactions_window_start is not None
        }
        counts = Counter(holding.provider_item_id for holding in holdings)
        df = pl.DataFrame(
            [
                {
                    "source_origin": item_id,
                    "source_file": source_file,
                    # Same UTC-date derivation as the holdings rows this
                    # accounts for — one calendar, never the machine's local one.
                    "holdings_date": _utc_date(extracted_at),
                    "holdings_count": counts[item_id],
                    "transactions_window_start": window_by_item[item_id],
                    "source_type": "plaid",
                    "extracted_at": extracted_at,
                    "loaded_at": loaded_at,
                }
                for item_id in sorted(reported_items)
            ],
            schema=_INVESTMENT_HOLDINGS_SNAPSHOTS_SCHEMA,
        )
        self.db.ingest_dataframe(
            PLAID_INVESTMENT_HOLDINGS_SNAPSHOTS.full_name, df, on_conflict="upsert"
        )
        SYNC_INVESTMENTS_RECORDS_LOADED.labels(
            table="plaid_investment_holdings_snapshots"
        ).inc(len(df))
        logger.info(f"Recorded {len(df)} Plaid holdings-snapshot receipt(s)")
        return len(df)

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
