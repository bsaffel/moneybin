"""SQLMesh Python model for core.fct_balances_daily.

One row per account per day, from the account's first observation (or, with no
observation, its first evidence of holding value) to the spine end: the newest
date of any evidence in the profile, whether an observation, a transaction, a
trade, or a broker-reported position. Observed days carry the observed balance;
between observations the last balance is carried forward, adjusted by the
transactions in its currency, and each observed day records its reconciliation
delta.

Every account runs to the same end. Ending each account at its own last
observation silently dropped it from every cross-account aggregate after that
date: reports.net_worth sums the accounts present on a date and takes the latest
one, so an account whose statement landed a week before another's vanished from
net worth. The end is the newest evidence, not the newest observation, because a
profile holding only CSV transactions has no observation at all, and an
observation-only end gives it no rows and the guard below nothing to see. The
end is never capped at today: a FULL model must not depend on the day it was
built, so a future-dated transaction extends the spine.

An account with evidence of holding value and no balance observation gets rows
too, from its first evidence to the spine end, with balance NULL: it holds
something and its balance is unknown. Every net-worth reader counts those rows
and withholds its total while any is present, rather than letting SUM() skip
them (net-worth-guard-on-daily-balances.md Requirement 5).

in_net_worth answers "does this account count toward net worth on this date",
from core.dim_accounts, once, so no reader restates the rule. It is stored per
row only because this model is kind FULL: every refresh recomputes it from
current settings. If this model ever becomes incremental, move the flag to read
time first, or an `accounts set` change will stop reaching rows already built.

An account that is genuinely gone should be excluded by archiving it
(`app.account_settings.archived` / `include_in_net_worth`), not by silently
ageing out of the spine.

Per-account precedence within a single date (most authoritative wins):
  user assertion > ofx/plaid/simplefin snapshot > tabular running balance

This is the only Python SQLMesh model in the project. The carry-forward logic
cannot be expressed in SQL without recursive CTEs, which SQLMesh/DuckDB support
but which would be harder to read and test than the equivalent Python walk.
"""

from __future__ import annotations

import math
import typing as t
from collections.abc import Iterator
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pyarrow as pa
from sqlmesh import (  # type: ignore[import-untyped] — sqlmesh has no type stubs
    ExecutionContext,
    model,
)

_SOURCE_PRECEDENCE = {
    "assertion": 3,
    "ofx": 2,
    "plaid": 2,
    "simplefin": 2,
    "tabular": 1,
}


def _to_decimal(value: object, default: Decimal = Decimal("0")) -> Decimal:
    """Convert pandas float64 (from fetchdf) to Decimal.

    fetchdf() returns DECIMAL columns as float64; Decimal(str()) recovers the
    value faithfully for personal-finance-scale amounts (float64 precision is
    adequate below ~$10B per the project's amount range).
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return default
    return Decimal(str(value))


def _to_currency(value: object) -> str | None:
    """Normalise a pandas currency cell to a hashable str | None.

    Unlike ``_to_decimal``, whose NaN branch is load-bearing (fetchdf renders an
    all-NULL DECIMAL column as float64, so its NULLs really do arrive as NaN),
    a VARCHAR column comes back as object dtype with None even when every value
    is NULL — so the NaN check here is defensive, not a live path.

    It stays because the failure it prevents is silent and expensive: NaN !=
    NaN, so an un-normalised NaN would never match the carried currency, and
    every unknown-currency account would have its reconciliation voided as a
    phantom currency change. One isinstance check is cheap insurance against a
    future reshape (a reindex or merge over this frame) introducing NaN.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return str(value)


def _select_winning_observations(group: pd.DataFrame) -> pd.DataFrame:
    """Reduce multiple observations for one account to one winner per date.

    Winner per balance_date: highest source precedence, then freshest observation,
    then source_type ascending as a final key. Here updated_at is the observation
    timestamp, excluding classification edits. The updated_at + source_type keys
    make the ofx/plaid/simplefin precedence tie deterministic — all are institution
    snapshots at the same tier, so without a tiebreak the winner would depend on
    pandas' (unstable) sort order.
    """
    group = group.copy()  # type: ignore[reportUnknownMemberType]
    group["_priority"] = (  # type: ignore[reportUnknownMemberType]
        group["source_type"]  # type: ignore[reportUnknownMemberType]
        .map(_SOURCE_PRECEDENCE)  # type: ignore[reportUnknownMemberType, reportUnknownArgumentType] — dict is valid for Series.map
        .fillna(0)  # type: ignore[reportUnknownMemberType]
    )
    return (
        group
        .sort_values(  # type: ignore[reportUnknownMemberType]
            ["balance_date", "_priority", "updated_at", "source_type"],
            ascending=[True, False, False, True],
        )
        .drop_duplicates(subset=["balance_date"], keep="first")
        .reset_index(drop=True)
    )


def _to_date(value: object) -> date | None:
    """Normalise a fetchdf DATE cell (Timestamp, date, NaT or None) to date | None."""
    if value is None or pd.isna(value):  # type: ignore[arg-type]  # pandas stubs reject object
        return None
    return pd.Timestamp(value).date()  # type: ignore[arg-type]  # pandas stubs reject object


def _counts_through(include: object, archived: object, archived_at: object) -> date:
    """The last balance_date on which an account counts toward net worth.

    date.min (never counts) when the account is excluded, or archived with no
    known date (V063 left undated legacy archives NULL, and they stay excluded
    on every date). Its archive date when archived on a known date. date.max
    (no end) otherwise.

    The sentinels are spelled inline: SQLMesh serialises every module global
    this model reaches, and it cannot serialise a date instance.
    """
    if not include:
        return date.min
    if archived:
        return _to_date(archived_at) or date.min
    return date.max


def _evidence_sql(
    transactions: str, investment_transactions: str, broker_reported: str
) -> str:
    """Per account, the first and last date of any evidence of holding value.

    Evidence is a row on either ledger or a broker-reported nonzero position.
    Open lots need no arm: core.fct_investment_lots is built only from
    core.fct_investment_transactions and keeps each row's account_id.
    """
    return f"""
        SELECT account_id, MIN(d) AS first_date, MAX(d) AS last_date
        FROM (
            SELECT account_id, transaction_date AS d FROM {transactions}
            UNION ALL
            SELECT account_id, trade_date AS d FROM {investment_transactions}
            UNION ALL
            SELECT account_id, as_of AS d FROM {broker_reported} WHERE has_position
        )
        WHERE account_id IS NOT NULL AND d IS NOT NULL
        GROUP BY account_id
    """  # noqa: S608  # table names from context.resolve_table(), not user input


def _walk_observed_account(
    account_id: str, group: pd.DataFrame, txns: pd.DataFrame, spine_end: date
) -> list[dict[str, t.Any]]:
    """The carry-forward walk for one account with observations, to spine_end."""
    rows: list[dict[str, t.Any]] = []
    winners: pd.DataFrame = _select_winning_observations(group)

    first_date = winners["balance_date"].min()  # type: ignore[reportUnknownMemberType]
    last_date = spine_end
    # pd.date_range returns Timestamps; .date converts to Python date objects,
    # which DuckDB maps to the DATE type without ambiguity.
    spine: list[date] = list(
        pd.date_range(first_date, last_date, freq="D").date  # type: ignore[reportUnknownMemberType, reportAttributeAccessIssue] — .date exists at runtime
    )

    # fetchdf() returns DATE columns as datetime64[us] Timestamps, not Python
    # date objects. Normalise to date so .get(d) matches spine elements.
    acct_txns_df = txns[txns["account_id"] == account_id].copy()  # type: ignore[reportUnknownMemberType]
    # date -> {currency_code: net_amount}, so the walk below can apply only
    # the transactions denominated in the currency it is currently carrying.
    acct_txns: dict[date, dict[str | None, object]] = {}
    if not acct_txns_df.empty:  # type: ignore[reportUnknownMemberType]
        acct_txns_df["d"] = pd.to_datetime(acct_txns_df["d"]).dt.date  # type: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        for txn_date, txn_currency, net_amount in zip(  # type: ignore[reportUnknownVariableType]
            acct_txns_df["d"],  # type: ignore[reportUnknownArgumentType]
            acct_txns_df["currency_code"],  # type: ignore[reportUnknownArgumentType]
            acct_txns_df["net_amount"],  # type: ignore[reportUnknownArgumentType]
            strict=True,
        ):
            bucket = acct_txns.setdefault(t.cast(date, txn_date), {})
            bucket[_to_currency(txn_currency)] = net_amount

    # Normalise balance_date to Python date objects so lookups match spine elements
    # regardless of whether fetchdf returns date or Timestamp.
    winners["balance_date"] = (  # type: ignore[reportUnknownMemberType]
        pd.to_datetime(winners["balance_date"]).dt.date  # type: ignore[reportUnknownMemberType, reportUnknownArgumentType]
    )
    observed_lookup: pd.DataFrame = winners.set_index("balance_date")[  # type: ignore[reportUnknownMemberType]
        ["balance", "source_type", "currency_code"]
    ]

    carry: Decimal | None = None
    carry_currency: str | None = None
    for d in spine:
        # Only the movement denominated in the carried currency. A
        # transaction in any other currency needs an FX rate to become an
        # amount of this balance, and MoneyBin has none until M1K.2 — it is
        # left out rather than added as though the units matched, and
        # resurfaces as reconciliation_delta at the next observation.
        txn_adj = _to_decimal(acct_txns.get(d, {}).get(carry_currency))

        if d in observed_lookup.index:  # type: ignore[reportUnknownMemberType]
            obs_balance = _to_decimal(observed_lookup.loc[d, "balance"])  # type: ignore[reportUnknownMemberType, reportUnknownArgumentType] — pandas .loc stubs return Unknown; safe at runtime
            obs_source: str = str(observed_lookup.loc[d, "source_type"])  # type: ignore[reportUnknownMemberType]
            obs_currency = _to_currency(observed_lookup.loc[d, "currency_code"])  # type: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            delta: Decimal | None
            if carry is None:
                # First observation: no prior carry, so delta is undefined.
                delta = None
            elif obs_currency != carry_currency:
                # The carry is denominated in the previous observation's
                # currency. Subtracting it from an observation in a different
                # one yields a number in no unit at all — and the row is
                # labelled with the *new* currency, so balance_drift's
                # currency_mismatch guard (account vs row) sees two agreeing
                # codes and passes it. Undefined until conversion (M1K.2);
                # balance_drift already renders a NULL delta as `no-data`.
                delta = None
            else:
                delta = obs_balance - (carry + txn_adj)
            rows.append({
                "account_id": account_id,
                "balance_date": d,
                "balance": obs_balance,
                "is_observed": True,
                "observation_source": obs_source,
                "reconciliation_delta": delta,
                "currency_code": obs_currency,
            })
            carry = obs_balance
            carry_currency = obs_currency
        else:
            assert carry is not None, (  # noqa: S101  # invariant, not user input
                "interpolated branch reached before first observation — "
                "spine should always start at the first observed date"
            )
            carry = carry + txn_adj
            rows.append({
                "account_id": account_id,
                "balance_date": d,
                "balance": carry,
                "is_observed": False,
                "observation_source": None,
                "reconciliation_delta": None,
                "currency_code": carry_currency,
            })
    return rows


def _unanchored_rows(
    account_id: str, first: date, spine_end: date, currency: str | None
) -> list[dict[str, t.Any]]:
    """One NULL-balance row per day for an account with evidence and no observation."""
    return [
        {
            "account_id": account_id,
            "balance_date": d,
            "balance": None,
            "is_observed": False,
            "observation_source": None,
            "reconciliation_delta": None,
            "currency_code": currency,
        }
        for d in pd.date_range(first, spine_end, freq="D").date  # type: ignore[reportUnknownMemberType, reportAttributeAccessIssue] — .date exists at runtime
    ]


def _build_spine_rows(
    obs: pd.DataFrame,
    txns: pd.DataFrame,
    evidence: pd.DataFrame,
    accounts: pd.DataFrame,
) -> list[dict[str, t.Any]]:
    """Every spine row, anchored and unanchored, each stamped with in_net_worth.

    obs: account_id, balance_date, balance, source_type, updated_at, currency_code.
    txns: account_id, d, currency_code, net_amount.
    evidence: account_id, first_date, last_date (see _evidence_sql).
    accounts: account_id, include_in_net_worth, archived, archived_at, currency_code.
    """
    ends = [
        d
        for d in (
            *(_to_date(v) for v in obs["balance_date"]),  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs
            *(_to_date(v) for v in evidence["last_date"]),  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs
        )
        if d is not None
    ]
    if not ends:
        return []
    spine_end = max(ends)

    counts_through: dict[str, date] = {}
    account_currency: dict[str, str | None] = {}
    for account_id, include, archived, archived_at, currency in zip(  # type: ignore[reportUnknownVariableType]  # zip over Series unpacks to Unknown
        accounts["account_id"],  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs
        accounts["include_in_net_worth"],  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs
        accounts["archived"],  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs
        accounts["archived_at"],  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs
        accounts["currency_code"],  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs
        strict=True,
    ):
        key = str(account_id)  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs
        counts_through[key] = _counts_through(include, archived, archived_at)  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs
        account_currency[key] = _to_currency(currency)  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs

    rows: list[dict[str, t.Any]] = []
    anchored: set[str] = set()
    for account_id, group in obs.groupby("account_id"):  # type: ignore[reportUnknownMemberType] — pandas stubs are incomplete
        anchored.add(str(account_id))
        rows.extend(_walk_observed_account(str(account_id), group, txns, spine_end))
    for account_id, first_date in zip(  # type: ignore[reportUnknownVariableType]  # zip over Series unpacks to Unknown
        evidence["account_id"],  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs
        evidence["first_date"],  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs
        strict=True,
    ):
        key = str(account_id)  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs
        first = _to_date(first_date)  # type: ignore[reportUnknownArgumentType]  # Series cells are Unknown to the stubs
        if key in anchored or first is None:
            continue
        rows.extend(_unanchored_rows(key, first, spine_end, account_currency.get(key)))

    for row in rows:
        # An account_id with no core.dim_accounts row never counts, which is
        # what the rungs' former INNER JOIN to dim_accounts did.
        row["in_net_worth"] = row["balance_date"] <= counts_through.get(
            row["account_id"], date.min
        )
    return rows


@model(
    "core.fct_balances_daily",
    kind="FULL",
    # context.fetchdf() SQL strings are opaque to SQLMesh's dependency scanner
    # — it cannot infer these from the string literals. Declaring them here
    # ensures SQLMesh materialises every input before this model executes.
    depends_on={
        "core.fct_balances",
        "prep.stg_simplefin__balances",
        "core.fct_transactions",
        "core.fct_investment_transactions",
        "core.dim_holdings_broker_reported",
        "prep.int_accounts__merged",
    },
    columns={
        "account_id": "VARCHAR",
        "observation_source": "VARCHAR",
        "currency_code": "VARCHAR",
        "balance": "DECIMAL(18, 2)",
        "reconciliation_delta": "DECIMAL(18, 2)",
        "is_observed": "BOOLEAN",
        "in_net_worth": "BOOLEAN",
        "balance_date": "DATE",
    },
    column_descriptions={
        "account_id": "Foreign key to core.dim_accounts.account_id",
        "observation_source": "source_type of the winning observation (ofx, tabular, assertion, plaid, simplefin); NULL if interpolated or the account has no balance observation",
        "currency_code": "ISO 4217 currency code, carried forward from the winning observation (or its interpolated predecessor) on each day; core.dim_accounts.currency_code when the account has no balance observation",
        "balance": "Balance as of end of this day; NULL when the account holds value (a transaction, a trade, or a broker-reported position) but has no balance observation. Count those rows before trusting a SUM: SUM() skips them silently",
        "reconciliation_delta": "Difference between observed and transaction-derived balance; NULL on interpolated days, on the first observation, and when the account has no balance observation",
        "is_observed": "TRUE if an authoritative observation exists for this date",
        "in_net_worth": "TRUE when this account counts toward net worth on this date: included in net worth, and not archived or archived on or after this date. Recomputed from the account's settings on every rebuild, and rebuilt whenever an archive or net-worth setting changes",
        "balance_date": "Calendar date",
    },
    description=(
        "One row per account per day, from the account's first observation (or "
        "first evidence of holding value) to the newest evidence date in the "
        "profile, so every account carries its last known balance on the latest "
        "date. Observed days use the most authoritative source; gaps carry the "
        "last balance forward, adjusted by transactions from core.fct_transactions. "
        "An account holding value with no balance observation has NULL-balance "
        "rows. in_net_worth says whether the row counts toward net worth. "
        "Self-heals on every sqlmesh run."
    ),
)
def execute(
    context: ExecutionContext,
    start: datetime,  # FULL kind ignores start/end
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> Iterator[pd.DataFrame]:
    """Build the per-account daily balance spine with carry-forward and reconciliation deltas."""
    # context.resolve_table() resolves the internal versioned name (e.g.
    # sqlmesh__core.core__fct_balances__<hash>) for the current plan execution.
    # Plain "core.fct_balances" only exists after promotion, not during backfill.
    fct_balances_table = context.resolve_table("core.fct_balances")
    simplefin_balances_table = context.resolve_table("prep.stg_simplefin__balances")
    fct_transactions_table = context.resolve_table("core.fct_transactions")
    investment_table = context.resolve_table("core.fct_investment_transactions")
    broker_table = context.resolve_table("core.dim_holdings_broker_reported")
    accounts_table = context.resolve_table("prep.int_accounts__merged")

    # SimpleFIN staging has one selected observation per account/date. Its receipt
    # time orders evidence; core.updated_at also advances for classification edits.
    obs: pd.DataFrame = context.fetchdf(
        f"""
        SELECT b.account_id, b.balance_date, b.balance, b.source_type,
            CASE WHEN b.source_type = 'simplefin' THEN sf.loaded_at
                 ELSE b.updated_at END AS updated_at,
            b.currency_code
        FROM {fct_balances_table} AS b
        LEFT JOIN {simplefin_balances_table} AS sf
            ON b.source_type = 'simplefin'
            AND sf.account_id = b.account_id
            AND sf.balance_date = b.balance_date
            AND sf.source_file = b.source_ref
        ORDER BY b.account_id, b.balance_date
        """  # noqa: S608  # table name from context.resolve_table(), not user input
    )
    # Grouped by currency_code as well as account/date: fct_transactions resolves
    # currency per row (its own, else the account's), so one account-date can hold
    # amounts in more than one currency. Summing across them would add unlike
    # units into a balance carried in a single currency, and the result is
    # invisible downstream — the row still reports the observation's currency, so
    # reports.balance_drift's currency_mismatch guard (which compares the balance
    # against the account) sees two matching USD values and passes the blend.
    txns: pd.DataFrame = context.fetchdf(
        f"""
        SELECT account_id, transaction_date AS d, currency_code, SUM(amount) AS net_amount
        FROM {fct_transactions_table}
        GROUP BY account_id, transaction_date, currency_code
        """  # noqa: S608  # table name from context.resolve_table(), not user input
    )
    evidence: pd.DataFrame = context.fetchdf(
        _evidence_sql(fct_transactions_table, investment_table, broker_table)
    )
    accounts: pd.DataFrame = context.fetchdf(
        f"""
        SELECT account_id, include_in_net_worth, archived, archived_at, currency_code
        FROM {accounts_table}
        """  # noqa: S608  # table name from context.resolve_table(), not user input
    )

    rows = _build_spine_rows(obs, txns, evidence, accounts)
    if not rows:
        # SQLMesh rejects empty DataFrames from Python models — use the
        # generator protocol instead: yield from () signals "no rows" without
        # triggering the "Cannot construct source query" error.
        yield from ()
        return

    # Build with an explicit pyarrow schema so DuckDB receives DECIMAL(18, 2)
    # columns end-to-end. Without this, DuckDB infers DECIMAL precision from
    # sample values (e.g., DECIMAL(6, 2) from a few small balances) and then
    # fails to cast larger values (a $15k savings balance overflows).
    # Per .claude/rules/database.md: no float for financial quantities — keep
    # Decimal precision through to the engine.
    schema = pa.schema([
        pa.field("account_id", pa.string()),
        pa.field("observation_source", pa.string()),
        pa.field("currency_code", pa.string()),
        pa.field("balance", pa.decimal128(18, 2)),
        pa.field("reconciliation_delta", pa.decimal128(18, 2)),
        pa.field("is_observed", pa.bool_()),
        pa.field("in_net_worth", pa.bool_()),
        pa.field("balance_date", pa.date32()),
    ])
    table = pa.Table.from_pylist(rows, schema=schema)
    yield table.to_pandas(types_mapper=pd.ArrowDtype)  # pyright: ignore[reportUnknownMemberType]  # pyarrow-stubs types to_pandas's categories as a bare list
