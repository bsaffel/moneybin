"""SQLMesh Python model for core.fct_balances_daily.

Per account: build a date spine from first to last observation, carry forward
the last known balance adjusted by intervening transactions, and compute
reconciliation deltas on observed days.

Per-account precedence within a single date (most authoritative wins):
  user assertion > ofx/plaid snapshot > tabular running balance

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

_SOURCE_PRECEDENCE = {"assertion": 3, "ofx": 2, "plaid": 2, "tabular": 1}


def _to_decimal(value: object, default: Decimal = Decimal("0")) -> Decimal:
    """Convert pandas float64 (from fetchdf) to Decimal.

    fetchdf() returns DECIMAL columns as float64; Decimal(str()) recovers the
    value faithfully for personal-finance-scale amounts (float64 precision is
    adequate below ~$10B per the project's amount range).
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return default
    return Decimal(str(value))


@model(
    "core.fct_balances_daily",
    kind="FULL",
    # context.fetchdf() SQL strings are opaque to SQLMesh's dependency scanner
    # — it cannot infer these from the string literals. Declaring them here
    # ensures SQLMesh materialises core.fct_balances and core.fct_transactions
    # before this model executes.
    depends_on={"core.fct_balances", "core.fct_transactions"},
    columns={
        "account_id": "VARCHAR",
        "balance_date": "DATE",
        "balance": "DECIMAL(18, 2)",
        "is_observed": "BOOLEAN",
        "observation_source": "VARCHAR",
        "reconciliation_delta": "DECIMAL(18, 2)",
    },
    column_descriptions={
        "account_id": "Foreign key to core.dim_accounts.account_id",
        "balance_date": "Calendar date",
        "balance": "Balance as of end of this day",
        "is_observed": "TRUE if an authoritative observation exists for this date",
        "observation_source": "source_type of the winning observation (ofx, tabular, assertion, plaid); NULL if interpolated",
        "reconciliation_delta": "Difference between observed and transaction-derived balance; NULL on interpolated days and on the first observation",
    },
    description=(
        "One row per account per day from first observation to last observation. "
        "Observed days use the most authoritative source for that date; gaps are "
        "filled by carrying the last balance forward, adjusted by intervening "
        "transactions from core.fct_transactions. Self-heals on every sqlmesh run."
    ),
)
def execute(
    context: ExecutionContext,
    start: datetime,  # noqa: ARG001 — FULL kind ignores start/end
    end: datetime,  # noqa: ARG001
    execution_time: datetime,  # noqa: ARG001
    **kwargs: t.Any,  # noqa: ARG001
) -> Iterator[pd.DataFrame]:
    """Build the per-account daily balance spine with carry-forward and reconciliation deltas."""
    # context.resolve_table() resolves the internal versioned name (e.g.
    # sqlmesh__core.core__fct_balances__<hash>) for the current plan execution.
    # Plain "core.fct_balances" only exists after promotion, not during backfill.
    fct_balances_table = context.resolve_table("core.fct_balances")
    fct_transactions_table = context.resolve_table("core.fct_transactions")

    obs: pd.DataFrame = context.fetchdf(
        f"""
        SELECT account_id, balance_date, balance, source_type
        FROM {fct_balances_table}
        ORDER BY account_id, balance_date
        """  # noqa: S608  # table name from context.resolve_table(), not user input
    )
    if obs.empty:
        # SQLMesh rejects empty DataFrames from Python models — use the
        # generator protocol instead: yield from () signals "no rows" without
        # triggering the "Cannot construct source query" error.
        yield from ()
        return

    txns: pd.DataFrame = context.fetchdf(
        f"""
        SELECT account_id, transaction_date AS d, SUM(amount) AS net_amount
        FROM {fct_transactions_table}
        GROUP BY account_id, transaction_date
        """  # noqa: S608  # table name from context.resolve_table(), not user input
    )

    rows: list[dict[str, t.Any]] = []
    for account_id, group in obs.groupby("account_id"):  # type: ignore[reportUnknownMemberType] — pandas stubs are incomplete
        group = group.copy()  # type: ignore[reportUnknownMemberType]
        group["_priority"] = (  # type: ignore[reportUnknownMemberType]
            group["source_type"]  # type: ignore[reportUnknownMemberType]
            .map(_SOURCE_PRECEDENCE)  # type: ignore[reportUnknownMemberType, reportUnknownArgumentType] — dict is valid for Series.map
            .fillna(0)  # type: ignore[reportUnknownMemberType]
        )
        winners: pd.DataFrame = (
            group
            .sort_values(  # type: ignore[reportUnknownMemberType]
                ["balance_date", "_priority"], ascending=[True, False]
            )
            .drop_duplicates(subset=["balance_date"], keep="first")
            .reset_index(drop=True)
        )

        first_date = winners["balance_date"].min()  # type: ignore[reportUnknownMemberType]
        last_date = winners["balance_date"].max()  # type: ignore[reportUnknownMemberType]
        # pd.date_range returns Timestamps; .date converts to Python date objects,
        # which DuckDB maps to the DATE type without ambiguity.
        spine: list[date] = list(
            pd.date_range(first_date, last_date, freq="D").date  # type: ignore[reportUnknownMemberType, reportAttributeAccessIssue] — .date exists at runtime
        )

        # fetchdf() returns DATE columns as datetime64[us] Timestamps, not Python
        # date objects. Normalise to date so .get(d) matches spine elements.
        acct_txns_df = txns[txns["account_id"] == account_id].copy()  # type: ignore[reportUnknownMemberType]
        if not acct_txns_df.empty:  # type: ignore[reportUnknownMemberType]
            acct_txns_df["d"] = pd.to_datetime(acct_txns_df["d"]).dt.date  # type: ignore[reportUnknownMemberType, reportUnknownArgumentType]
            acct_txns: pd.Series = acct_txns_df.set_index("d")["net_amount"]  # type: ignore[type-arg,reportUnknownMemberType]
        else:
            acct_txns = pd.Series(dtype=object)

        # Normalise balance_date to Python date objects so lookups match spine elements
        # regardless of whether fetchdf returns date or Timestamp.
        winners["balance_date"] = (  # type: ignore[reportUnknownMemberType]
            pd.to_datetime(winners["balance_date"]).dt.date  # type: ignore[reportUnknownMemberType, reportUnknownArgumentType]
        )
        observed_lookup: pd.DataFrame = winners.set_index("balance_date")[  # type: ignore[reportUnknownMemberType]
            ["balance", "source_type"]
        ]

        carry: Decimal | None = None
        for d in spine:
            txn_raw = acct_txns.get(d) if not acct_txns.empty else None  # type: ignore[call-overload] — Series.get accepts date keys at runtime
            txn_adj = _to_decimal(txn_raw)  # type: ignore[reportUnknownArgumentType] — txn_raw type unknown from pandas stubs

            if d in observed_lookup.index:  # type: ignore[reportUnknownMemberType]
                obs_balance = _to_decimal(observed_lookup.loc[d, "balance"])  # type: ignore[reportUnknownMemberType, reportUnknownArgumentType] — pandas .loc stubs return Unknown; safe at runtime
                obs_source: str = str(observed_lookup.loc[d, "source_type"])  # type: ignore[reportUnknownMemberType]
                delta: Decimal | None
                if carry is not None:
                    delta = obs_balance - (carry + txn_adj)
                else:
                    # First observation: no prior carry, so delta is undefined.
                    delta = None
                rows.append({
                    "account_id": account_id,
                    "balance_date": d,
                    "balance": obs_balance,
                    "is_observed": True,
                    "observation_source": obs_source,
                    "reconciliation_delta": delta,
                })
                carry = obs_balance
            else:
                assert carry is not None, (  # noqa: S101 — invariant, not user input
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
                })

    # Build with an explicit pyarrow schema so DuckDB receives DECIMAL(18, 2)
    # columns end-to-end. Without this, DuckDB infers DECIMAL precision from
    # sample values (e.g., DECIMAL(6, 2) from a few small balances) and then
    # fails to cast larger values (a $15k savings balance overflows).
    # Per .claude/rules/database.md: no float for financial quantities — keep
    # Decimal precision through to the engine.
    schema = pa.schema([
        pa.field("account_id", pa.string()),
        pa.field("balance_date", pa.date32()),
        pa.field("balance", pa.decimal128(18, 2)),
        pa.field("is_observed", pa.bool_()),
        pa.field("observation_source", pa.string()),
        pa.field("reconciliation_delta", pa.decimal128(18, 2)),
    ])
    table = pa.Table.from_pylist(rows, schema=schema)
    yield table.to_pandas(types_mapper=pd.ArrowDtype)
