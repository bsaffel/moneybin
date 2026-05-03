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

import typing as t
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from sqlmesh import (  # type: ignore[import-untyped] — sqlmesh has no type stubs
    ExecutionContext,
    model,
)

_SOURCE_PRECEDENCE = {"assertion": 3, "ofx": 2, "plaid": 2, "tabular": 1}

_EMPTY_COLUMNS = [
    "account_id",
    "balance_date",
    "balance",
    "is_observed",
    "observation_source",
    "reconciliation_delta",
]


@model(
    "core.fct_balances_daily",
    kind="FULL",
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
) -> pd.DataFrame:
    """Build the per-account daily balance spine with carry-forward and reconciliation deltas."""
    obs: pd.DataFrame = context.fetchdf(
        """
        SELECT account_id, balance_date, balance, source_type
        FROM core.fct_balances
        ORDER BY account_id, balance_date
        """
    )
    if obs.empty:
        return pd.DataFrame(columns=_EMPTY_COLUMNS)  # type: ignore[reportArgumentType] — list[str] is valid for DataFrame(columns=)

    txns: pd.DataFrame = context.fetchdf(
        """
        SELECT account_id, transaction_date AS d, SUM(amount) AS net_amount
        FROM core.fct_transactions
        GROUP BY account_id, transaction_date
        """
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

        acct_txns: pd.Series = (  # type: ignore[type-arg] — pd.Series[T] not supported by pandas stubs
            txns[txns["account_id"] == account_id].set_index("d")["net_amount"]  # type: ignore[reportUnknownMemberType]  # type: ignore[reportUnknownMemberType]
            if not txns.empty
            else pd.Series(dtype=object)
        )

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
            txn_adj = Decimal(str(txn_raw)) if txn_raw is not None else Decimal("0")  # type: ignore[reportUnknownArgumentType] — txn_raw type unknown from pandas stubs

            if d in observed_lookup.index:  # type: ignore[reportUnknownMemberType]
                obs_balance = Decimal(str(observed_lookup.loc[d, "balance"]))  # type: ignore[reportUnknownMemberType, reportUnknownArgumentType] — pandas .loc stubs return Unknown; safe at runtime
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
                carry = (carry if carry is not None else Decimal("0")) + txn_adj
                rows.append({
                    "account_id": account_id,
                    "balance_date": d,
                    "balance": carry,
                    "is_observed": False,
                    "observation_source": None,
                    "reconciliation_delta": None,
                })

    return pd.DataFrame(rows)
