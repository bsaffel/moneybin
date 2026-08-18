"""core:large_transactions — top-N transactions with z-score anomaly lens."""

from __future__ import annotations

from typing import Any

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    Binding,
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    report,
)
from moneybin.reports.definitions._shared import LARGE_TXN_ANOMALIES
from moneybin.tables import REPORTS_LARGE_TRANSACTIONS

#: Cut in SQL against the row's own currency, so a conversion invalidates them.
_ORIGINAL_CURRENCY_ANALYTICS = (
    "amount_zscore_account",
    "amount_zscore_category",
    "is_top_100",
)


def _blank_original_currency_analytics(
    rows: list[dict[str, Any]], _currency: str
) -> None:
    """Drop the anomaly lens on a converted read rather than mislabel it.

    Both z-scores standardize ``ABS(amount)`` against the account's or
    category's own median and MAD over its full history, and ``is_top_100``
    ranks against that same population — all computed in SQL, in whatever
    currency each row was recorded in. Conversion prices every row at its own
    ``txn_date`` rate, so the converted amounts are not one scaling of the
    originals: two charges equal at 100 EUR come back as 100 and 200 USD if the
    rate moved between their dates, while carrying the single score they earned
    as equals. ``currency_code`` is relabeled to the target at the same time, so
    the row no longer says which currency its score was measured in.

    Restating them is not something a read can do. The baselines span the
    account's whole history, not the rows returned, and this path resolves only
    from stored rates — repricing that history at per-date rates is exactly the
    provider round trip a report read must never make. So the honest answer is
    the one these columns already give for an account with too little history
    to score: null.

    The row *set* is still ranked and filtered by original-currency magnitude,
    because that happens in SQL before anything is priced. That is why the
    column descriptions and ``fx_basis`` say so rather than leaving the nulls
    to be read as "no anomalies here".
    """
    for row in rows:
        for column in _ORIGINAL_CURRENCY_ANALYTICS:
            row[column] = None


@report(
    report_id="core:large_transactions",
    name="large_transactions",
    view=REPORTS_LARGE_TRANSACTIONS,
    classes={
        "transaction_id": DataClass.RECORD_ID,
        "account_id": DataClass.RECORD_ID,
        # dim_accounts.display_name (user-authored) → USER_NOTE; not the bank's
        # official_name (INSTITUTION) nor gsheet_connections.account_name.
        "account_name": DataClass.USER_NOTE,
        "txn_date": DataClass.TXN_DATE,
        "amount": DataClass.TXN_AMOUNT,
        "description": DataClass.DESCRIPTION,
        "merchant_id": DataClass.RECORD_ID,
        "merchant_normalized": DataClass.MERCHANT_NAME,
        "category": DataClass.CATEGORY,
        "currency_code": DataClass.CURRENCY,
        "amount_zscore_account": DataClass.AGGREGATE,
        "amount_zscore_category": DataClass.AGGREGATE,
        "is_top_100": DataClass.AGGREGATE,
    },
    parameter_classes={
        "top": DataClass.AGGREGATE,
        "anomaly": DataClass.TXN_TYPE,
    },
    columns=(
        OutputColumn(
            "transaction_id", "Canonical transaction identifier.", DataClass.RECORD_ID
        ),
        OutputColumn("account_id", "Owning account identifier.", DataClass.RECORD_ID),
        OutputColumn("account_name", "Account display name.", DataClass.USER_NOTE),
        OutputColumn("txn_date", "Transaction date.", DataClass.TXN_DATE),
        OutputColumn("amount", "Signed transaction amount.", DataClass.TXN_AMOUNT),
        OutputColumn(
            "description", "Original transaction description.", DataClass.DESCRIPTION
        ),
        OutputColumn(
            "merchant_id", "Canonical merchant identifier.", DataClass.RECORD_ID
        ),
        OutputColumn(
            "merchant_normalized",
            "Normalized merchant label.",
            DataClass.MERCHANT_NAME,
        ),
        OutputColumn("category", "Transaction category.", DataClass.CATEGORY),
        OutputColumn(
            "currency_code",
            "ISO 4217 currency this row is denominated in; null means unknown.",
            DataClass.CURRENCY,
        ),
        OutputColumn(
            "amount_zscore_account",
            "Modified absolute-amount z-score against the same-currency account "
            "baseline. Null on a read priced into another currency: the "
            "baseline is the account's own currency and cannot be restated at "
            "per-date rates.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "amount_zscore_category",
            "Modified absolute-amount z-score against the same-currency category "
            "baseline. Null on a read priced into another currency, for the "
            "same reason as the account score.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "is_top_100",
            "Whether the transaction is among its currency's top 100 by absolute "
            "amount. Null on a read priced into another currency: the ranking "
            "is over the original currency's population.",
            DataClass.AGGREGATE,
        ),
    ),
    semantics=ReportSemantics(
        unit="currency",
        currency="currency_code",
        sign="negative expense; positive income; ranking uses absolute amount",
        kind="flow",
        valuation_basis="transaction amount ranked by absolute magnitude",
        fx_basis=(
            "each row is one transaction on one date, so a requested display "
            "currency prices it at that date's rate; a row that cannot be priced "
            "leaves the whole report segmented per currency_code, never blended. "
            "Rates move between those dates, so a converted read is not one "
            "scaling of the original amounts: the rows are still selected, "
            "ranked and anomaly-filtered by original-currency magnitude, and the "
            "two z-scores and is_top_100 come back null rather than describe a "
            "currency they were not measured in"
        ),
        fx_date="txn_date",
        time_basis="inclusive full observed transaction period",
        denominator=(
            "account or category median absolute deviation scaled by 1.4826 "
            "for modified z-scores"
        ),
        comparison_window="account and category baselines over full observed history",
        exclusions=(
            "transfers",
            "archived accounts",
            "account z-scores for zero median absolute deviation",
            "category z-scores for fewer than five transactions or zero median "
            "absolute deviation",
        ),
        provenance=("reports.large_transactions",),
    ),
    # Both z-scores ARE a direct function of amount — unlike
    # recurring_subscriptions.amount_bucket, which only scopes a window
    # partition — so "it is computed from amount" cannot be the argument. What
    # makes them safe is the standardization: the model computes
    # (ABS(amount) - median_abs) / (1.4826 * MAD) against the group's own robust
    # location and scale, and projects NEITHER statistic as a column.
    class_downgrades={
        "amount_zscore_account": "modified z-score standardized against the "
        "per-account median and MAD, neither of which this view projects. The "
        "column is an affine image of ABS(amount) with both constants unknown "
        "to the caller: it fixes the transaction's position within its "
        "account's spread, denominated in units of that spread, and inverting "
        "it to an amount requires first recovering the account's location and "
        "scale",
        "amount_zscore_category": "same construction against the per-category "
        "median and MAD (NULL below 5 transactions in the category); safe for "
        "the same reason — the standardizing statistics are not columns of "
        "this view, so the ratio does not carry the amount that produced it",
    },
    on_converted=_blank_original_currency_analytics,
)
def large_transactions(
    db: Database,  # noqa: ARG001  # contract handle; this runner builds pure SQL
    *,
    top: int = 25,
    anomaly: str = "none",
) -> ReportQuery:
    """Top transactions by absolute amount with per-account/category z-scores.

    Amounts use the accounting convention (negative = expense, positive =
    income) in each row's own currency_code; rows are segmented per currency, never blended.

    Rows interleave the currencies, largest first within each, so a truncated
    result still represents every currency. Compare amounts only between rows
    sharing a currency_code.

    A display currency your rows are not already in prices each row at its own
    date's rate and returns the two z-scores and is_top_100 as null: those are
    cut in SQL against each row's original currency and a per-date conversion
    is not one scaling of them. Which rows come back is still decided by
    original-currency magnitude. Read the rows in their own currency to get the
    anomaly lens.

    Args:
        db: Open read-only database connection.
        top: Top N by ABS(amount) **within each currency** (>= 1). Ranking
            across currencies would compare unlike units, so one
            high-denomination currency could crowd every other currency out of
            the result entirely. A single-currency profile gets the same N rows
            it always did. On MCP the result is additionally capped at the
            session max_rows; the CLI is uncapped.
        anomaly: account | category | none — filter to z>2.5 in the named
            scope. Applied in SQL against the original-currency scores, so it
            selects the same rows whether or not a display currency is
            requested — but a converted read returns those scores as null.

    Examples:
        reports(report_id="core:large_transactions", parameters={"top": 50, "anomaly": "account"})
    """
    if anomaly not in LARGE_TXN_ANOMALIES:
        raise ValueError(f"Unknown anomaly: {anomaly}")
    # top < 1 would rank nothing into the result and read as "no large
    # transactions" rather than as the bad argument it is.
    if top < 1:
        raise ValueError(f"top must be >= 1, got {top!r}")
    predicate = "1=1"
    if anomaly == "account":
        predicate = "amount_zscore_account > 2.5"
    elif anomaly == "category":
        predicate = "amount_zscore_category > 2.5"
    sql = f"""
        SELECT transaction_id, account_id, account_name, txn_date, amount,
               description, merchant_id, merchant_normalized, category,
               currency_code,
               amount_zscore_account, amount_zscore_category, is_top_100
        FROM (
            SELECT transaction_id, account_id, account_name, txn_date, amount,
                   description, merchant_id, merchant_normalized, category,
                   currency_code,
                   amount_zscore_account, amount_zscore_category, is_top_100,
                   ROW_NUMBER() OVER (
                       PARTITION BY currency_code ORDER BY ABS(amount) DESC
                   ) AS rank_in_currency
            FROM {REPORTS_LARGE_TRANSACTIONS.full_name}
            WHERE {predicate}
        )
        WHERE rank_in_currency <= ?
        ORDER BY rank_in_currency, currency_code
    """  # noqa: S608  # TableRef + LARGE_TXN_ANOMALIES allowlists
    # The binding declares the class of the value it carries (R9).
    return ReportQuery(sql, [Binding(top, DataClass.AGGREGATE)])
