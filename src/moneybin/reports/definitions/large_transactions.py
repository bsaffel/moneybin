"""core:large_transactions — top-N transactions with z-score anomaly lens."""

from __future__ import annotations

from typing import Any

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    ORIGINAL_CURRENCY_COLUMN,
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
    rows: list[dict[str, Any]], currency: str
) -> None:
    """Drop the anomaly lens on the rows a conversion repriced, and only those.

    Both z-scores standardize ``ABS(amount)`` against the account's or
    category's own median and MAD over its full history, and ``is_top_100``
    ranks against that same population — all computed in SQL, in whatever
    currency each row was recorded in. Conversion prices every row at its own
    ``txn_date`` rate, so the converted amounts are not one scaling of the
    originals: two charges equal at 100 EUR come back as 100 and 200 USD if the
    rate moved between their dates, while carrying the single score they earned
    as equals.

    Restating them is not something a read can do. The baselines span the
    account's whole history, not the rows returned, and this path resolves only
    from stored rates — repricing that history at per-date rates is exactly the
    provider round trip a report read must never make. So the honest answer is
    the one these columns already give for an account with too little history
    to score: null.

    A mixed-currency result reaches here for the sake of its foreign rows, and
    the rows already in the target ride along untouched: an identity rate moved
    nothing, so their scores still describe the amounts on screen. Nulling those
    would make a row read differently depending on what some *other* row needed,
    which is the opposite of what this repair is for. Rows are told apart by
    ``ORIGINAL_CURRENCY_COLUMN``, which conversion attaches precisely because
    ``currency_code`` has been relabelled to the target by now; a row that
    carries no original currency is nulled, since nothing on it can show the
    score is still measured in the currency shown.

    The row *set* is still ranked and filtered by original-currency magnitude,
    because that happens in SQL before anything is priced. That is why the
    column descriptions and ``fx_basis`` say so rather than leaving the nulls
    to be read as "no anomalies here".
    """
    for row in rows:
        if row.get(ORIGINAL_CURRENCY_COLUMN) == currency:
            continue
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
        OutputColumn(
            "merchant_id", "Canonical merchant identifier.", DataClass.RECORD_ID
        ),
        OutputColumn("account_name", "Account display name.", DataClass.USER_NOTE),
        OutputColumn(
            "merchant_normalized",
            "Normalized merchant label.",
            DataClass.MERCHANT_NAME,
        ),
        OutputColumn(
            "description", "Original transaction description.", DataClass.DESCRIPTION
        ),
        OutputColumn("category", "Transaction category.", DataClass.CATEGORY),
        OutputColumn(
            "currency_code",
            "ISO 4217 currency this row is denominated in; null means unknown.",
            DataClass.CURRENCY,
        ),
        OutputColumn("txn_date", "Transaction date.", DataClass.TXN_DATE),
        OutputColumn(
            "amount_zscore_account",
            "Modified absolute-amount z-score against the same-currency account "
            "baseline. Null on a row this read repriced: the baseline is the "
            "account's own currency and cannot be restated at per-date rates. A "
            "row already in the display currency keeps its score.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "amount_zscore_category",
            "Modified absolute-amount z-score against the same-currency category "
            "baseline. Null on a row this read repriced, for the same reason as "
            "the account score.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "is_top_100",
            "Whether the transaction is among its currency's top 100 by absolute "
            "amount. Null on a row this read repriced: the ranking is over the "
            "original currency's population.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "amount",
            "Signed transaction amount.",
            DataClass.TXN_AMOUNT,
            money_kind="flow",
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
            "ranked and anomaly-filtered by original-currency magnitude, and on "
            "any row this read repriced the two z-scores and is_top_100 come "
            "back null rather than describe a currency they were not measured "
            "in. A row already in the display currency was not repriced and "
            "keeps them, with original_currency_code naming the currency they "
            "were measured in"
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
    # Requirement 6: the shape of a transaction listing — when, where, what,
    # how much. `description` is free text and may elide; the two z-scores and
    # the `is_top_100` flag are why a row is *here* rather than what it is, and
    # a reader who wants them asks for `--wide`. `transaction_id` and
    # `account_id` are deliberately out: an id is unbounded in width and would
    # crowd out the columns that identify the row to a human.
    default_columns=(
        "account_name",
        "description",
        "currency_code",
        "txn_date",
        "amount",
    ),
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

    A display currency prices each row that is not already in it at that row's
    own date's rate, and returns the two z-scores and is_top_100 as null for
    exactly those rows: they are cut in SQL against each row's original
    currency and a per-date conversion is not one scaling of them. Rows already
    in the display currency were not repriced and keep the anomaly lens. Which
    rows come back is still decided by original-currency magnitude. Read the
    rows in their own currency to get the lens on all of them.

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
    # `BETWEEN 1 AND ?`, not `<= ?`: DuckDB 1.5.4 and 1.5.5 rewrite a
    # `ROW_NUMBER() ... <= n` filter into a top-N and push it through the
    # recursive match-group CTE behind core.bridge_transfers, where it fails
    # with "Type mismatch for SET OPERATION". The BETWEEN spelling is the same
    # predicate and escapes the rewrite; tests/scenarios pins it on real views.
    sql = f"""
        SELECT transaction_id, account_id, merchant_id, account_name,
               merchant_normalized, description, category, currency_code,
               txn_date,
               amount_zscore_account, amount_zscore_category, is_top_100, amount
        FROM (
            SELECT transaction_id, account_id, merchant_id, account_name,
                   merchant_normalized, description, category, currency_code,
                   txn_date,
                   amount_zscore_account, amount_zscore_category, is_top_100,
                   amount,
                   ROW_NUMBER() OVER (
                       PARTITION BY currency_code ORDER BY ABS(amount) DESC
                   ) AS rank_in_currency
            FROM {REPORTS_LARGE_TRANSACTIONS.full_name}
            WHERE {predicate}
        )
        WHERE rank_in_currency BETWEEN 1 AND ?
        ORDER BY rank_in_currency, currency_code
    """  # noqa: S608  # TableRef + LARGE_TXN_ANOMALIES allowlists
    # The binding declares the class of the value it carries (R9).
    return ReportQuery(sql, [Binding(top, DataClass.AGGREGATE)])
