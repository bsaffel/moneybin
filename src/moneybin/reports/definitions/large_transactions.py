"""reports_large_transactions — top-N transactions with z-score anomaly lens."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    report,
)
from moneybin.reports.definitions._shared import LARGE_TXN_ANOMALIES
from moneybin.tables import REPORTS_LARGE_TRANSACTIONS


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
            "amount_zscore_account",
            "Modified absolute-amount z-score against the account baseline.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "amount_zscore_category",
            "Modified absolute-amount z-score against the category baseline.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "is_top_100",
            "Whether the transaction is among the top 100 by absolute amount.",
            DataClass.AGGREGATE,
        ),
    ),
    semantics=ReportSemantics(
        unit="currency",
        currency="summary.display_currency",
        sign="negative expense; positive income; ranking uses absolute amount",
        kind="flow",
        valuation_basis="transaction amount ranked by absolute magnitude",
        fx_basis="no FX conversion in v1; assumes single-currency inputs",
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
)
def large_transactions(
    db: Database,  # noqa: ARG001  # contract handle; this runner builds pure SQL
    *,
    top: int = 25,
    anomaly: str = "none",
) -> ReportQuery:
    """Top transactions by absolute amount with per-account/category z-scores.

    Amounts use the accounting convention (negative = expense, positive =
    income) in the currency named by summary.display_currency.

    Args:
        db: Open read-only database connection.
        top: Top N by ABS(amount) (>= 1). On MCP the result is additionally
            capped at the session max_rows; the CLI is uncapped.
        anomaly: account | category | none — filter to z>2.5 in the named scope.

    Examples:
        reports(report_id="core:large_transactions", parameters={"top": 50, "anomaly": "account"})
    """
    if anomaly not in LARGE_TXN_ANOMALIES:
        raise ValueError(f"Unknown anomaly: {anomaly}")
    # top < 1 would emit LIMIT 0/-1 (DuckDB treats -1 as no limit → full scan).
    if top < 1:
        raise ValueError(f"top must be >= 1, got {top!r}")
    sql = f"""
        SELECT transaction_id, account_id, account_name, txn_date, amount,
               description, merchant_id, merchant_normalized, category,
               amount_zscore_account, amount_zscore_category, is_top_100
        FROM {REPORTS_LARGE_TRANSACTIONS.full_name}
    """  # noqa: S608  # TableRef interpolation
    if anomaly == "account":
        sql += " WHERE amount_zscore_account > 2.5"
    elif anomaly == "category":
        sql += " WHERE amount_zscore_category > 2.5"
    sql += " ORDER BY ABS(amount) DESC LIMIT ?"
    return ReportQuery(sql, [top])
