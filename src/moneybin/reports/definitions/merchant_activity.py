"""core:merchants / `reports merchants` — per-merchant lifetime totals."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    Binding,
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    report,
)
from moneybin.reports.definitions._shared import MERCHANTS_SORTS
from moneybin.tables import REPORTS_MERCHANT_ACTIVITY


@report(
    report_id="core:merchants",
    name="merchants",
    view=REPORTS_MERCHANT_ACTIVITY,
    classes={
        "merchant_id": DataClass.RECORD_ID,
        "merchant_normalized": DataClass.MERCHANT_NAME,
        "currency_code": DataClass.CURRENCY,
        "total_spend": DataClass.TXN_AMOUNT,
        "total_inflow": DataClass.TXN_AMOUNT,
        "total_outflow": DataClass.TXN_AMOUNT,
        "txn_count": DataClass.AGGREGATE,
        "avg_amount": DataClass.TXN_AMOUNT,
        "median_amount": DataClass.TXN_AMOUNT,
        "first_seen": DataClass.TXN_DATE,
        "last_seen": DataClass.TXN_DATE,
        "active_months": DataClass.AGGREGATE,
        "top_category": DataClass.CATEGORY,
        "account_count": DataClass.AGGREGATE,
    },
    parameter_classes={
        "top": DataClass.AGGREGATE,
        "sort": DataClass.TXN_TYPE,
    },
    columns=(
        OutputColumn(
            "merchant_id", "Canonical merchant identifier.", DataClass.RECORD_ID
        ),
        OutputColumn(
            "merchant_normalized",
            "Canonical merchant label or uncategorized bucket.",
            DataClass.MERCHANT_NAME,
        ),
        OutputColumn(
            "currency_code",
            "ISO 4217 currency this row is denominated in; null means unknown.",
            DataClass.CURRENCY,
        ),
        OutputColumn(
            "total_spend",
            "Lifetime absolute outflow.",
            DataClass.TXN_AMOUNT,
            money_kind="magnitude",
        ),
        OutputColumn(
            "total_inflow",
            "Lifetime sum of positive amounts.",
            DataClass.TXN_AMOUNT,
            money_kind="magnitude",
        ),
        OutputColumn(
            "total_outflow",
            "Lifetime sum of negative amounts, kept negative.",
            DataClass.TXN_AMOUNT,
            # Kept negative, unlike `total_spend` one row up, which is the same
            # money as an absolute. Declaring them the same kind would render
            # one of the two with a sign it does not carry.
            money_kind="flow",
        ),
        OutputColumn("txn_count", "Transaction count.", DataClass.AGGREGATE),
        OutputColumn(
            "avg_amount",
            "Mean signed amount.",
            DataClass.TXN_AMOUNT,
            money_kind="flow",
        ),
        OutputColumn(
            "median_amount",
            "Median signed amount.",
            DataClass.TXN_AMOUNT,
            money_kind="flow",
        ),
        OutputColumn("first_seen", "Earliest transaction date.", DataClass.TXN_DATE),
        OutputColumn("last_seen", "Latest transaction date.", DataClass.TXN_DATE),
        OutputColumn(
            "active_months",
            "Distinct active calendar-month count.",
            DataClass.AGGREGATE,
        ),
        OutputColumn("top_category", "Modal category.", DataClass.CATEGORY),
        OutputColumn("account_count", "Distinct account count.", DataClass.AGGREGATE),
    ),
    semantics=ReportSemantics(
        unit="currency",
        currency="currency_code",
        sign=(
            "spend is positive absolute outflow; outflow is negative; inflow is "
            "positive; average and median are signed"
        ),
        kind="flow",
        valuation_basis="transaction amount",
        fx_basis=(
            "amounts are aggregated per currency_code over a span of dates rather "
            "than one, so pricing a row into one display currency would leave "
            "several rows sharing a grain key and no date to price them on; rows "
            "stay segmented per currency_code, never blended"
        ),
        time_basis=(
            "inclusive full observed transaction period from first_seen through "
            "last_seen"
        ),
        denominator="txn_count for avg_amount",
        comparison_window=None,
        exclusions=("transfers", "archived accounts"),
        provenance=("reports.merchant_activity",),
    ),
    # Requirement 6: who is being paid, how much, how often, how recently.
    # `total_inflow` / `total_outflow` decompose the spend and `avg_amount` /
    # `median_amount` characterise it — all reachable with `--wide`.
    default_columns=(
        "merchant_normalized",
        "currency_code",
        "total_spend",
        "txn_count",
        "last_seen",
    ),
)
def merchant_activity(
    db: Database,  # noqa: ARG001  # contract handle; this runner builds pure SQL
    *,
    top: int = 25,
    sort: str = "spend",
) -> ReportQuery:
    """Per-merchant lifetime activity totals (spend, count, first/last seen).

    total_spend is positive absolute outflow; total_outflow is negative;
    total_inflow is positive; avg_amount and median_amount are signed. Monetary
    values are denominated in each row's own currency_code.

    Rows interleave the currencies, highest-ranked first within each, so a
    truncated result still represents every currency. Compare monetary values
    only between rows sharing a currency_code.

    Args:
        db: Open read-only database connection.
        top: Limit rows **within each currency** (>= 1). A spend-sorted ranking
            across currencies compares unlike units, so one high-denomination
            currency could take every slot. A single-currency profile gets the
            same N rows it always did. On MCP the result is additionally capped
            at the session max_rows; the CLI is uncapped.
        sort: spend | count | recent.

    Examples:
        reports(report_id="core:merchants", parameters={"top": 10, "sort": "count"})
    """
    if sort not in MERCHANTS_SORTS:
        raise ValueError(f"Unknown sort: {sort}")
    if top < 1:
        raise ValueError(f"top must be >= 1, got {top!r}")
    sql = f"""
        SELECT merchant_id, merchant_normalized, currency_code, total_spend, total_inflow,
               total_outflow, txn_count, avg_amount, median_amount,
               first_seen, last_seen, active_months, top_category,
               account_count
        FROM (
            SELECT merchant_id, merchant_normalized, currency_code, total_spend,
                   total_inflow, total_outflow, txn_count, avg_amount,
                   median_amount, first_seen, last_seen, active_months,
                   top_category, account_count,
                   ROW_NUMBER() OVER (
                       PARTITION BY currency_code ORDER BY {MERCHANTS_SORTS[sort]}
                   ) AS rank_in_currency
            FROM {REPORTS_MERCHANT_ACTIVITY.full_name}
        )
        WHERE rank_in_currency <= ?
        ORDER BY rank_in_currency, currency_code
    """  # noqa: S608  # TableRef + MERCHANTS_SORTS allowlists
    return ReportQuery(sql, [Binding(top, DataClass.AGGREGATE)])
