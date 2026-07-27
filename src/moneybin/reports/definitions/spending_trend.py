"""core:spending / `reports spending` — monthly spending trend with deltas."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    report,
)
from moneybin.reports.definitions._shared import SPENDING_COMPARES, resolve_window
from moneybin.tables import REPORTS_SPENDING_TREND


@report(
    report_id="core:spending",
    name="spending",
    view=REPORTS_SPENDING_TREND,
    classes={
        "year_month": DataClass.TXN_DATE,
        "category": DataClass.CATEGORY,
        "currency_code": DataClass.CURRENCY,
        "total_spend": DataClass.TXN_AMOUNT,
        "txn_count": DataClass.AGGREGATE,
        "prev_month_spend": DataClass.TXN_AMOUNT,
        "mom_delta": DataClass.TXN_AMOUNT,
        "mom_pct": DataClass.AGGREGATE,
        "prev_year_spend": DataClass.TXN_AMOUNT,
        "yoy_delta": DataClass.TXN_AMOUNT,
        "yoy_pct": DataClass.AGGREGATE,
        "trailing_3mo_avg": DataClass.TXN_AMOUNT,
    },
    parameter_classes={
        "from_month": DataClass.TXN_DATE,
        "to_month": DataClass.TXN_DATE,
        "category": DataClass.CATEGORY,
        "compare": DataClass.TXN_TYPE,
    },
    columns=(
        OutputColumn("year_month", "Calendar month as YYYY-MM.", DataClass.TXN_DATE),
        OutputColumn("category", "Spending category.", DataClass.CATEGORY),
        OutputColumn(
            "currency_code",
            "ISO 4217 currency this row is denominated in; null means unknown.",
            DataClass.CURRENCY,
        ),
        OutputColumn(
            "total_spend",
            "Absolute outflow in the month and category.",
            DataClass.TXN_AMOUNT,
        ),
        OutputColumn("txn_count", "Outflow transaction count.", DataClass.AGGREGATE),
        OutputColumn(
            "prev_month_spend",
            "Spend in the previous calendar month.",
            DataClass.TXN_AMOUNT,
        ),
        OutputColumn(
            "mom_delta",
            "Current spend minus previous-month spend.",
            DataClass.TXN_AMOUNT,
        ),
        OutputColumn(
            "mom_pct",
            "Month-over-month delta divided by previous-month spend.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "prev_year_spend",
            "Spend in the same calendar month one year earlier.",
            DataClass.TXN_AMOUNT,
        ),
        OutputColumn(
            "yoy_delta",
            "Current spend minus same-month prior-year spend.",
            DataClass.TXN_AMOUNT,
        ),
        OutputColumn(
            "yoy_pct",
            "Year-over-year delta divided by prior-year spend.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "trailing_3mo_avg",
            "Rolling three-month average ending in the current month.",
            DataClass.TXN_AMOUNT,
        ),
    ),
    semantics=ReportSemantics(
        unit="currency",
        currency="currency_code",
        sign="spend is positive absolute outflow; deltas are current minus comparison",
        kind="flow",
        valuation_basis="transaction amount",
        fx_basis="no FX conversion in v1; rows are segmented per currency_code, never blended",
        time_basis=(
            "inclusive eligible-data calendar-month period with zero-filled missing "
            "category-months"
        ),
        denominator=(
            "previous-month spend for mom_pct; prior-year spend for yoy_pct; "
            "available calendar months up to three for trailing_3mo_avg, including "
            "zero-spend months"
        ),
        comparison_window=(
            "previous calendar month, same calendar month one year earlier, and "
            "trailing three calendar months including current month"
        ),
        exclusions=("transfers", "archived accounts", "non-outflows"),
        provenance=("reports.spending_trend",),
    ),
    class_downgrades={
        "mom_pct": "ratio of two already-declared TXN_AMOUNT columns "
        "(total_spend / prev_month_spend); a percentage change reveals no "
        "absolute dollar amount",
        "yoy_pct": "ratio of two already-declared TXN_AMOUNT columns "
        "(total_spend / prev_year_spend); a percentage change reveals no "
        "absolute dollar amount",
    },
)
def spending_trend(
    db: Database,  # noqa: ARG001  # contract handle; this runner builds pure SQL
    *,
    from_month: str | None = None,
    to_month: str | None = None,
    category: str | None = None,
    compare: str = "yoy",
) -> ReportQuery:
    """Monthly spending trend with MoM, YoY, and 3-month-trailing deltas.

    Defaults to the last 12 calendar months when both bounds are omitted. YoY
    columns come from the underlying view (all history), so narrowing the window
    does not null out yoy_pct. Spending amounts are positive absolute outflows;
    comparison deltas are current spend minus comparison-period spend. Monetary
    values are denominated in each row's own currency_code.

    Args:
        db: Open read-only database connection.
        from_month: Lower bound (inclusive) as 'YYYY-MM'.
        to_month: Upper bound (inclusive) as 'YYYY-MM'.
        category: Filter to a specific category text. None returns all.
        compare: yoy | mom | trailing — caller-side intent only; the view
            returns all three comparison columns regardless.

    Examples:
        reports(report_id="core:spending", parameters={"category": "Groceries"})
        reports(report_id="core:spending", parameters={"from_month": "2023-01", "to_month": "2023-12"})
    """
    # Validate so agents see the allowed values and can't pass arbitrary strings;
    # the view returns all three comparison columns regardless, so `compare` has
    # no effect on the SQL below (caller-side intent only — the raise is reachable).
    if compare not in SPENDING_COMPARES:
        raise ValueError(f"Unknown compare: {compare}")
    from_month, to_month, period, hint = resolve_window(
        from_month,
        to_month,
        report_id="core:spending",
    )

    ranked = f"""
        SELECT year_month, category, currency_code, total_spend, txn_count,
               prev_month_spend, mom_delta, mom_pct,
               prev_year_spend, yoy_delta, yoy_pct,
               trailing_3mo_avg,
               ROW_NUMBER() OVER (
                   PARTITION BY year_month, currency_code
                   ORDER BY total_spend DESC
               ) AS rank_in_currency
        FROM {REPORTS_SPENDING_TREND.full_name}
        WHERE 1=1
    """  # noqa: S608  # TableRef interpolation
    params: list[object] = []
    if from_month:
        ranked += " AND year_month >= substr(?, 1, 7)"
        params.append(from_month)
    if to_month:
        ranked += " AND year_month <= substr(?, 1, 7)"
        params.append(to_month)
    if category:
        ranked += " AND category = ?"
        params.append(category)

    # Spend still ranks only within a currency — comparing total_spend across
    # denominations would order by exchange-rate scale rather than by spending.
    # But sorting currency-major on top of that let the row cap take every
    # category of the lexicographically-first currency before the next currency
    # started, so a capped month reported one currency's categories and dropped
    # the others entirely. Sorting on the per-currency rank interleaves them, so
    # any prefix of a month holds every currency that fits
    # (multi-currency.md Requirement 5).
    sql = f"""
        SELECT year_month, category, currency_code, total_spend, txn_count,
               prev_month_spend, mom_delta, mom_pct,
               prev_year_spend, yoy_delta, yoy_pct,
               trailing_3mo_avg
        FROM ({ranked})
        ORDER BY year_month, rank_in_currency, currency_code
    """  # noqa: S608  # subquery built from TableRef + allowlisted filters

    actions = [
        "Run reports(report_id='core:spending', "
        "parameters={'category': '<name>'}) to filter to one category",
        "Run reports(report_id='core:cashflow') for inflow, outflow, and net",
        "Run reports(report_id='core:recurring') for recurring charge patterns",
    ]
    if hint:
        actions.insert(0, hint)
    return ReportQuery(sql, params, actions=actions, period=period)
