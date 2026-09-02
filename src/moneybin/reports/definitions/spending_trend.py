"""core:spending / `reports spending` — monthly spending trend with deltas."""

from __future__ import annotations

from collections.abc import Mapping

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    Binding,
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    report,
)
from moneybin.reports.definitions._shared import SPENDING_COMPARES, resolve_window
from moneybin.tables import REPORTS_SPENDING_TREND

#: The comparison a caller gets without asking. Named once because the runner
#: signature and the default column set below both need it, and a drift between
#: them would render one comparison while the view was asked for another.
DEFAULT_COMPARE = "yoy"

#: The columns each comparison is *about*. The view returns all three regardless
#: (see the runner), so this is the one place `compare` has an observable
#: effect — before this, passing it changed nothing a caller could see.
_COMPARISON_COLUMNS: Mapping[str, tuple[str, ...]] = {
    "mom": ("mom_pct",),
    "yoy": ("yoy_pct",),
    "trailing": ("trailing_3mo_avg",),
}


def _default_columns(parameters: Mapping[str, object]) -> tuple[str, ...]:
    """Requirement 6: the month, the category, the spend, and the comparison asked for.

    Parameter-aware because `compare` selects among columns the projection
    always carries — a static tuple would either show all three comparisons,
    which does not fit 80 characters, or pick one and ignore the parameter.

    Reads the mapping defensively rather than indexing it: the runner has
    already rejected an unknown `compare` by the time this runs, and a
    `KeyError` raised from the render path would take down a table whose rows
    are already computed and correct.
    """
    compare = parameters.get("compare") or DEFAULT_COMPARE
    comparison = _COMPARISON_COLUMNS.get(
        str(compare), _COMPARISON_COLUMNS[DEFAULT_COMPARE]
    )
    # `currency_code` is part of the grain: without it two rows differing only
    # in currency read as one month's category counted twice.
    #
    # Ordered by Rule B, and `total_spend` leads the measure block rather than
    # ending it: it is the base every comparison here is measured against, and
    # `column-ordering.md` Rule C gives a comparative's base precedence over
    # headline-last precisely so a delta is never printed before the quantity
    # it is a delta of.
    return ("category", "currency_code", "year_month", "total_spend", *comparison)


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
        OutputColumn("category", "Spending category.", DataClass.CATEGORY),
        OutputColumn(
            "currency_code",
            "ISO 4217 currency this row is denominated in; null means unknown.",
            DataClass.CURRENCY,
        ),
        OutputColumn("year_month", "Calendar month as YYYY-MM.", DataClass.TXN_DATE),
        OutputColumn("txn_count", "Outflow transaction count.", DataClass.AGGREGATE),
        OutputColumn(
            "total_spend",
            "Absolute outflow in the month and category.",
            DataClass.TXN_AMOUNT,
            # SUM(ABS(t.amount)) in the model: a positive absolute outflow, not
            # income. Rendering it as a `flow` would sign it `+` and colour it
            # green — spending reported as earnings.
            money_kind="magnitude",
        ),
        OutputColumn(
            "prev_month_spend",
            "Spend in the previous calendar month.",
            DataClass.TXN_AMOUNT,
            money_kind="magnitude",
        ),
        OutputColumn(
            "mom_delta",
            "Current spend minus previous-month spend.",
            DataClass.TXN_AMOUNT,
            # A change in a spend magnitude, so the sign means direction rather
            # than income/expense: positive is spending that *rose*, which is
            # the unfavourable direction.
            money_kind="delta",
            polarity="expense",
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
            money_kind="magnitude",
        ),
        OutputColumn(
            "yoy_delta",
            "Current spend minus same-month prior-year spend.",
            DataClass.TXN_AMOUNT,
            money_kind="delta",
            polarity="expense",
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
            money_kind="magnitude",
        ),
    ),
    semantics=ReportSemantics(
        unit="currency",
        currency="currency_code",
        sign="spend is positive absolute outflow; deltas are current minus comparison",
        kind="flow",
        valuation_basis="transaction amount",
        fx_basis=(
            "amounts are aggregated per currency_code, so pricing a row into one "
            "display currency would leave several rows sharing a grain key; rows "
            "stay segmented per currency_code, never blended"
        ),
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
    default_columns=_default_columns,
)
def spending_trend(
    db: Database,  # noqa: ARG001  # contract handle; this runner builds pure SQL
    *,
    from_month: str | None = None,
    to_month: str | None = None,
    category: str | None = None,
    compare: str = DEFAULT_COMPARE,
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
        compare: yoy | mom | trailing — selects which comparison the text
            table shows by default. The view returns all three columns
            regardless, so JSON, MCP, and --wide are unaffected.

    Examples:
        reports(report_id="core:spending", parameters={"category": "Groceries"})
        reports(report_id="core:spending", parameters={"from_month": "2023-01", "to_month": "2023-12"})
    """
    # Validate so agents see the allowed values and can't pass arbitrary strings;
    # the view returns all three comparison columns regardless, so `compare` has
    # no effect on the SQL below — it selects the text table's default columns
    # (`_default_columns`), and the raise is reachable.
    if compare not in SPENDING_COMPARES:
        raise ValueError(f"Unknown compare: {compare}")
    from_month, to_month, period, hint = resolve_window(
        from_month,
        to_month,
        report_id="core:spending",
    )

    ranked = f"""
        SELECT category, currency_code, year_month, txn_count, total_spend,
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
    # Each binding declares the class of the value it carries (R9). The class is
    # read off the binding by the provenance renderer, which cannot recover it
    # from the signature: these appends are conditional, so binding N is not a
    # fixed offset into the parameter list.
    params: list[Binding] = []
    if from_month:
        ranked += " AND year_month >= substr(?, 1, 7)"
        params.append(Binding(from_month, DataClass.TXN_DATE))
    if to_month:
        ranked += " AND year_month <= substr(?, 1, 7)"
        params.append(Binding(to_month, DataClass.TXN_DATE))
    if category:
        ranked += " AND category = ?"
        params.append(Binding(category, DataClass.CATEGORY))

    # Spend still ranks only within a currency — comparing total_spend across
    # denominations would order by exchange-rate scale rather than by spending.
    # But sorting currency-major on top of that let the row cap take every
    # category of the lexicographically-first currency before the next currency
    # started, so a capped month reported one currency's categories and dropped
    # the others entirely. Sorting on the per-currency rank interleaves them, so
    # any prefix of a month holds every currency that fits
    # (multi-currency.md Requirement 5).
    sql = f"""
        SELECT category, currency_code, year_month, txn_count, total_spend,
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
