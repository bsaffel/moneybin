"""core:cashflow / `reports cashflow` — monthly inflow/outflow/net rollup."""

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
from moneybin.reports.definitions._shared import CASHFLOW_GROUPINGS, resolve_window
from moneybin.tables import REPORTS_CASH_FLOW


@report(
    report_id="core:cashflow",
    name="cashflow",
    view=REPORTS_CASH_FLOW,
    classes={
        "year_month": DataClass.TXN_DATE,
        "account_id": DataClass.RECORD_ID,
        # dim_accounts.display_name (user-authored) → USER_NOTE; not the bank's
        # official_name (INSTITUTION) nor gsheet_connections.account_name.
        "account_name": DataClass.USER_NOTE,
        "category": DataClass.CATEGORY,
        "currency_code": DataClass.CURRENCY,
        "inflow": DataClass.TXN_AMOUNT,
        "outflow": DataClass.TXN_AMOUNT,
        "net": DataClass.TXN_AMOUNT,
        "txn_count": DataClass.AGGREGATE,
    },
    parameter_classes={
        "from_month": DataClass.TXN_DATE,
        "to_month": DataClass.TXN_DATE,
        "by": DataClass.TXN_TYPE,
    },
    columns=(
        OutputColumn("year_month", "Calendar month as YYYY-MM.", DataClass.TXN_DATE),
        OutputColumn("account_id", "Owning account identifier.", DataClass.RECORD_ID),
        OutputColumn("account_name", "Account display name.", DataClass.USER_NOTE),
        OutputColumn("category", "Transaction category.", DataClass.CATEGORY),
        OutputColumn(
            "currency_code",
            "ISO 4217 currency these sums are denominated in; null means unknown.",
            DataClass.CURRENCY,
        ),
        OutputColumn("inflow", "Sum of positive amounts.", DataClass.TXN_AMOUNT),
        OutputColumn(
            "outflow", "Sum of negative amounts, kept negative.", DataClass.TXN_AMOUNT
        ),
        OutputColumn("net", "Inflow plus outflow.", DataClass.TXN_AMOUNT),
        OutputColumn(
            "txn_count", "Non-transfer transaction count.", DataClass.AGGREGATE
        ),
    ),
    semantics=ReportSemantics(
        unit="currency",
        currency="currency_code",
        sign="negative expense; positive income",
        kind="flow",
        valuation_basis="transaction amount",
        fx_basis=(
            "amounts are aggregated per currency_code, so pricing a row into one "
            "display currency would leave several rows sharing a grain key; rows "
            "stay segmented per currency_code, never blended"
        ),
        time_basis="inclusive calendar-month period",
        denominator=None,
        comparison_window=None,
        exclusions=("transfers", "archived accounts"),
        provenance=("reports.cash_flow",),
    ),
)
def cash_flow(
    db: Database,  # noqa: ARG001  # contract handle; this runner builds pure SQL
    *,
    from_month: str | None = None,
    to_month: str | None = None,
    by: str = "account-and-category",
) -> ReportQuery:
    """Monthly cash flow rollup: inflow/outflow/net per account x category.

    Defaults to the last 12 calendar months when both bounds are omitted.
    Amounts use the accounting convention (negative = expense, positive =
    income) in each row's own currency_code; rows are segmented per currency, never blended.

    Args:
        db: Open read-only database connection.
        from_month: Lower bound (inclusive) as 'YYYY-MM' (also accepts
            'YYYY-MM-DD' and ignores the day).
        to_month: Upper bound (inclusive) as 'YYYY-MM'.
        by: account | category | account-and-category — how to group.

    Examples:
        reports(report_id="core:cashflow", parameters={"by": "category", "from_month": "2024-01"})
        reports(report_id="core:cashflow", parameters={"by": "account"})
    """
    if by not in CASHFLOW_GROUPINGS:
        raise ValueError(f"Unknown by: {by}")
    from_month, to_month, period, hint = resolve_window(
        from_month,
        to_month,
        report_id="core:cashflow",
    )

    # currency_code groups unconditionally, for every `by` value. Dropping it
    # from one grouping would re-blend the currencies the view just separated
    # (multi-currency.md Requirement 5).
    select_cols = "year_month, currency_code"
    group_cols = "year_month, currency_code"
    if by in ("account", "account-and-category"):
        # account_id keeps rows distinct when two accounts share a display_name.
        select_cols += ", account_id, account_name"
        group_cols += ", account_id, account_name"
    if by in ("category", "account-and-category"):
        select_cols += ", category"
        group_cols += ", category"

    grouped = f"""
        SELECT {select_cols},
               SUM(inflow) AS inflow,
               SUM(outflow) AS outflow,
               SUM(net) AS net,
               SUM(txn_count) AS txn_count,
               ROW_NUMBER() OVER (
                   PARTITION BY year_month, currency_code
                   ORDER BY ABS(SUM(net)) DESC
               ) AS rank_in_currency
        FROM {REPORTS_CASH_FLOW.full_name}
        WHERE 1=1
    """  # noqa: S608  # select_cols + TableRef allowlists
    # Each binding declares the class of the value it carries (R9).
    params: list[Binding] = []
    if from_month:
        grouped += " AND year_month >= substr(?, 1, 7)"
        params.append(Binding(from_month, DataClass.TXN_DATE))
    if to_month:
        grouped += " AND year_month <= substr(?, 1, 7)"
        params.append(Binding(to_month, DataClass.TXN_DATE))
    grouped += f" GROUP BY {group_cols}"  # noqa: S608  # group_cols allowlist

    # `by="account"` / `"category"` puts several rows in one month per currency,
    # so sorting currency-major hands the row cap that whole month's budget in
    # the lexicographically-first currency and the others are absent from the
    # response rather than ranked lower. Ranking within (month, currency) and
    # sorting on that rank takes one row from each currency before a second from
    # any (multi-currency.md Requirement 5). `by="none"` is already one row per
    # (month, currency), where every rank is 1 and this reduces to the plain
    # chronological listing.
    sql = f"""
        SELECT {select_cols}, inflow, outflow, net, txn_count
        FROM ({grouped})
        ORDER BY year_month, rank_in_currency, currency_code
    """  # noqa: S608  # select_cols allowlist

    actions = [
        "Rerun reports(report_id='core:cashflow', "
        "parameters={'by': 'category'}) to regroup by category",
        "Run reports(report_id='core:spending') for outflow-only MoM and YoY trends",
    ]
    if hint:
        actions.insert(0, hint)
    return ReportQuery(sql, params, actions=actions, period=period)
