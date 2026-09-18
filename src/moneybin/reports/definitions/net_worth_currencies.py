"""core:net_worth_currencies / `reports net-worth-currencies` — the currency-grain rung."""

from __future__ import annotations

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    report,
)
from moneybin.reports.definitions._shared import resolve_date_range
from moneybin.tables import REPORTS_NET_WORTH_CURRENCIES

_REPORT_ID = "core:net_worth_currencies"


@report(
    report_id=_REPORT_ID,
    name="net_worth_currencies",
    view=REPORTS_NET_WORTH_CURRENCIES,
    classes={
        "currency_code": DataClass.CURRENCY,
        "home_currency_code": DataClass.CURRENCY,
        "rate_source": DataClass.TXN_TYPE,
        "balance_date": DataClass.TXN_DATE,
        "rate_published_date": DataClass.TXN_DATE,
        "account_count": DataClass.AGGREGATE,
        "carried_forward_count": DataClass.AGGREGATE,
        "total_assets": DataClass.BALANCE,
        "total_liabilities": DataClass.BALANCE,
        "net_worth": DataClass.BALANCE,
        "total_assets_home": DataClass.BALANCE,
        "total_liabilities_home": DataClass.BALANCE,
        "net_worth_home": DataClass.BALANCE,
    },
    parameter_classes={"from_date": DataClass.TXN_DATE, "to_date": DataClass.TXN_DATE},
    columns=(
        OutputColumn(
            "currency_code",
            "ISO 4217 currency of this segment; null means unknown.",
            DataClass.CURRENCY,
        ),
        OutputColumn(
            "home_currency_code",
            "The profile's home currency; null until chosen.",
            DataClass.CURRENCY,
        ),
        OutputColumn(
            "rate_source",
            "override, provider, or identity; null when unpriced.",
            DataClass.TXN_TYPE,
        ),
        OutputColumn("balance_date", "Calendar date.", DataClass.TXN_DATE),
        OutputColumn(
            "rate_published_date",
            "Day the applied rate was published.",
            DataClass.TXN_DATE,
        ),
        OutputColumn(
            "account_count",
            "Accounts contributing on this date in this currency.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "carried_forward_count",
            "How many of them are carried forward.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "total_assets",
            "Sum of positive balances in currency_code.",
            DataClass.BALANCE,
            money_kind="balance",
        ),
        OutputColumn(
            "total_liabilities",
            "Sum of negative balances in currency_code, kept negative.",
            DataClass.BALANCE,
            money_kind="balance",
        ),
        OutputColumn(
            "net_worth",
            "This currency's segment in its own unit.",
            DataClass.BALANCE,
            money_kind="balance",
        ),
        OutputColumn(
            "total_assets_home",
            "total_assets in the home currency; null when unpriced.",
            DataClass.BALANCE,
            money_kind="balance",
            currency_basis="home",
        ),
        OutputColumn(
            "total_liabilities_home",
            "total_liabilities in the home currency; null when unpriced.",
            DataClass.BALANCE,
            money_kind="balance",
            currency_basis="home",
        ),
        OutputColumn(
            "net_worth_home",
            "This segment in the home currency; null when unpriced.",
            DataClass.BALANCE,
            money_kind="balance",
            currency_basis="home",
        ),
    ),
    semantics=ReportSemantics(
        unit="currency",
        currency="currency_code",
        sign="assets positive; liabilities negative; net worth is their signed sum",
        kind="position",
        valuation_basis=(
            "daily balance carried forward from the latest observation on or "
            "before balance_date"
        ),
        fx_basis=(
            "each row is one currency on one date, priced from its own "
            "currency_code; *_home columns are already in home_currency_code "
            "and are priced from it; a row that cannot be priced leaves the "
            "whole report segmented per currency_code, never blended"
        ),
        fx_date="balance_date",
        time_basis="one row per currency per day; latest day when no range is given",
        denominator=None,
        comparison_window=None,
        exclusions=(
            "accounts excluded from net worth",
            "archived accounts after their archive date",
        ),
        provenance=(
            "reports.net_worth_currencies",
            "core.fct_balances_daily",
            "core.dim_accounts",
            "core.fct_exchange_rates_effective",
        ),
    ),
    default_columns=("currency_code", "balance_date", "net_worth", "net_worth_home"),
)
def net_worth_currencies(
    db: Database,  # contract handle; this runner builds pure SQL
    *,
    from_date: str | None = None,
    to_date: str | None = None,
) -> ReportQuery:
    """Net worth per currency per day: the currency-grain rung of the ladder.

    One row per (currency_code, balance_date), summing every included
    account's balance in that currency, plus the home-currency conversion of
    both totals. Defaults to the latest available day when no range is given.

    Args:
        db: Open read-only database connection.
        from_date: Lower bound (inclusive) as 'YYYY-MM-DD'; leaves the upper
            end open when given alone.
        to_date: Upper bound (inclusive) as 'YYYY-MM-DD'; leaves the lower
            end open when given alone.

    Examples:
        reports(report_id="core:net_worth_currencies")
        reports(report_id="core:net_worth_currencies", parameters={"from_date": "2026-01-01"})
    """
    rng = resolve_date_range(
        from_date, to_date, report_id=_REPORT_ID, view=REPORTS_NET_WORTH_CURRENCIES
    )
    # Sorting on balance_date major and the currency column minor can hand a
    # row cap one date's currencies in full and cut the next date off partway
    # through its own currency list — the default (no range given) query
    # never has more than one date, but an explicit range can. `rank_in_currency`
    # — one currency's own row sequence, oldest first — makes the cap advance
    # every currency's date depth evenly instead: no currency is dropped from
    # a date while another currency still has a row at that same depth
    # (test_currency_truncation.py).
    sql = f"""
        WITH ranked AS (
            SELECT currency_code, home_currency_code, rate_source, balance_date,
                   rate_published_date, account_count, carried_forward_count,
                   total_assets, total_liabilities, net_worth, total_assets_home,
                   total_liabilities_home, net_worth_home,
                   ROW_NUMBER() OVER (
                       PARTITION BY currency_code ORDER BY balance_date
                   ) AS rank_in_currency
            FROM {REPORTS_NET_WORTH_CURRENCIES.full_name}
            WHERE 1=1{rng.where_sql}
        )
        SELECT currency_code, home_currency_code, rate_source, balance_date,
               rate_published_date, account_count, carried_forward_count,
               total_assets, total_liabilities, net_worth, total_assets_home,
               total_liabilities_home, net_worth_home
        FROM ranked
        ORDER BY rank_in_currency, balance_date, currency_code NULLS LAST
    """  # noqa: S608  # TableRef interpolation, static column list
    actions = [
        "Run reports(report_id='core:net_worth') for the single home-currency total",
        "Run reports(report_id='core:net_worth_accounts') for the account-level breakdown",
    ]
    return ReportQuery(sql, rng.params, actions=actions, period=rng.period)
