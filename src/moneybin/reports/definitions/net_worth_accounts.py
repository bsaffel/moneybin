"""core:net_worth_accounts / `reports net-worth-accounts` — the account-grain rung."""

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
from moneybin.tables import REPORTS_NET_WORTH_ACCOUNTS

_REPORT_ID = "core:net_worth_accounts"


@report(
    report_id=_REPORT_ID,
    name="net_worth_accounts",
    view=REPORTS_NET_WORTH_ACCOUNTS,
    classes={
        "account_id": DataClass.RECORD_ID,
        # dim_accounts.display_name (user-authored) → USER_NOTE; not the bank's
        # official_name (INSTITUTION) nor gsheet_connections.account_name.
        "account_name": DataClass.USER_NOTE,
        "currency_code": DataClass.CURRENCY,
        "home_currency_code": DataClass.CURRENCY,
        # Matches dim_accounts.account_type's own CLASSIFICATION entry
        # (taxonomy.py) — a closed depository/credit/loan/investment/other
        # vocabulary, not a user-authored CATEGORY.
        "account_type": DataClass.TXN_TYPE,
        "is_observed": DataClass.TXN_TYPE,
        "observation_source": DataClass.TXN_TYPE,
        "rate_source": DataClass.TXN_TYPE,
        "balance_date": DataClass.TXN_DATE,
        "rate_published_date": DataClass.TXN_DATE,
        "days_since_observed": DataClass.AGGREGATE,
        "reconciliation_delta": DataClass.BALANCE,
        "account_balance": DataClass.BALANCE,
        "account_balance_home": DataClass.BALANCE,
    },
    parameter_classes={"from_date": DataClass.TXN_DATE, "to_date": DataClass.TXN_DATE},
    columns=(
        OutputColumn(
            "account_id", "Owning account identifier; grain key.", DataClass.RECORD_ID
        ),
        OutputColumn("account_name", "Account display name.", DataClass.USER_NOTE),
        OutputColumn(
            "currency_code",
            "The account's own currency; null means unknown.",
            DataClass.CURRENCY,
        ),
        OutputColumn(
            "home_currency_code",
            "The profile's home currency; null until chosen.",
            DataClass.CURRENCY,
        ),
        OutputColumn(
            "account_type",
            "Canonical classification: depository, credit, loan, investment, other.",
            DataClass.TXN_TYPE,
        ),
        OutputColumn(
            "is_observed",
            "False means the balance is carried forward from an earlier observation.",
            DataClass.TXN_TYPE,
        ),
        OutputColumn(
            "observation_source",
            "ofx, tabular, assertion, or plaid; null when interpolated.",
            DataClass.TXN_TYPE,
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
            "days_since_observed",
            "Days since the balance was last actually observed; 0 on an observed day.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "reconciliation_delta",
            "Observed minus transaction-derived; null on interpolated days.",
            DataClass.BALANCE,
            # `balance` for its rendering contract, not because a discrepancy is
            # a position: signed, and uncoloured. Mirrors balance_drift.drift's
            # own reasoning exactly — a `flow` would paint a positive
            # discrepancy green, and `delta` demands a favourable direction
            # that does not exist here, since drifting either way is equally
            # wrong. Not the brief's suggested `flow` fallback: `flow` carries
            # the same income/expense colouring problem `delta` does, and this
            # column has no more of a favourable direction than
            # balance_drift.drift, which already settled on `balance`.
            money_kind="balance",
        ),
        OutputColumn(
            "account_balance",
            "In currency_code.",
            DataClass.BALANCE,
            money_kind="balance",
        ),
        OutputColumn(
            "account_balance_home",
            "In home_currency_code; null when unpriced.",
            DataClass.BALANCE,
            money_kind="balance",
            currency_basis="home",
        ),
    ),
    semantics=ReportSemantics(
        unit="currency",
        currency="currency_code",
        sign="account_balance is a signed position: positive an asset, negative a liability",
        kind="position",
        valuation_basis=(
            "daily balance carried forward from the latest observation on or "
            "before balance_date"
        ),
        fx_basis=(
            "each row is one account on one date, priced from its own "
            "currency_code; account_balance_home is already in "
            "home_currency_code and is priced from it; a row that cannot be "
            "priced leaves the whole report segmented per currency_code, "
            "never blended"
        ),
        fx_date="balance_date",
        time_basis="one row per account per day; latest day when no range is given",
        denominator=None,
        comparison_window=None,
        exclusions=(
            "accounts excluded from net worth",
            "archived accounts after their archive date",
        ),
        provenance=(
            "reports.net_worth_accounts",
            "core.fct_balances_daily",
            "core.dim_accounts",
            "core.fct_exchange_rates_effective",
        ),
    ),
    class_downgrades={
        "days_since_observed": (
            "an integer day count (balance_date minus the account's last "
            "observed date), not the date itself; derivation classes it "
            "TXN_DATE because it is computed by subtracting two TXN_DATE "
            "columns, but the magnitude alone reveals neither date — the "
            "mis-declaration balance_drift.days_since_assertion made is not "
            "repeated here"
        ),
    },
    default_columns=(
        "account_name",
        "currency_code",
        "account_balance",
        "account_balance_home",
    ),
)
def net_worth_accounts(
    db: Database,  # contract handle; this runner builds pure SQL
    *,
    from_date: str | None = None,
    to_date: str | None = None,
) -> ReportQuery:
    """Net worth per account per day: the account-grain rung of the ladder.

    One row per (account_id, balance_date), in the account's own currency_code
    and in the profile's home currency. Defaults to the latest available day
    when no range is given.

    Args:
        db: Open read-only database connection.
        from_date: Lower bound (inclusive) as 'YYYY-MM-DD'; leaves the upper
            end open when given alone.
        to_date: Upper bound (inclusive) as 'YYYY-MM-DD'; leaves the lower
            end open when given alone.

    Examples:
        reports(report_id="core:net_worth_accounts")
        reports(report_id="core:net_worth_accounts", parameters={"to_date": "2026-03-31"})
    """
    rng = resolve_date_range(
        from_date, to_date, report_id=_REPORT_ID, view=REPORTS_NET_WORTH_ACCOUNTS
    )
    sql = f"""
        SELECT account_id, account_name, currency_code, home_currency_code,
               account_type, is_observed, observation_source, rate_source,
               balance_date, rate_published_date, days_since_observed,
               reconciliation_delta, account_balance, account_balance_home
        FROM {REPORTS_NET_WORTH_ACCOUNTS.full_name}
        WHERE 1=1{rng.where_sql}
        ORDER BY balance_date, account_name, account_id
    """  # noqa: S608  # TableRef interpolation, static column list
    actions = [
        "Run reports(report_id='core:net_worth') for the single home-currency total",
        "Run reports(report_id='core:net_worth_currencies') for the currency-level breakdown",
    ]
    return ReportQuery(sql, rng.params, actions=actions, period=rng.period)
