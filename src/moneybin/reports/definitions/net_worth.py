"""core:net_worth / `reports net-worth` — the day-grain, fail-closed total."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any, Literal

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    report,
)
from moneybin.reports.definitions._shared import resolve_date_range
from moneybin.tables import REPORTS_NET_WORTH

_REPORT_ID = "core:net_worth"

#: The nine columns `reports.net_worth` itself projects, in its declared order.
_VIEW_COLUMNS = (
    "home_currency_code",
    "balance_date",
    "account_count",
    "carried_forward_count",
    "currency_count",
    "unpriced_currency_count",
    "total_assets",
    "total_liabilities",
    "net_worth",
)

#: DuckDB bucket-boundary expression per interval. ``daily`` needs none: every
#: row is already its own bucket, so the "last date in the bucket" rule below
#: is a no-op and every day survives.
_BUCKET_EXPR: Mapping[str, str] = {
    "daily": "balance_date",
    "weekly": "date_trunc('week', balance_date)",
    "monthly": "date_trunc('month', balance_date)",
}


def _default_columns(parameters: Mapping[str, Any]) -> tuple[str, ...]:
    """Requirement 6: the date, the fail-closed guard, and the headline.

    `change_abs`/`change_pct` only exist in the projection when `interval` was
    given (see the runner), so a static tuple would either name a column an
    unbucketed read never returns or hide the change columns a bucketed one
    always does. `unpriced_currency_count` sits ahead of `net_worth` on the
    unbucketed set — without it a reader sees only a blank cell on an unpriced
    day and no reason for it — but drops out once `change_abs`/`change_pct`
    join the row: five columns overflow requirement 9's 80 characters, and
    the two change columns already say plainly that something is missing
    when they read null.
    """
    if parameters.get("interval") is not None:
        return ("balance_date", "net_worth", "change_abs", "change_pct")
    return ("balance_date", "unpriced_currency_count", "net_worth")


def _recompute_net_worth_and_change(rows: list[dict[str, Any]], currency: str) -> None:
    """Repair after independent conversion (see `_restate_networth_total`).

    Conversion prices every home-basis money column independently, so two
    things drift on a converted read: `net_worth` no longer equals
    `total_assets + total_liabilities` (each rounds separately), and
    `change_abs`/`change_pct` — computed by the runner's own SQL from the
    *original* `net_worth` series — describe the pre-conversion currency.
    Both are restated here from the converted rows, in the row order the SQL
    already returned (chronological), so `change_abs` still reads consecutive
    buckets against each other.
    """
    for row in rows:
        assets = row.get("total_assets")
        liabilities = row.get("total_liabilities")
        if isinstance(assets, Decimal) and isinstance(liabilities, Decimal):
            row["net_worth"] = assets + liabilities
        else:
            row["net_worth"] = None
    if not rows or "change_abs" not in rows[0]:
        # An unbucketed read (`interval` omitted) never projected these
        # columns; nothing to restate.
        return
    prior: Decimal | None = None
    for row in rows:
        net_worth = row["net_worth"]
        if isinstance(net_worth, Decimal) and isinstance(prior, Decimal):
            change_abs = net_worth - prior
            row["change_abs"] = change_abs
            row["change_pct"] = float(change_abs) / float(prior) if prior != 0 else None
        else:
            row["change_abs"] = None
            row["change_pct"] = None
        prior = net_worth


@report(
    report_id=_REPORT_ID,
    name="net_worth",
    view=REPORTS_NET_WORTH,
    classes={
        "home_currency_code": DataClass.CURRENCY,
        "balance_date": DataClass.TXN_DATE,
        "account_count": DataClass.AGGREGATE,
        "carried_forward_count": DataClass.AGGREGATE,
        "currency_count": DataClass.AGGREGATE,
        "unpriced_currency_count": DataClass.AGGREGATE,
        "total_assets": DataClass.BALANCE,
        "total_liabilities": DataClass.BALANCE,
        "net_worth": DataClass.BALANCE,
        "change_abs": DataClass.BALANCE,
        "change_pct": DataClass.AGGREGATE,
    },
    parameter_classes={
        "from_date": DataClass.TXN_DATE,
        "to_date": DataClass.TXN_DATE,
        "interval": DataClass.TXN_TYPE,
    },
    columns=(
        OutputColumn(
            "home_currency_code",
            "The profile's home currency; null until chosen.",
            DataClass.CURRENCY,
        ),
        OutputColumn("balance_date", "Calendar date.", DataClass.TXN_DATE),
        OutputColumn(
            "account_count",
            "Accounts contributing on this date, across every currency.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "carried_forward_count",
            "How many of them are carried forward rather than observed.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "currency_count",
            "Distinct currencies held on this date; unknown counts as one.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "unpriced_currency_count",
            "How many of them had no rate on this date; 0 means the totals "
            "below are complete.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "total_assets",
            "Sum of positive balances in home_currency_code; null when "
            "unpriced_currency_count > 0.",
            DataClass.BALANCE,
            money_kind="balance",
            currency_basis="home",
        ),
        OutputColumn(
            "total_liabilities",
            "Sum of negative balances in home_currency_code, kept negative; "
            "null when unpriced_currency_count > 0.",
            DataClass.BALANCE,
            money_kind="balance",
            currency_basis="home",
        ),
        OutputColumn(
            "net_worth",
            "Headline: total_assets + total_liabilities in "
            "home_currency_code; null when unpriced_currency_count > 0.",
            DataClass.BALANCE,
            money_kind="balance",
            currency_basis="home",
        ),
        OutputColumn(
            "change_abs",
            "Current bucket's net worth minus the immediately preceding "
            "returned bucket's; present only when interval is given.",
            DataClass.BALANCE,
            # A change in a position rather than a spend magnitude, and net
            # worth rising is the good news — matches _HISTORY_COLUMNS'
            # change_abs (service_reports.py), the rung this one replaces.
            money_kind="delta",
            polarity="income",
            currency_basis="home",
        ),
        OutputColumn(
            "change_pct",
            "change_abs divided by the preceding bucket's net worth; null "
            "when the prior value is null or zero.",
            DataClass.AGGREGATE,
        ),
    ),
    semantics=ReportSemantics(
        unit="currency",
        currency="home_currency_code",
        sign=(
            "net worth is a signed position; change is current bucket minus "
            "the immediately preceding returned bucket"
        ),
        kind="position",
        valuation_basis=(
            "daily balance carried forward from the latest observation on or "
            "before balance_date, summed per currency at that day's rate"
        ),
        fx_basis=(
            "every measure is already denominated in home_currency_code — "
            "priced per currency and date before this day-grain total is "
            "summed — so a display-currency read prices FROM "
            "home_currency_code rather than segmenting per row currency"
        ),
        fx_date="balance_date",
        time_basis=(
            "one row per day; latest available day when interval is omitted "
            "and no range is given. With interval, one row per bucket "
            "(date_trunc('week'|'month', balance_date); daily is every day) "
            "holding the bucket's last available date, and the latest-day "
            "default does not apply — an unranged bucketed read buckets the "
            "whole history"
        ),
        denominator="prior period-end net worth for change_pct",
        comparison_window="immediately preceding returned bucket",
        exclusions=(
            "accounts excluded from net worth",
            "archived accounts after their archive date",
            "dates where any held currency is unpriced (measures are null)",
        ),
        provenance=(
            "reports.net_worth",
            "core.fct_balances_daily",
            "core.dim_accounts",
            "core.fct_exchange_rates_effective",
        ),
    ),
    on_converted=_recompute_net_worth_and_change,
    default_columns=_default_columns,
)
def net_worth(
    db: Database,  # contract handle; this runner builds pure SQL
    *,
    from_date: str | None = None,
    to_date: str | None = None,
    interval: Literal["daily", "weekly", "monthly"] | None = None,
) -> ReportQuery:
    """Net worth per day in the home currency: the day-grain rung of the ladder.

    One row per balance_date, fail-closed to null measures on any date where a
    held currency has no rate. Defaults to the latest available day when no
    range or interval is given.

    With interval, one row per bucket instead — the row whose balance_date is
    the bucket's last available date — plus change_abs and change_pct against
    the immediately preceding returned bucket. Passing interval with no range
    buckets the whole history rather than defaulting to the latest day:
    unlike the unbucketed read, a rollup with only its latest bucket would
    have no prior bucket to compare against.

    Args:
        db: Open read-only database connection.
        from_date: Lower bound (inclusive) as 'YYYY-MM-DD'; leaves the upper
            end open when given alone.
        to_date: Upper bound (inclusive) as 'YYYY-MM-DD'; leaves the lower
            end open when given alone.
        interval: daily | weekly | monthly — buckets the range into one row
            per bucket with change_abs/change_pct. Omitted returns the plain
            day-grain rows with no change columns.

    Examples:
        reports(report_id="core:net_worth")
        reports(report_id="core:net_worth", parameters={"interval": "monthly", "from_date": "2026-01-01"})
    """
    if interval is not None and interval not in _BUCKET_EXPR:
        raise ValueError(f"Unknown interval: {interval}")

    view_cols = ", ".join(_VIEW_COLUMNS)

    if interval is None:
        rng = resolve_date_range(
            from_date, to_date, report_id=_REPORT_ID, view=REPORTS_NET_WORTH
        )
        sql = f"""
            SELECT {view_cols}
            FROM {REPORTS_NET_WORTH.full_name}
            WHERE 1=1{rng.where_sql}
            ORDER BY balance_date
        """  # noqa: S608  # TableRef interpolation, static column list
        actions = [
            "Run reports(report_id='core:net_worth', "
            "parameters={'interval': 'monthly'}) for period-over-period change",
            "Run reports(report_id='core:net_worth_currencies') for the "
            "currency-level breakdown",
        ]
        return ReportQuery(sql, rng.params, actions=actions, period=rng.period)

    rng = resolve_date_range(
        from_date,
        to_date,
        report_id=_REPORT_ID,
        view=REPORTS_NET_WORTH,
        default_latest=False,
    )
    bucket_expr = _BUCKET_EXPR[interval]
    sql = f"""
        WITH ranked AS (
            SELECT {view_cols},
                   ROW_NUMBER() OVER (
                       PARTITION BY {bucket_expr} ORDER BY balance_date DESC
                   ) AS rank_in_bucket
            FROM {REPORTS_NET_WORTH.full_name}
            WHERE 1=1{rng.where_sql}
        ), bucketed AS (
            SELECT {view_cols},
                   LAG(net_worth) OVER (ORDER BY balance_date) AS prior_net_worth
            FROM ranked
            WHERE rank_in_bucket = 1
        )
        SELECT {view_cols},
               (net_worth - prior_net_worth)::DECIMAL(18, 2) AS change_abs,
               CAST(net_worth - prior_net_worth AS DOUBLE)
                   / NULLIF(CAST(prior_net_worth AS DOUBLE), 0) AS change_pct
        FROM bucketed
        ORDER BY balance_date
    """  # noqa: S608  # TableRef interpolation, bucket_expr from a closed dict keyed by a validated interval
    actions = [
        "Run reports(report_id='core:net_worth') for the single latest-day total",
        "Run reports(report_id='core:net_worth_accounts') for the account-level breakdown",
    ]
    return ReportQuery(sql, rng.params, actions=actions, period=rng.period)
