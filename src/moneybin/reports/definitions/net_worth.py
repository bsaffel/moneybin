"""core:net_worth / `reports net-worth` — the day-grain, fail-closed total."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any, Literal

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import (
    Binding,
    OutputColumn,
    ReportQuery,
    ReportSemantics,
    report,
)
from moneybin.reports.definitions._shared import (
    DateRange,
    resolve_date_range,
    unanchored_candidates_ctes,
)
from moneybin.tables import PROFILE_SETTINGS, REPORTS_NET_WORTH

_REPORT_ID = "core:net_worth"

#: The ten columns `reports.net_worth` itself projects, in its declared order.
_VIEW_COLUMNS = (
    "home_currency_code",
    "balance_date",
    "account_count",
    "carried_forward_count",
    "currency_count",
    "unpriced_currency_count",
    "unanchored_account_count",
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


def _source_cte(view_cols: str, rng: DateRange) -> tuple[str, list[Binding]]:
    """The rows a read starts from: the view's filtered rows, plus one synthesized row.

    The synthesized row appears only when an explicit range filtered to nothing
    and an eligible candidate is dateable inside it — the one row the view cannot
    date for a range it never saw.
    """
    base = f"""
        filtered AS (
            SELECT {view_cols}
            FROM {REPORTS_NET_WORTH.full_name}
            WHERE 1=1{rng.where_sql}
        )
    """  # noqa: S608  # TableRef interpolation, static column list
    if not rng.is_ranged:
        return (
            f"{base}, source AS (SELECT {view_cols} FROM filtered)",  # noqa: S608  # static column list
            list(rng.params),
        )
    candidates_sql, candidate_params = unanchored_candidates_ctes(rng)
    sql = f"""
        {base},
        {candidates_sql},
        synthesized AS (
            SELECT
                (SELECT p.home_currency FROM {PROFILE_SETTINGS.full_name} AS p)
                    AS home_currency_code,
                MIN(synthesis_date) AS balance_date,
                0 AS account_count,
                0 AS carried_forward_count,
                0 AS currency_count,
                0 AS unpriced_currency_count,
                CAST(COUNT(*) AS INTEGER) AS unanchored_account_count,
                CAST(NULL AS DECIMAL(18, 2)) AS total_assets,
                CAST(NULL AS DECIMAL(18, 2)) AS total_liabilities,
                CAST(NULL AS DECIMAL(18, 2)) AS net_worth
            FROM unanchored_candidates
            HAVING COUNT(*) > 0 AND NOT EXISTS (SELECT 1 FROM filtered)
        ),
        source AS (
            SELECT {view_cols} FROM filtered
            UNION ALL
            SELECT {view_cols} FROM synthesized
        )
    """  # noqa: S608  # TableRef interpolation, static column list
    return sql, [*rng.params, *candidate_params]


def _default_columns(parameters: Mapping[str, Any]) -> tuple[str, ...]:
    """Requirement 6: the date, the fail-closed guard, and the headline.

    `unpriced_currency_count` stays in the set whether or not `interval` is
    given — it is not redundant with the change columns once they join the
    row. `change_abs`/`change_pct` go null on exactly the same unpriced
    dates `net_worth` does, *and* on the first returned bucket (no prior to
    compare against) and after any other unpriced bucket, so a null change
    column alone does not tell a reader which of those it is. Only
    `unpriced_currency_count` answers that.

    `change_pct` drops out of the bucketed default set: `balance_date`,
    `unpriced_currency_count`, `net_worth`, `change_abs` already measures 74
    characters, and a fifth column crosses requirement 9's 80-character
    bound. It stays one `--wide` away rather than pushed onto a reader who
    only asked for the trend.
    """
    if parameters.get("interval") is not None:
        return ("balance_date", "unpriced_currency_count", "net_worth", "change_abs")
    return ("balance_date", "unpriced_currency_count", "net_worth")


def _recompute_net_worth_and_change(rows: list[dict[str, Any]], currency: str) -> None:
    """Repair after independent conversion.

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
        "unanchored_account_count": DataClass.AGGREGATE,
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
            "Currency of this row's amounts: the profile's home currency, or "
            "the display currency on a converted read. Null until chosen.",
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
            "unanchored_account_count",
            "Accounts in net worth that hold value (holdings or transaction "
            "activity) but have no balance observation; 0 means none. The "
            "totals are null while it is above 0.",
            DataClass.AGGREGATE,
        ),
        OutputColumn(
            "total_assets",
            "Sum of positive balances in home_currency_code; null when "
            "unpriced_currency_count or unanchored_account_count > 0.",
            DataClass.BALANCE,
            money_kind="balance",
            currency_basis="home",
        ),
        OutputColumn(
            "total_liabilities",
            "Sum of negative balances in home_currency_code, kept negative; "
            "null when unpriced_currency_count or unanchored_account_count > 0.",
            DataClass.BALANCE,
            money_kind="balance",
            currency_basis="home",
        ),
        OutputColumn(
            "net_worth",
            "Headline: total_assets + total_liabilities in "
            "home_currency_code; null when unpriced_currency_count or "
            "unanchored_account_count > 0.",
            DataClass.BALANCE,
            money_kind="balance",
            currency_basis="home",
        ),
        OutputColumn(
            "change_abs",
            "Current bucket's net worth minus the immediately preceding "
            "returned bucket's; present only when interval is given. Null "
            "on the first returned bucket and whenever either bucket's "
            "net_worth is null.",
            DataClass.BALANCE,
            # A change in a position rather than a spend magnitude, and net
            # worth rising is the good news.
            money_kind="delta",
            polarity="income",
            currency_basis="home",
        ),
        OutputColumn(
            "change_pct",
            "change_abs divided by the preceding bucket's net worth. Null "
            "on the first returned bucket, whenever either bucket's "
            "net_worth is null, or when the preceding value is zero.",
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
            "(weekly buckets are ISO weeks starting Monday, via "
            "date_trunc('week', balance_date); monthly via "
            "date_trunc('month', balance_date); daily is every day) "
            "holding the bucket's last available date, and the latest-day "
            "default does not apply — an unranged bucketed read buckets the "
            "whole history. Rows are returned oldest-first, so a row cap "
            "keeps the earliest buckets, not the most recent"
        ),
        denominator="prior period-end net worth for change_pct",
        comparison_window="immediately preceding returned bucket",
        exclusions=(
            "accounts excluded from net worth",
            "archived accounts after their archive date",
            "dates where any held currency is unpriced (measures are null)",
            "dates where an eligible account holding value has no balance "
            "observation (measures are null)",
        ),
        provenance=(
            "reports.net_worth",
            "core.fct_balances_daily",
            "core.dim_accounts",
            "core.fct_exchange_rates_effective",
            "core.dim_unanchored_accounts",
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
    the immediately preceding returned bucket. Weekly buckets are ISO weeks
    starting Monday (DuckDB's date_trunc('week', ...)), not Sunday-start.
    Passing interval with no range buckets the whole history rather than
    defaulting to the latest day: unlike the unbucketed read, a rollup with
    only its latest bucket would have no prior bucket to compare against.

    Rows are returned oldest-first (ORDER BY balance_date), on both the
    bucketed and unbucketed reads, so a row cap keeps the earliest dates in
    the requested range rather than the most recent — bound a recent window
    with from_date instead of relying on a limit.

    Args:
        db: Open read-only database connection.
        from_date: Lower bound (inclusive) as 'YYYY-MM-DD'; leaves the upper
            end open when given alone.
        to_date: Upper bound (inclusive) as 'YYYY-MM-DD'; leaves the lower
            end open when given alone. An explicit range with no balance
            rows, while an eligible account holding value has no balance
            observation, returns one row dated inside the range with null
            measures and unanchored_account_count set.
        interval: daily | weekly | monthly — buckets the range into one row
            per bucket with change_abs/change_pct. Weekly buckets are ISO
            weeks starting Monday. Omitted returns the plain day-grain rows
            with no change columns.

    Examples:
        reports(report_id="core:net_worth")
        reports(report_id="core:net_worth", parameters={"interval": "monthly", "from_date": "2026-01-01"})
    """
    if interval is not None and interval not in _BUCKET_EXPR:
        raise UserError(
            "Report parameter has an invalid value.",
            code=error_codes.REPORT_PARAMETER_INVALID_VALUE,
            details={
                "report_id": _REPORT_ID,
                "parameter": "interval",
                "expected": "one of daily, weekly, monthly",
            },
        )

    view_cols = ", ".join(_VIEW_COLUMNS)

    if interval is None:
        rng = resolve_date_range(
            from_date, to_date, report_id=_REPORT_ID, view=REPORTS_NET_WORTH
        )
        source_sql, params = _source_cte(view_cols, rng)
        sql = f"""
            WITH {source_sql}
            SELECT {view_cols} FROM source
            ORDER BY balance_date
        """  # noqa: S608  # CTE text built above from TableRefs and a static column list
        actions = [
            "Run reports(report_id='core:net_worth', "
            "parameters={'interval': 'monthly'}) for period-over-period change",
            "Run reports(report_id='core:net_worth_currencies') for the "
            "currency-level breakdown",
        ]
        return ReportQuery(sql, params, actions=actions, period=rng.period)

    rng = resolve_date_range(
        from_date,
        to_date,
        report_id=_REPORT_ID,
        view=REPORTS_NET_WORTH,
        default_latest=False,
    )
    bucket_expr = _BUCKET_EXPR[interval]
    source_sql, params = _source_cte(view_cols, rng)
    sql = f"""
        WITH {source_sql},
        ranked AS (
            SELECT {view_cols},
                   ROW_NUMBER() OVER (
                       PARTITION BY {bucket_expr} ORDER BY balance_date DESC
                   ) AS rank_in_bucket
            FROM source
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
        "Set from_date to bound a recent window — rows return oldest-first, "
        "so a row limit keeps the earliest buckets, not the most recent",
    ]
    return ReportQuery(sql, params, actions=actions, period=rng.period)
