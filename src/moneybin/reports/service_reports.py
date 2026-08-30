"""Service-backed reports that share the SQL report catalog/result contract."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from typing import Any, Literal, cast

from pydantic import JsonValue

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.payloads.networth import (
    NetWorthAccountRow,
    NetWorthCurrencySegment,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.catalog import ServiceReportSpec
from moneybin.reports._framework.contract import (
    ORIGINAL_CURRENCY_COLUMN,
    OutputColumn,
    ParamSpec,
    ReportSemantics,
)
from moneybin.reports._framework.execute import (
    CatalogReportExecution,
    build_catalog_execution,
)
from moneybin.services.networth_service import NetworthService

_SNAPSHOT_COLUMNS = (
    OutputColumn("balance_date", "Resolved snapshot date.", DataClass.TXN_DATE),
    OutputColumn(
        "currency_code",
        "ISO 4217 currency this row's totals are denominated in; null means unknown.",
        DataClass.CURRENCY,
    ),
    OutputColumn(
        "net_worth",
        "Sum of included balances denominated in currency_code; totals rows only.",
        DataClass.BALANCE,
        # A profile whose liabilities exceed its assets has a negative net
        # worth, and `balance` is the kind that keeps the `−` while leaving a
        # positive position undecorated.
        money_kind="balance",
    ),
    OutputColumn(
        "total_assets",
        "Sum of positive balances in currency_code; totals rows only.",
        DataClass.BALANCE,
        money_kind="balance",
    ),
    OutputColumn(
        "total_liabilities",
        "Sum of negative balances in currency_code, retained as negative; "
        "totals rows only.",
        DataClass.BALANCE,
        money_kind="balance",
    ),
    OutputColumn(
        "account_count",
        "Count of accounts contributing to this currency's totals; totals rows only.",
        DataClass.AGGREGATE,
    ),
    OutputColumn(
        "account_id",
        "Canonical account identifier; null on a currency's totals row.",
        DataClass.RECORD_ID,
    ),
    OutputColumn("account_name", "Account display name.", DataClass.USER_NOTE),
    OutputColumn(
        "account_balance",
        "Balance for the breakdown account.",
        DataClass.BALANCE,
        money_kind="balance",
    ),
    OutputColumn(
        "observation_source",
        "Source of the account balance observation.",
        DataClass.TXN_TYPE,
    ),
)
_SNAPSHOT_CLASSES = {column.name: column.data_class for column in _SNAPSHOT_COLUMNS}
_SNAPSHOT_SEMANTICS = ReportSemantics(
    unit="currency",
    currency="currency_code",
    sign=(
        "assets and positive account balances are positive; liabilities and "
        "negative account balances are negative; net worth is their signed sum"
    ),
    kind="position",
    valuation_basis=(
        "resolved transaction-adjusted daily positions on or before the "
        "resolved balance_date"
    ),
    fx_basis=(
        "each row states one currency's position or one account's balance on one "
        "date, so a requested display currency prices it at that date's rate; a "
        "row that cannot be priced leaves the whole report segmented per "
        "currency_code, never blended"
    ),
    fx_date="balance_date",
    time_basis=(
        "point-in-time position at the latest available balance_date on or before "
        "the requested as_of date; latest available when omitted; balance_date "
        "and headline amounts are null when no position exists"
    ),
    denominator=None,
    comparison_window=None,
    exclusions=("archived accounts", "accounts excluded from net worth"),
    provenance=(
        "reports.net_worth",
        "core.fct_balances_daily",
        "core.dim_accounts",
    ),
)

_HISTORY_COLUMNS = (
    OutputColumn(
        "period", "Start date of the selected period bucket.", DataClass.TXN_DATE
    ),
    OutputColumn(
        "currency_code",
        "ISO 4217 currency this series is denominated in; null means unknown.",
        DataClass.CURRENCY,
    ),
    OutputColumn(
        "net_worth",
        "Resolved transaction-adjusted period-end position in currency_code.",
        DataClass.BALANCE,
        money_kind="balance",
    ),
    OutputColumn(
        "change_abs",
        "Current period-end net worth minus the prior period-end position.",
        DataClass.BALANCE,
        # A change in a position rather than in a spend magnitude, and the one
        # delta in the catalog whose favourable direction is up: net worth
        # rising is the good news, so it reads as income rather than expense.
        money_kind="delta",
        polarity="income",
    ),
    OutputColumn(
        "change_pct",
        "Absolute change divided by prior period-end net worth.",
        DataClass.AGGREGATE,
    ),
)
_HISTORY_CLASSES = {column.name: column.data_class for column in _HISTORY_COLUMNS}
_HISTORY_SEMANTICS = ReportSemantics(
    unit="currency",
    currency="currency_code",
    sign=(
        "net worth is a signed position; change is current minus prior period-end "
        "position"
    ),
    kind="position",
    valuation_basis=(
        "last resolved transaction-adjusted daily position in each selected period"
    ),
    fx_basis=(
        "each period's net worth is aggregated per currency_code, so pricing a row "
        "into one display currency would leave several rows sharing a period; rows "
        "stay segmented per currency_code, never blended"
    ),
    time_basis=(
        "inclusive from_date/to_date window bucketed daily, weekly, or monthly; "
        "period labels are bucket start dates"
    ),
    denominator="prior period-end net worth for change_pct",
    comparison_window="immediately preceding returned period bucket",
    exclusions=(
        "archived accounts",
        "accounts excluded from net worth",
        "empty period buckets",
        "percentage change when prior net worth is zero or absent",
    ),
    provenance=("reports.net_worth",),
)

_ISO_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


def _invalid_iso_date(report_id: str, parameter: str) -> UserError:
    return UserError(
        "Report parameter must be an ISO date.",
        code=error_codes.REPORT_PARAMETER_INVALID_VALUE,
        details={
            "report_id": report_id,
            "parameter": parameter,
            "expected": "ISO date (YYYY-MM-DD)",
        },
    )


def _validate_iso_date(
    parameters: Mapping[str, JsonValue],
    *,
    report_id: str,
    parameter: str,
) -> date | None:
    value = parameters[parameter]
    if value is None:
        return None
    text = cast(str, value)
    if _ISO_DATE.fullmatch(text) is None:
        raise _invalid_iso_date(report_id, parameter)
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise _invalid_iso_date(report_id, parameter) from exc


def _validate_networth_parameters(parameters: Mapping[str, JsonValue]) -> None:
    _validate_iso_date(
        parameters,
        report_id="core:networth",
        parameter="as_of",
    )


def _validate_networth_history_parameters(
    parameters: Mapping[str, JsonValue],
) -> None:
    from_date = _validate_iso_date(
        parameters,
        report_id="core:networth_history",
        parameter="from_date",
    )
    to_date = _validate_iso_date(
        parameters,
        report_id="core:networth_history",
        parameter="to_date",
    )
    if from_date is not None and to_date is not None and from_date > to_date:
        raise UserError(
            "Report date range is invalid.",
            code=error_codes.REPORT_PARAMETER_INVALID_RANGE,
            details={
                "report_id": "core:networth_history",
                "parameters": ["from_date", "to_date"],
                "relation": "from_date <= to_date",
            },
        )


def _execute_networth(
    db: Database,
    parameters: Mapping[str, JsonValue],
    limit: int | None,
) -> CatalogReportExecution:
    params = dict(parameters)
    as_of = params["as_of"]
    account_ids = params["account_ids"]
    snapshot = NetworthService(db).current(
        as_of_date=date.fromisoformat(as_of) if isinstance(as_of, str) else None,
        account_ids=cast(list[str], account_ids)
        if isinstance(account_ids, list)
        else None,
    )

    # Two kinds of row, never fused: one per currency carrying that currency's
    # position, then one per account carrying only its own balance. A
    # mixed-currency profile therefore gets one headline per currency instead
    # of one blended figure (multi-currency.md Requirement 5), and a
    # single-currency profile sees exactly the figures it always did.
    #
    # They are separate rows because display conversion prices each row on its
    # own and relabels it into the target currency. A position repeated across
    # its accounts' rows would arrive as several indistinguishable positions,
    # and anything summing them would count it once per account (Decision 7 in
    # the M1K.2 display-conversion record).
    def _totals_row(segment: NetWorthCurrencySegment | None) -> dict[str, Any]:
        return {
            "balance_date": snapshot.balance_date,
            "currency_code": segment.currency_code if segment else None,
            "net_worth": segment.net_worth if segment else None,
            "total_assets": segment.total_assets if segment else None,
            "total_liabilities": segment.total_liabilities if segment else None,
            "account_count": segment.account_count if segment else None,
            "account_id": None,
            "account_name": None,
            "account_balance": None,
            "observation_source": None,
        }

    def _account_row(account: NetWorthAccountRow) -> dict[str, Any]:
        return {
            "balance_date": snapshot.balance_date,
            "currency_code": account.currency_code,
            "net_worth": None,
            "total_assets": None,
            "total_liabilities": None,
            "account_count": None,
            "account_id": account.account_id,
            "account_name": account.display_name,
            "account_balance": account.balance,
            "observation_source": account.observation_source,
        }

    # Totals lead because the row cap is applied as a prefix: a profile holding
    # two dollar accounts and one euro account would otherwise push euro past a
    # small limit and return a page that reads as single-currency. Blend by
    # omission is the same defect as blend by summation. This ordering is what
    # carries the *segmented* read, which merges nothing; a converting one caps
    # after `_restate_networth_total` has run (`truncate_execution`), so its
    # totals cannot be cut before they are summed. Taking them from per_currency
    # narrowing the breakdown without narrowing the position — filtering to a
    # dollar account still reports the euro the profile holds.
    rows = [_totals_row(segment) for segment in snapshot.per_currency]
    rows.extend(_account_row(account) for account in snapshot.per_account)
    if not rows:
        rows = [_totals_row(None)]

    return build_catalog_execution(
        NETWORTH_REPORT,
        parameters=params,
        records=rows,
        columns=[column.name for column in _SNAPSHOT_COLUMNS],
        column_types=[
            "DATE",
            "VARCHAR",
            "DECIMAL(18,2)",
            "DECIMAL(18,2)",
            "DECIMAL(18,2)",
            "BIGINT",
            "VARCHAR",
            "VARCHAR",
            "DECIMAL(18,2)",
            "VARCHAR",
        ],
        max_rows=limit,
        actions=[
            "Run reports(report_id='core:networth_history', "
            "parameters={'from_date': 'YYYY-MM-DD', 'to_date': 'YYYY-MM-DD'}) "
            "for the time series",
            "Run accounts_balances(view='history', reference='<account>') "
            "to drill into one account",
            "Run accounts(include_closed=True) to inspect closed or excluded accounts",
        ],
        period=(
            snapshot.balance_date.isoformat()
            if snapshot.balance_date is not None
            else None
        ),
        sql=None,
    )


def _execute_networth_history(
    db: Database,
    parameters: Mapping[str, JsonValue],
    limit: int | None,
) -> CatalogReportExecution:
    params = dict(parameters)
    from_date = date.fromisoformat(str(params["from_date"]))
    to_date = date.fromisoformat(str(params["to_date"]))
    interval = str(params["interval"])
    payload = NetworthService(db).history(from_date, to_date, interval=interval)
    rows = [
        {
            "period": point.period,
            "currency_code": point.currency_code,
            "net_worth": point.net_worth,
            "change_abs": point.change_abs,
            "change_pct": point.change_pct,
        }
        for point in payload.points
    ]
    column_types = [
        "VARCHAR",
        "VARCHAR",
        _decimal_column_type(rows, "net_worth", fallback="DECIMAL(38,2)"),
        _decimal_column_type(rows, "change_abs", fallback="DECIMAL(38,2)"),
        _decimal_column_type(rows, "change_pct", fallback="DOUBLE"),
    ]
    return build_catalog_execution(
        NETWORTH_HISTORY_REPORT,
        parameters=params,
        records=rows,
        columns=[column.name for column in _HISTORY_COLUMNS],
        column_types=column_types,
        max_rows=limit,
        actions=[
            "Run reports(report_id='core:networth') for a single-date account breakdown",
            "Rerun reports(report_id='core:networth_history', "
            "parameters={'from_date': 'YYYY-MM-DD', 'to_date': 'YYYY-MM-DD', "
            "'interval': 'weekly'}) for finer resolution",
        ],
        period=f"{from_date.isoformat()} to {to_date.isoformat()} ({interval})",
        sql=None,
    )


def _decimal_column_type(
    rows: Sequence[Mapping[str, object]],
    column: str,
    *,
    fallback: str,
) -> str:
    """Describe retained Decimal values without narrowing their scale."""
    values = [row[column] for row in rows if row[column] is not None]
    if not values or not all(isinstance(value, Decimal) for value in values):
        return fallback

    decimals = cast(list[Decimal], values)
    scale = max(max(-cast(int, value.as_tuple().exponent), 0) for value in decimals)
    integer_digits = max(
        max(
            len(value.as_tuple().digits) + cast(int, value.as_tuple().exponent),
            0,
        )
        for value in decimals
    )
    precision = max(integer_digits + scale, 1)
    if precision > 38:
        raise ValueError(f"{column} exceeds DuckDB DECIMAL(38) precision")
    return f"DECIMAL({precision},{scale})"


def _sum_money(rows: list[dict[str, Any]], column: str) -> Decimal | None:
    """Total one money column across rows, or ``None`` if no row carries one."""
    values = [row[column] for row in rows if isinstance(row[column], Decimal)]
    return sum(values, Decimal(0)) if values else None


def _restate_networth_total(rows: list[dict[str, Any]], currency: str) -> None:
    """Collapse the converted totals into one headline and restate its identity.

    Pricing a multi-currency snapshot into one currency breaks two things.
    Conversion relabels every row into the target, so the per-currency split
    ``_totals_row`` emits becomes several totals rows all claiming the same
    unit — and a consumer reading "the totals row" gets an arbitrary fraction
    of the position, which is the blend-by-omission the split exists to
    prevent. Separately, each money column converts and rounds independently,
    so the three round apart: assets ``1.00`` and liabilities ``-0.01`` at rate
    ``0.5`` give ``0.50`` and ``-0.01`` while net worth ``0.99`` gives ``0.50``,
    a report whose own columns no longer add up.

    Runs only after a conversion, so the rows are known to share a unit before
    anything is summed. Account-breakdown rows hold nulls in all three headline
    fields, so only a totals row states the identity and only a totals row is
    touched; they keep their order after the collapsed headline.

    What it deliberately does not do is make the headline equal the sum of the
    displayed account rows. It sums the per-currency totals, each converted and
    rounded once, so two 0.01 EUR accounts at rate 0.5 display 0.01 each while
    their 0.02 EUR total converts to 0.01 — the headline is a cent under what
    the visible rows add to. Summing the account rows instead would trade that
    for two worse things: rounding error that grows with the number of
    accounts, and a headline that follows an ``account_ids`` filter, so
    narrowing the breakdown to one dollar account would silently report that
    account as the whole position. The headline answers "what is this profile
    worth", which is why it is built from the totals and why filtering narrows
    only the rows beneath it.
    """
    totals = [row for row in rows if row["account_id"] is None]
    if not totals:
        return
    head = totals[0]
    if len(totals) > 1:
        counts = [row["account_count"] for row in totals if row["account_count"]]
        head["total_assets"] = _sum_money(totals, "total_assets")
        head["total_liabilities"] = _sum_money(totals, "total_liabilities")
        head["account_count"] = sum(counts) if counts else None
        head["currency_code"] = currency
        # Several currencies were summed, so no one stored rate priced this
        # figure and the head row's own original would name whichever currency
        # happened to sort first. The rates are all still on the envelope; the
        # per-account rows beneath keep theirs.
        head[ORIGINAL_CURRENCY_COLUMN] = None
        rows[:] = [head, *(row for row in rows if row["account_id"] is not None)]
    assets = head["total_assets"]
    liabilities = head["total_liabilities"]
    if isinstance(assets, Decimal) and isinstance(liabilities, Decimal):
        head["net_worth"] = assets + liabilities


NETWORTH_REPORT = ServiceReportSpec(
    report_id="core:networth",
    name="networth",
    description=(
        "Current or as-of net worth snapshot with per-account breakdown. "
        "Rows come in two kinds: totals rows first (account_id null) — one per "
        "currency held, or exactly one carrying the summed position when "
        "display_currency prices the read — then one row per account carrying "
        "only its own balance. Amounts are in each row's own currency_code."
    ),
    parameters=(
        ParamSpec(
            "as_of",
            str | None,
            None,
            False,
            "ISO date (YYYY-MM-DD); latest available when omitted.",
            DataClass.TXN_DATE,
        ),
        ParamSpec(
            "account_ids",
            list[str] | None,
            None,
            False,
            "Account IDs included in the breakdown; headline totals stay global.",
            DataClass.RECORD_ID,
        ),
    ),
    columns=_SNAPSHOT_COLUMNS,
    semantics=_SNAPSHOT_SEMANTICS,
    classes=_SNAPSHOT_CLASSES,
    examples=(
        'reports(report_id="core:networth")',
        ('reports(report_id="core:networth", parameters={"as_of": "2026-07-01"})'),
    ),
    executor=_execute_networth,
    validator=_validate_networth_parameters,
    on_converted=_restate_networth_total,
)

NETWORTH_HISTORY_REPORT = ServiceReportSpec(
    report_id="core:networth_history",
    name="networth_history",
    description=(
        "Net worth history with period-over-period absolute and percentage change. "
        "Amounts are denominated in each row's own currency_code."
    ),
    parameters=(
        ParamSpec(
            "from_date",
            str,
            None,
            True,
            "Inclusive ISO start date (YYYY-MM-DD).",
            DataClass.TXN_DATE,
        ),
        ParamSpec(
            "to_date",
            str,
            None,
            True,
            "Inclusive ISO end date (YYYY-MM-DD).",
            DataClass.TXN_DATE,
        ),
        ParamSpec(
            "interval",
            Literal["daily", "weekly", "monthly"],
            "monthly",
            False,
            "Period bucket: daily, weekly, or monthly.",
            DataClass.TXN_TYPE,
        ),
    ),
    columns=_HISTORY_COLUMNS,
    semantics=_HISTORY_SEMANTICS,
    classes=_HISTORY_CLASSES,
    examples=(
        (
            'reports(report_id="core:networth_history", '
            'parameters={"from_date": "2026-01-01", '
            '"to_date": "2026-07-01", "interval": "monthly"})'
        ),
    ),
    executor=_execute_networth_history,
    validator=_validate_networth_history_parameters,
)

SERVICE_REPORTS: tuple[ServiceReportSpec, ...] = (
    NETWORTH_REPORT,
    NETWORTH_HISTORY_REPORT,
)
