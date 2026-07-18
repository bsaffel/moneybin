"""Service-backed reports that share the SQL report catalog/result contract."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Literal, cast

from pydantic import JsonValue

from moneybin.database import Database
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.catalog import ServiceReportSpec
from moneybin.reports._framework.contract import (
    OutputColumn,
    ParamSpec,
    ReportSemantics,
)
from moneybin.reports._framework.execute import (
    CatalogReportResult,
    build_catalog_result,
)
from moneybin.services.networth_service import NetworthService

_SNAPSHOT_COLUMNS = (
    OutputColumn("balance_date", "Resolved snapshot date.", DataClass.TXN_DATE),
    OutputColumn("net_worth", "Sum of all included balances.", DataClass.BALANCE),
    OutputColumn("total_assets", "Sum of positive balances.", DataClass.BALANCE),
    OutputColumn(
        "total_liabilities",
        "Sum of negative balances, retained as negative.",
        DataClass.BALANCE,
    ),
    OutputColumn(
        "account_count",
        "Count of accounts contributing to the headline totals.",
        DataClass.AGGREGATE,
    ),
    OutputColumn(
        "account_id",
        "Canonical account identifier for the breakdown row.",
        DataClass.ACCOUNT_IDENTIFIER,
    ),
    OutputColumn("account_name", "Account display name.", DataClass.USER_NOTE),
    OutputColumn(
        "account_balance",
        "Balance for the breakdown account.",
        DataClass.BALANCE,
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
    currency="summary.display_currency",
    sign=(
        "assets and positive account balances are positive; liabilities and "
        "negative account balances are negative; net worth is their signed sum"
    ),
    kind="position",
    valuation_basis=(
        "latest resolved daily balance, observed or carried forward, on or "
        "before the resolved balance_date"
    ),
    fx_basis="no FX conversion in v1; assumes single-currency inputs",
    time_basis=(
        "point-in-time position at the latest available balance_date on or before "
        "the requested as_of date; latest available when omitted; when no report "
        "rows exist, the existing service returns a zero position dated current "
        "local date"
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
        "net_worth",
        "Last net-worth position observed in the period.",
        DataClass.BALANCE,
    ),
    OutputColumn(
        "change_abs",
        "Current period-end net worth minus the prior period-end position.",
        DataClass.BALANCE,
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
    currency="summary.display_currency",
    sign=(
        "net worth is a signed position; change is current minus prior period-end "
        "position"
    ),
    kind="position",
    valuation_basis="last observed net-worth position in each selected period",
    fx_basis="no FX conversion in v1; assumes single-currency inputs",
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


def _execute_networth(
    db: Database,
    parameters: Mapping[str, JsonValue],
    limit: int,
) -> CatalogReportResult:
    params = dict(parameters)
    as_of = params["as_of"]
    account_ids = params["account_ids"]
    snapshot = NetworthService(db).current(
        as_of_date=date.fromisoformat(as_of) if isinstance(as_of, str) else None,
        account_ids=cast(list[str], account_ids)
        if isinstance(account_ids, list)
        else None,
    )

    common = {
        "balance_date": snapshot.balance_date,
        "net_worth": snapshot.net_worth,
        "total_assets": snapshot.total_assets,
        "total_liabilities": snapshot.total_liabilities,
        "account_count": snapshot.account_count,
    }
    rows = [
        {
            **common,
            "account_id": account.account_id,
            "account_name": account.display_name,
            "account_balance": account.balance,
            "observation_source": account.observation_source,
        }
        for account in snapshot.per_account
    ]
    if not rows:
        rows = [
            {
                **common,
                "account_id": None,
                "account_name": None,
                "account_balance": None,
                "observation_source": None,
            }
        ]

    return build_catalog_result(
        NETWORTH_REPORT,
        parameters=params,
        records=rows,
        columns=[column.name for column in _SNAPSHOT_COLUMNS],
        max_rows=limit,
        actions=[
            "Use core:networth_history for the time series",
            "Use accounts_balance_history to drill into one account",
            "Use accounts to inspect archived or excluded accounts",
        ],
        period=snapshot.balance_date.isoformat(),
    )


def _execute_networth_history(
    db: Database,
    parameters: Mapping[str, JsonValue],
    limit: int,
) -> CatalogReportResult:
    params = dict(parameters)
    from_date = date.fromisoformat(str(params["from_date"]))
    to_date = date.fromisoformat(str(params["to_date"]))
    interval = str(params["interval"])
    payload = NetworthService(db).history(from_date, to_date, interval=interval)
    rows = [
        {
            "period": point.period,
            "net_worth": point.net_worth,
            "change_abs": point.change_abs,
            "change_pct": point.change_pct,
        }
        for point in payload.points
    ]
    return build_catalog_result(
        NETWORTH_HISTORY_REPORT,
        parameters=params,
        records=rows,
        columns=[column.name for column in _HISTORY_COLUMNS],
        max_rows=limit,
        actions=[
            "Use core:networth for a single-date snapshot with account breakdown",
            "Switch interval to daily or weekly for finer resolution",
        ],
        period=f"{from_date.isoformat()} to {to_date.isoformat()} ({interval})",
    )


NETWORTH_REPORT = ServiceReportSpec(
    report_id="core:networth",
    name="networth",
    description=(
        "Current or as-of net worth snapshot with per-account breakdown. "
        "Amounts are in the currency named by summary.display_currency."
    ),
    parameters=(
        ParamSpec(
            "as_of",
            str | None,
            None,
            False,
            "ISO date (YYYY-MM-DD); latest available when omitted.",
        ),
        ParamSpec(
            "account_ids",
            list[str] | None,
            None,
            False,
            "Account IDs included in the breakdown; headline totals stay global.",
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
)

NETWORTH_HISTORY_REPORT = ServiceReportSpec(
    report_id="core:networth_history",
    name="networth_history",
    description=(
        "Net worth history with period-over-period absolute and percentage change. "
        "Amounts are in the currency named by summary.display_currency."
    ),
    parameters=(
        ParamSpec(
            "from_date",
            str,
            None,
            True,
            "Inclusive ISO start date (YYYY-MM-DD).",
        ),
        ParamSpec(
            "to_date",
            str,
            None,
            True,
            "Inclusive ISO end date (YYYY-MM-DD).",
        ),
        ParamSpec(
            "interval",
            Literal["daily", "weekly", "monthly"],
            "monthly",
            False,
            "Period bucket: daily, weekly, or monthly.",
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
)

SERVICE_REPORTS: tuple[ServiceReportSpec, ...] = (
    NETWORTH_REPORT,
    NETWORTH_HISTORY_REPORT,
)
