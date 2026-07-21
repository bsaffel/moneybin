"""Service-backed reports that share the SQL report catalog/result contract."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from typing import Literal, cast

from pydantic import JsonValue

from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.catalog import ServiceReportSpec
from moneybin.reports._framework.contract import (
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
        DataClass.RECORD_ID,
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
        "resolved transaction-adjusted daily positions on or before the "
        "resolved balance_date"
    ),
    fx_basis="no FX conversion in v1; assumes single-currency inputs",
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
        "net_worth",
        "Resolved transaction-adjusted period-end position.",
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
    valuation_basis=(
        "last resolved transaction-adjusted daily position in each selected period"
    ),
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

_ISO_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


def _invalid_iso_date(report_id: str, parameter: str) -> UserError:
    return UserError(
        "Report parameter must be an ISO date.",
        code="REPORT_PARAMETER_INVALID_VALUE",
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
            code="REPORT_PARAMETER_INVALID_RANGE",
            details={
                "report_id": "core:networth_history",
                "parameters": ["from_date", "to_date"],
                "relation": "from_date <= to_date",
            },
        )


def _execute_networth(
    db: Database,
    parameters: Mapping[str, JsonValue],
    limit: int,
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

    return build_catalog_execution(
        NETWORTH_REPORT,
        parameters=params,
        records=rows,
        columns=[column.name for column in _SNAPSHOT_COLUMNS],
        column_types=[
            "DATE",
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
    limit: int,
) -> CatalogReportExecution:
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
    column_types = [
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
