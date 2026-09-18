"""Shared vocabulary and helpers for the in-tree report runners.

The enum allowlists are the canonical parameter vocabularies — runners validate
against them and raise ``ValueError``. Both surfaces turn that into a clean error
envelope: the CLI via ``handle_cli_errors`` (ValueError → INFRA_INVALID_INPUT),
the MCP decorator via its own classified-error path.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import Binding
from moneybin.tables import TableRef

# Month bound as YYYY-MM. Enforced because the runners canonicalize with
# substr(?, 1, 7), which would let a malformed "2024-1" through and produce
# silently wrong lexicographic window bounds.
_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
# Full ISO date as YYYY-MM-DD (e.g. balance_drift's `since`).
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_month(value: str, param: str) -> None:
    """Raise ValueError if ``value`` is not a YYYY-MM month string."""
    if not _MONTH_RE.match(value):
        raise ValueError(f"{param} must be YYYY-MM, got {value!r}")


def validate_date(value: str, param: str) -> None:
    """Raise ValueError if ``value`` is not a YYYY-MM-DD date string."""
    if not _DATE_RE.match(value):
        raise ValueError(f"{param} must be an ISO date (YYYY-MM-DD), got {value!r}")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"{param} must be an ISO date (YYYY-MM-DD), got {value!r}"
        ) from exc


CASHFLOW_GROUPINGS: tuple[str, ...] = ("account", "category", "account-and-category")
SPENDING_COMPARES: tuple[str, ...] = ("yoy", "mom", "trailing")
RECURRING_STATUSES: tuple[str, ...] = ("active", "inactive", "all")
RECURRING_CADENCES: tuple[str, ...] = (
    "weekly",
    "biweekly",
    "monthly",
    "quarterly",
    "yearly",
    "irregular",
)
MERCHANTS_SORTS: dict[str, str] = {
    "spend": "total_spend DESC",
    "count": "txn_count DESC",
    "recent": "last_seen DESC",
}
LARGE_TXN_ANOMALIES: tuple[str, ...] = ("none", "account", "category")
DRIFT_STATUSES: tuple[str, ...] = (
    "drift",
    "warning",
    "clean",
    "no-data",
    "currency-mismatch",
    "all",
)
REALIZED_FX_COVERAGE: tuple[str, ...] = ("complete", "incomplete", "all")


def default_window(months: int = 12) -> tuple[str, str]:
    """Return (from_month, to_month) as YYYY-MM strings for the last N months.

    Uses UTC so the window stays stable across deploy timezones and matches a
    manual query against DuckDB's ``current_date`` (no TZ-aware date type; reads
    the system clock, treated as UTC). A local-time read would drift by a month
    near calendar boundaries.
    """
    end = datetime.now(UTC).replace(day=1)
    year = end.year
    month = end.month - (months - 1)
    while month <= 0:
        month += 12
        year -= 1
    start = end.replace(year=year, month=month)
    return start.strftime("%Y-%m"), end.strftime("%Y-%m")


def resolve_window(
    from_month: str | None,
    to_month: str | None,
    *,
    report_id: str | None = None,
) -> tuple[str | None, str | None, str | None, str | None]:
    """Default to the last 12 months when both bounds are omitted.

    Returns ``(from_month, to_month, period, hint)`` — ``period`` is the
    human-readable window for the envelope, and ``hint`` is the "widen the
    window" actions note when the window was defaulted (else ``None``). Shared
    by the time-windowed runners so the defaulting and the hint string stay in
    lockstep.
    """
    if from_month is not None:
        validate_month(from_month, "from_month")
    if to_month is not None:
        validate_month(to_month, "to_month")
    defaulted = from_month is None and to_month is None
    if defaulted:
        from_month, to_month = default_window(12)
    # A one-sided window still filters; report it so the envelope's period signals
    # that a temporal bound was applied rather than reading as "no filter" (None).
    if from_month and to_month:
        period = f"{from_month} to {to_month}"
    elif from_month:
        period = f"from {from_month}"
    elif to_month:
        period = f"through {to_month}"
    else:
        period = None
    hint = None
    if defaulted:
        report_call = (
            f"reports(report_id={report_id!r}, "
            "parameters={'from_month': 'YYYY-MM', 'to_month': 'YYYY-MM'})"
            if report_id is not None
            else "reports with explicit from_month and to_month parameters"
        )
        hint = f"Rerun {report_call} to widen or shift the last 12 months"
    return from_month, to_month, period, hint


def _invalid_date_range_param(report_id: str, parameter: str) -> UserError:
    """R9's inverted/malformed date-range refusal, shared by every net-worth rung."""
    return UserError(
        "Report parameter must be an ISO date.",
        code=error_codes.REPORT_PARAMETER_INVALID_VALUE,
        details={
            "report_id": report_id,
            "parameter": parameter,
            "expected": "ISO date (YYYY-MM-DD)",
        },
    )


def _parse_range_bound(
    value: str | None, *, report_id: str, parameter: str
) -> date | None:
    """Parse one optional ISO-date bound, or raise on a malformed one.

    The regex fixes the ``YYYY-MM-DD`` shape first — rejecting something like
    ``"2026/01/01"`` — so ``date.fromisoformat`` only has to reject a
    shape-valid but impossible calendar day (e.g. ``"2026-02-30"``).
    """
    if value is None:
        return None
    if _DATE_RE.fullmatch(value) is None:
        raise _invalid_date_range_param(report_id, parameter)
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise _invalid_date_range_param(report_id, parameter) from exc


@dataclass(frozen=True, slots=True)
class DateRange:
    """A validated ``balance_date`` window, ready to splice into a runner's SQL."""

    where_sql: str
    """The ``WHERE`` fragment to append, with a leading space; never empty."""
    params: list[Binding]
    """Positional bindings for ``where_sql``'s ``?`` placeholders, in order."""
    period: str | None
    """Human window for ``ReportQuery.period``; ``None`` for the latest-day default."""


def resolve_date_range(
    from_date: str | None,
    to_date: str | None,
    *,
    report_id: str,
    view: TableRef,
) -> DateRange:
    """Validate an optional ``balance_date`` range and build its SQL fragment.

    Shared by every net-worth rung (``core:net_worth``, ``core:net_worth_currencies``,
    ``core:net_worth_accounts``) so the inverted-range refusal and the one-sided
    "stays open" rule (spec §Data Model) cannot drift between them. A one-sided
    bound never collapses to a single day: ``from_date`` alone leaves the upper
    end open, and ``to_date`` alone leaves the lower end open. Neither bound
    given defaults to the latest available day, mirroring the retired
    ``NetworthService.current()``.

    Validation raises before any SQL is built, so an inverted or malformed
    range never reaches the database.
    """
    parsed_from = _parse_range_bound(
        from_date, report_id=report_id, parameter="from_date"
    )
    parsed_to = _parse_range_bound(to_date, report_id=report_id, parameter="to_date")
    if parsed_from is not None and parsed_to is not None and parsed_from > parsed_to:
        raise UserError(
            "Report date range is invalid.",
            code=error_codes.REPORT_PARAMETER_INVALID_RANGE,
            details={
                "report_id": report_id,
                "parameters": ["from_date", "to_date"],
                "relation": "from_date <= to_date",
            },
        )
    if from_date and to_date:
        return DateRange(
            where_sql=" AND balance_date >= ? AND balance_date <= ?",
            params=[
                Binding(from_date, DataClass.TXN_DATE),
                Binding(to_date, DataClass.TXN_DATE),
            ],
            period=f"{from_date} to {to_date}",
        )
    if from_date:
        return DateRange(
            where_sql=" AND balance_date >= ?",
            params=[Binding(from_date, DataClass.TXN_DATE)],
            period=f"from {from_date}",
        )
    if to_date:
        return DateRange(
            where_sql=" AND balance_date <= ?",
            params=[Binding(to_date, DataClass.TXN_DATE)],
            period=f"through {to_date}",
        )
    latest_day_sql = (
        f" AND balance_date = (SELECT MAX(balance_date) FROM {view.full_name})"  # noqa: S608  # TableRef interpolation, not a user value
    )
    return DateRange(where_sql=latest_day_sql, params=[], period=None)
