"""Shared vocabulary and helpers for the in-tree report runners.

The enum allowlists are the canonical parameter vocabularies — runners validate
against them and raise ``ValueError``. Both surfaces turn that into a clean error
envelope: the CLI via ``handle_cli_errors`` (ValueError → INFRA_INVALID_INPUT),
the MCP decorator via its own classified-error path.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

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
DRIFT_STATUSES: tuple[str, ...] = ("drift", "warning", "clean", "no-data", "all")


_WIDEN_WINDOW_HINT = (
    "Showing the last 12 months — pass from_month='YYYY-MM' and/or "
    "to_month='YYYY-MM' to widen or shift the window."
)


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
    from_month: str | None, to_month: str | None
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
    return from_month, to_month, period, (_WIDEN_WINDOW_HINT if defaulted else None)
