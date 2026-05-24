"""Shared vocabulary and helpers for the in-tree report runners.

The enum allowlists are the canonical parameter vocabularies — runners validate
against them and raise ``ValueError`` (the CLI registrar turns that into a clean
``BadParameter``; the MCP decorator into an error envelope).
"""

from __future__ import annotations

from datetime import UTC, datetime

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
