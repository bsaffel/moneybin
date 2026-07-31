"""Source guard: a capped report response must not omit a whole currency.

`reports(...)` truncates with a prefix — `build_catalog_execution` keeps
`records[:max_rows]` (`reports/_framework/execute.py`) — so the sort order
decides what a bounded response *can* contain. A currency sorted after the cap
is absent, not ranked lower, and nothing in the envelope says so.

Two distinct failure limbs produce that outcome. Only the first is a property
of the sort keys, which is why this file guards it by scanning source and the
second is guarded behaviourally beside the surface that ranks:

- **Limb A — interleaving.** `currency_code` sorts major to the metric, so the
  cap is handed one currency's rows in full before the next currency starts.
  This scan owns Limb A.
- **Limb B — cross-unit ranking.** A money-derived metric is ranked *across*
  currencies with no `PARTITION BY currency_code`, so the high-denomination
  currency wins every slot. Limb B leaves no trace in the sort keys — the
  offending `ORDER BY` need not mention `currency_code` at all — so no source
  scan can catch it. See
  `test_categorization_service.py::test_impact_queue_spans_currencies_under_cap`.

Why a scan and not fixtures alone: a per-report mixed-currency fixture only
covers the report someone thought to write a fixture for. Limb A has now
reached six sites across three review rounds — four runners, then the curated
`sql_schema` examples, then `cash_flow` / `spending_trend` /
`networth_history` — each fixed where it was found rather than swept for. The
scan fires on the seventh without anyone building a fixture for it.

It covers both channels that reach `reports(...)`: the SQL runners in
`reports/definitions/` and the service methods behind `ServiceReportSpec`
executors. The service channel is derived from what `service_reports` actually
imports, so a new service-backed report inherits the guard instead of escaping
it — `networth_history` escaped the previous definitions-only scan exactly that
way.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest

from moneybin.reports import service_reports
from moneybin.reports.definitions import large_transactions

pytestmark = pytest.mark.unit

# An ORDER BY key list runs to the end of its source line or to the end of the
# string literal holding it, whichever comes first. Both report channels build
# SQL as `sql += " ORDER BY ..."` fragments or as triple-quoted blocks, so this
# boundary captures the whole key list in every real case without parsing SQL
# that f-string interpolation has already made unparseable.
_ORDER_BY = re.compile(r"ORDER BY\s+(?P<keys>[^\"'\n]+)")

# The fix shape: rank within each currency and sort on that rank, so any prefix
# of the result holds every currency that fits. Either the rank column a
# runner's CTE projects, or the window written inline.
_RANK_TOKEN = re.compile(
    r"\brank_in_currency\b|ROW_NUMBER\(\)\s*OVER\s*\(", re.IGNORECASE
)
_CURRENCY_KEY = re.compile(r"\bcurrency_code\b", re.IGNORECASE)

# A window's PARTITION BY names currency_code to *segment* the rank, not to sort
# the response. Blanked before the check so it is never read as a sort key.
_PARTITION_BY = re.compile(r"PARTITION BY[^)]*", re.IGNORECASE)


def _lets_a_cap_omit_a_currency(keys: str) -> bool:
    """True when this key list hands the cap one currency before the next.

    The rank need not lead — an outer time dimension may precede it, since
    truncating whole trailing periods drops them from every currency alike.
    What matters is that a per-currency rank comes *before* any bare
    `currency_code` key: once currency sorts major to the rank, the prefix the
    cap keeps is one currency's rows in full.
    """
    sortable = _PARTITION_BY.sub("", keys)
    currency = _CURRENCY_KEY.search(sortable)
    if currency is None:
        return False
    rank = _RANK_TOKEN.search(sortable)
    return rank is None or rank.start() > currency.start()


# Sites that name currency_code in a sort key and are nonetheless safe, keyed by
# (module, keys). Each value argues why a prefix of this result cannot omit a
# currency — not that truncation is unlikely to reach it.
#
# Set equality below, not a subset: a new offender fails, and so does a stale
# exemption for a site that was since fixed or deleted.
_CURRENCY_SORT_OK: dict[tuple[str, str], str] = {
    (
        "networth_service.py",
        "n.currency_code",
    ): (
        "The query pins one balance_date (the MAX from the `latest` CTE), and "
        "reports.net_worth is grained (balance_date, currency_code), so the "
        "result is exactly one row per currency. Any prefix of k rows holds k "
        "currencies whatever the sort key is — there is no ordering that "
        "survives truncation better."
    ),
}


def _scanned_sources() -> list[Path]:
    """Every module whose SQL can reach a truncated `reports(...)` response.

    Derived, not listed: the definitions package plus the service classes
    `service_reports` imports to build `ServiceReportSpec` rows. A new
    service-backed report inherits the scan by being imported there.
    """
    definitions = sorted(Path(inspect.getfile(large_transactions)).parent.glob("*.py"))
    services = {
        Path(inspect.getfile(member))
        for _, member in inspect.getmembers(service_reports, inspect.isclass)
        if member.__module__.startswith("moneybin.services.")
    }
    return definitions + sorted(services)


def _offenders() -> dict[tuple[str, str], str]:
    """Sort keys that name currency_code without a per-currency rank first."""
    found: dict[tuple[str, str], str] = {}
    for path in _scanned_sources():
        for number, line in enumerate(path.read_text().splitlines(), 1):
            for match in _ORDER_BY.finditer(line):
                keys = match.group("keys").strip()
                if _lets_a_cap_omit_a_currency(keys):
                    found[(path.name, keys)] = f"{path.name}:{number}: ORDER BY {keys}"
    return found


def test_no_report_sort_lets_a_cap_omit_a_currency() -> None:
    """Limb A: currency_code must never sort major to the ranking metric.

    Leading with the non-currency dimension is not a defence. `cash_flow`
    groups on a caller-chosen dimension and `spending_trend` on category, so a
    single month holds several rows per currency; `ORDER BY year_month,
    currency_code` then hands the whole month's budget to the
    lexicographically-first currency before the next one starts. The previous
    scan exempted both on the theory that a leading `year_month` makes
    truncation drop tail *months* across all currencies alike. That holds only
    when each (month, currency) is exactly one row, which is true of neither.
    """
    offenders = _offenders()

    assert set(offenders) == set(_CURRENCY_SORT_OK), (
        "a currency sorted after the row cap is absent from the response, not "
        "ranked lower, and nothing in the envelope says so; rank within each "
        "currency and order by that rank first:\n"
        + "\n".join(
            offenders.get(key, f"{key[0]}: stale exemption for ORDER BY {key[1]}")
            for key in sorted(set(offenders) ^ set(_CURRENCY_SORT_OK))
        )
    )


def test_scan_reaches_both_report_channels() -> None:
    """The scan must cover service-backed reports, not just the runners.

    `networth_history` is a `ServiceReportSpec` whose SQL lives in
    `networth_service`, so a definitions-only scan could not see it — which is
    how it kept a currency-major sort through the round that swept the
    runners. This asserts the derivation actually reaches that module, so
    narrowing `_scanned_sources` back to the definitions package fails here
    rather than silently halving the guard's coverage.
    """
    scanned = {path.name for path in _scanned_sources()}

    assert "cash_flow.py" in scanned
    assert "networth_service.py" in scanned
