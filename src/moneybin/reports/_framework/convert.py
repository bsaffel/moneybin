"""Convert a report's money columns into a display currency (Requirement 9).

Presentation only. Nothing here writes a converted amount anywhere — the rows it
returns are the ones about to be redacted and rendered, and the original-currency
values stay untouched in ``core.*``.

Two properties shape the whole module:

**A report read never fetches a rate.** Rates are gathered during the refresh
cascade, where the exclusive per-profile writer lock is already held; a read
opens the database read-only, so fetching here would take that lock behind a
read-only-looking command and fail whenever a sync held it. Callers therefore
pass a ``CurrencyService`` built with no adapter, and a cache miss is simply a
miss.

**A missing rate degrades, it never raises.** ``resolve_rate`` raises for an
uncovered pair because a caller asking for one rate must not receive a
fabricated one. A *report* asking for many has a third option the single-rate
caller does not: fall back to M1K.1 per-currency segmentation and say so
(Requirement 15). One exotic holding must not break a net-worth read.
"""

from __future__ import annotations

import calendar
import logging
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import ReportSemantics
from moneybin.services._validators import validate_currency_code
from moneybin.services.currency_service import (
    CurrencyService,
    RateUnavailableError,
    ResolvedRate,
    apply_rate,
    canonical_currency,
)

logger = logging.getLogger(__name__)

_MONTH_GRAIN = re.compile(r"\d{4}-\d{2}")

#: Why a report went unpriced when it states no reason of its own.
_NO_DECLARED_BASIS = (
    "this report does not declare which column names each row's currency and "
    "which dates it, so its amounts cannot be priced"
)

#: The classes whose values are money and therefore convert. ``AGGREGATE`` is
#: deliberately absent: it covers counts, ratios, z-scores, and confidences,
#: and converting one would corrupt it. A money column declared ``AGGREGATE``
#: is a declaration bug in that report, not a case to handle here.
MONEY_CLASSES = frozenset({
    DataClass.BALANCE,
    DataClass.TXN_AMOUNT,
    DataClass.INCOME_AMOUNT,
})


@dataclass(frozen=True, slots=True)
class ConversionOutcome:
    """The rows a report should render, and what happened to them.

    ``display_currency`` is the requested currency when every row converted and
    ``None`` when any did not — there is no partial state, because a result
    holding some converted and some original amounts under one label is the
    blended number multi-currency.md exists to prevent.
    """

    records: list[dict[str, Any]]
    display_currency: str | None
    degraded_reason: str | None = None
    applied_rates: tuple[ResolvedRate, ...] = ()
    """Every distinct rate that priced a row, deduplicated by (currency, date).

    Requirement 10 wants "the exact rate behind any converted number", and
    `convert_records` is the only place that still knows it: the rate is applied
    into the amount and `currency_column` is relabelled to the target, so once
    this returns, nothing downstream can recover what priced what.

    Empty whenever no conversion happened — a segmented result priced nothing,
    and an empty tuple says that without a caller having to cross-check
    ``display_currency``.
    """


def money_columns(classes: Mapping[str, DataClass]) -> tuple[str, ...]:
    """The declared columns holding money, in declaration order."""
    return tuple(
        name for name, data_class in classes.items() if data_class in MONEY_CLASSES
    )


def convert_records(
    records: Sequence[Mapping[str, Any]],
    *,
    classes: Mapping[str, DataClass],
    semantics: ReportSemantics,
    to_currency: str,
    service: CurrencyService,
) -> ConversionOutcome:
    """Price every row's money columns in ``to_currency``, or segment and say why.

    Converts nothing unless the report declares both the column naming each row's
    currency (``semantics.currency``) and the column dating it
    (``semantics.fx_date``); without either, no defensible rate exists to apply.
    """
    target = canonical_currency(to_currency)
    rows = [dict(record) for record in records]

    amounts = money_columns(classes)
    if not _holds_money(rows, amounts):
        # Nothing to convert is not a degraded conversion. A report of counts and
        # categories has no amounts to denominate, so it reports no currency and
        # no complaint.
        #
        # Declaring a money column is not holding money in one, so the test is
        # the values rather than the schema: `core:networth` on an empty profile
        # returns one placeholder row with every amount NULL, and its NULL
        # `currency_code` would otherwise be read below as a row that lost its
        # currency. A report returning no rows at all reaches the same state
        # from the other side — the row loop is what resolves a rate, so it
        # would otherwise claim the target currency without reading one.
        return ConversionOutcome(rows, None)

    currency_column = semantics.currency
    date_column = semantics.fx_date
    if currency_column is None or date_column is None:
        # A report that cannot be priced states why in its own `fx_basis`, and
        # the obstacle differs per report: `core:cashflow` has one date at its
        # grain and still cannot convert, because `currency_code` sits in its
        # GROUP BY; `core:merchants` spans a range of dates instead of one.
        # Deriving a reason from whichever declaration is missing would give one
        # report's obstacle for another's. A report that declares no basis
        # either — every user-created one, per `dynamic.py` — gets the generic
        # reading, which is accurate for it.
        return ConversionOutcome(rows, None, semantics.fx_basis or _NO_DECLARED_BASIS)

    # Resolved once per (currency, date) rather than per row: a 12-month rollup
    # in three currencies asks 36 questions however many rows it holds.
    resolved: dict[tuple[str, date], ResolvedRate] = {}
    plan: list[tuple[str, date]] = []
    for row in rows:
        source = row.get(currency_column)
        if not isinstance(source, str) or not source.strip():
            # The M1K.1 case that must never be guessed: the amount is in
            # *something*, and pricing it as though it were already the target
            # would invent a number rather than decline to state one.
            return ConversionOutcome(
                rows, None, "some rows do not record which currency they hold"
            )
        on = _rate_date(row.get(date_column))
        if on is None:
            return ConversionOutcome(
                rows,
                None,
                f"some rows carry no usable date in '{date_column}' to price them on",
            )
        base = canonical_currency(source)
        if not _is_currency_code(base):
            # `currency_code` holds whatever the source file put there, and
            # `resolve_rate` refuses a code that would match nothing by raising a
            # plain UserError — not RateUnavailableError. Caught here instead,
            # because one mis-mapped cell must not break a whole read. The value
            # never rides the reason: arbitrary source text is exactly what a
            # malformed code contains.
            return ConversionOutcome(
                rows,
                None,
                "some rows record a currency that is not an ISO-4217 code",
            )
        plan.append((base, on))
        if (base, on) not in resolved:
            try:
                resolved[(base, on)] = service.resolve_rate(base, target, on)
            except RateUnavailableError:
                return ConversionOutcome(
                    rows, None, _missing_reason(service, base, target)
                )

    # Built beside the originals rather than in place: a row that fails to
    # convert half way through would otherwise leave the caller holding a
    # partially converted result under a single currency label.
    converted: list[dict[str, Any]] = []
    for row, key in zip(rows, plan, strict=True):
        rate = resolved[key].rate
        priced = dict(row)
        for column in amounts:
            value = row.get(column)
            if value is None:
                continue
            amount = _as_decimal(value)
            if amount is None:
                # The column name rides the response, never this record: a
                # user-created report names its own columns, so the name is
                # user-authored text (test_log_hygiene.py).
                logger.warning(
                    "A report column declared as money holds a non-numeric "
                    "value; segmenting rather than converting"
                )
                return ConversionOutcome(
                    rows,
                    None,
                    "a column declared as money does not hold a number that can "
                    "be priced in another currency",
                )
            priced[column] = apply_rate(amount, rate)
        priced[currency_column] = target
        converted.append(priced)

    # A row already in the target resolves to an in-memory identity rate that
    # was never stored, and `display_currency` defaults to the home currency —
    # so the ordinary single-currency read arrives here with every row in that
    # state. Publishing those would have the terminal announce a conversion on
    # a report where nothing was priced, and have the CLI count an entry that
    # is not a stored rate. Sorted, rather than left in resolution order, so
    # the provenance a caller reads does not depend on which row came first.
    applied = tuple(
        sorted(
            (
                rate
                for rate in resolved.values()
                if rate.from_currency != rate.to_currency
            ),
            key=lambda r: (r.from_currency, r.requested_date, r.rate_date),
        )
    )
    return ConversionOutcome(converted, target, None, applied)


def _holds_money(rows: Sequence[Mapping[str, Any]], amounts: Sequence[str]) -> bool:
    """Whether any declared money column holds a value in any row."""
    return any(row.get(column) is not None for row in rows for column in amounts)


def _is_currency_code(candidate: str) -> bool:
    """Whether ``candidate`` can name a currency at all.

    Shares the shape gate ``require_currency`` applies rather than re-deriving
    one, so a code the service would refuse is refused here identically.
    """
    try:
        validate_currency_code(candidate)
    except ValueError:
        return False
    return True


def _as_decimal(value: Any) -> Decimal | None:
    """``value`` as a Decimal, or ``None`` when it does not hold a number.

    ``str`` is converted deliberately: DuckDB hands back ``DECIMAL`` as
    ``decimal.Decimal``, but a service-backed report may have formatted one.
    ``bool`` is excluded even though it is an ``int`` — a true/false in a money
    column is a declaration bug, and pricing it would hide that.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | float | str):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None
    return None


def _rate_date(value: Any) -> date | None:
    """The day a row's amounts are priced on, or ``None`` if it names none.

    A ``YYYY-MM`` grain resolves to that month's close, because a month's flows
    are summarized as of the month end — capped at today, since a rate for a day
    that has not happened does not exist and ``resolve_rate`` refuses to invent
    one. A month still open therefore prices at today, and rolls forward as the
    month does.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if _MONTH_GRAIN.fullmatch(text):
        year, month = int(text[:4]), int(text[5:7])
        try:
            close = date(year, month, calendar.monthrange(year, month)[1])
        except ValueError:
            # The regex matches the shape, not the calendar: '2026-13' and
            # '0000-00' both reach here. Naming no usable date segments the
            # report, which is what every other unpriceable row does — raising
            # would take down the read instead.
            return None
        return min(close, date.today())  # noqa: DTZ011  # a rate date is a calendar day, not an instant
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _missing_reason(service: CurrencyService, base: str, target: str) -> str:
    """Why ``base``→``target`` could not be priced, from what the cache can show.

    The distinction the *error* carries — unsupported currency versus a date the
    provider did not publish — is unavailable here by construction: it is decided
    by reading the provider's currency list, and a report read has no provider.
    What the cache can answer without one is whether this pair has ever been
    priced at all, which separates the two remedies well enough to act on.

    Neither branch names a date. ``degraded_reason`` rides the response envelope
    and the CLI's durable log, and a date someone asked about is a date money
    moved on.
    """
    if service.list_rates(base, target):
        return (
            f"no stored {base}->{target} rate for some of the dates in this "
            "report; run 'moneybin refresh' to gather the missing dates"
        )
    return (
        f"no stored {base}->{target} rates at all; run 'moneybin refresh' to "
        "gather them, and record one with 'moneybin fx set' if refresh reports "
        "the pair unsupported"
    )
