"""Resolve an exchange rate for one pair and one date, or fail loud.

Precedence is override → cached provider row → the Friday a weekend resolves
back to → one live fetch → raise. Nothing here ever substitutes a rate: a
conversion the user cannot audit is worse than a conversion that did not happen,
because the wrong number looks exactly like the right one
(``docs/specs/multi-currency.md`` Requirement 12).

Original amounts stay canonical. This service reads rates and writes only the
provider cache and the user's own corrections; no converted amount is ever
stored back over an original.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal

import polars as pl

from moneybin import error_codes
from moneybin.connectors.feed_errors import FeedError
from moneybin.connectors.rates.protocol import RateAdapter, RateObservation
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.metrics.registry import (
    FX_RATE_FETCH_DURATION_SECONDS,
    FX_RATE_RESOLUTION_TOTAL,
    FX_RATE_ROWS_WRITTEN_TOTAL,
)
from moneybin.repositories.exchange_rate_repo import ExchangeRateOverridesRepo
from moneybin.services._validators import validate_currency_code, validate_note_text
from moneybin.services.audit_service import AuditEvent
from moneybin.tables import EXCHANGE_RATE_OVERRIDES, EXCHANGE_RATES

logger = logging.getLogger(__name__)

#: DECIMAL(18,8) — the exchange-rate precision in `.claude/rules/database.md`,
#: shared by raw.exchange_rates and app.exchange_rate_overrides.
RATE_PRECISION = 18
RATE_SCALE = 8
RATE_QUANTUM = Decimal(1).scaleb(-RATE_SCALE)
MAX_STORED_RATE = Decimal(10) ** (RATE_PRECISION - RATE_SCALE) - RATE_QUANTUM

#: Converted amounts are money, so they land on DECIMAL(18,2) like every other
#: amount in the ledger.
MONEY_QUANTUM = Decimal("0.01")

#: The sentinel `ResolvedRate.source` carries when the user's own correction
#: answered. Deliberately NOT a value of `raw.exchange_rates.source_type`, whose
#: comment reserves that column for providers — an override lives in a different
#: table and no provider row ever spells this.
OVERRIDE_SOURCE = "override"

_RAW_RATE_SCHEMA = {
    "from_currency": pl.Utf8,
    "to_currency": pl.Utf8,
    "rate_date": pl.Date,
    "rate": pl.Decimal(RATE_PRECISION, RATE_SCALE),
    "source_type": pl.Utf8,
}


@dataclass(frozen=True, slots=True)
class ResolvedRate:
    """One rate, and the provenance a caller needs to show it.

    ``requested_date`` and ``rate_date`` are separate because they genuinely
    differ: a Sunday conversion is priced with Friday's published rate, and
    Requirement 10 requires showing the exact rate behind any converted figure —
    which means naming the day it was published, not the day asked about.

    ``source`` is not called ``source_type`` on purpose. It holds a provider's
    ``source_type`` *or* the ``"override"`` sentinel, and that sentinel never
    appears in ``raw.exchange_rates.source_type``. Sharing the name would claim
    the two are joinable, which is exactly the layer-specific-alias failure
    `database.md`'s one-concept-one-name rule guards against — read in the
    other direction.
    """

    from_currency: str
    to_currency: str
    requested_date: date
    rate_date: date
    rate: Decimal
    source: str


class RateUnavailableError(UserError):
    """No override, no cached row, and no fetchable rate for a pair and date.

    Carries ``FX_CURRENCY_UNSUPPORTED`` when the provider prices one of the
    currencies on no date at all, and ``FX_RATE_UNAVAILABLE`` when the pair is
    priced in general but not here. The remedies differ, so the codes do.
    """


def apply_rate(amount: Decimal, rate: Decimal) -> Decimal:
    """Convert one amount at one already-resolved rate.

    ``ROUND_HALF_UP`` is stated rather than inherited — Decimal's context
    default is ``ROUND_HALF_EVEN``, which would send half of all exact ties the
    other way and drift every converted total against the accounting convention
    the rest of the ledger uses.

    Separate from :meth:`CurrencyService.convert` so a caller converting many
    rows resolves the rate once, and so the arithmetic is testable over the
    whole amount x rate space without a database round trip per example
    (``tests/property/test_currency_conversion.py``).
    """
    return (amount * rate).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


class CurrencyService:
    """Reads rates; writes the provider cache and the user's corrections."""

    def __init__(
        self,
        db: Database,
        *,
        adapter: RateAdapter | None = None,
        actor: str = "system",
    ) -> None:
        """Initialize with an injected adapter; ``None`` means offline-only."""
        self._db = db
        self._adapter = adapter
        self._actor = actor

    # ------------------------------ resolution ------------------------------

    def resolve_rate(
        self, from_currency: str, to_currency: str, on: date
    ) -> ResolvedRate:
        """The rate to price ``from_currency`` into ``to_currency`` on ``on``.

        Raises ``RateUnavailableError`` rather than returning a substitute.
        """
        base = self._require_currency(from_currency)
        quote = self._require_currency(to_currency)

        if base == quote:
            # No layer is consulted, so no outcome is counted: an identity pair
            # is the common case once a display currency is set, and counting it
            # would swamp the override/cached/fetched ratio this counter exists
            # to expose.
            return ResolvedRate(base, quote, on, on, Decimal(1), base)

        stored = self._stored_rate(base, quote, on)
        if stored is not None:
            rate, source = stored
            FX_RATE_RESOLUTION_TOTAL.labels(outcome=_outcome_for(source)).inc()
            return ResolvedRate(base, quote, on, on, rate, source)

        published = _last_publication_day(on)
        if published != on:
            stored = self._stored_rate(base, quote, published)
            if stored is not None:
                rate, source = stored
                FX_RATE_RESOLUTION_TOTAL.labels(outcome=_outcome_for(source)).inc()
                return ResolvedRate(base, quote, on, published, rate, source)

        observation = self._fetch(base, quote, on)
        self._store(observation)
        FX_RATE_RESOLUTION_TOTAL.labels(outcome="fetched").inc()
        return ResolvedRate(
            base,
            quote,
            on,
            observation.rate_date,
            observation.rate,
            observation.source_type,
        )

    def convert(
        self, amount: Decimal, from_currency: str, to_currency: str, on: date
    ) -> tuple[Decimal, ResolvedRate]:
        """Convert one amount, returning it with the rate that produced it.

        The ``ResolvedRate`` is not optional context: Requirement 10 requires
        showing the exact rate behind any converted figure, and a caller that
        received only the number could not comply.
        """
        resolved = self.resolve_rate(from_currency, to_currency, on)
        return apply_rate(amount, resolved.rate), resolved

    def list_rates(
        self, from_currency: str, to_currency: str, *, since: date | None = None
    ) -> list[ResolvedRate]:
        """The resolved series for one pair, newest first.

        One row per date — the rate that actually applied, not every candidate
        that competed for it. The winner is picked by the same ordering
        ``_stored_rate`` uses, so a date listed here answers exactly what a
        conversion on that date would.
        """
        base = self._require_currency(from_currency)
        quote = self._require_currency(to_currency)
        params: list[object] = [OVERRIDE_SOURCE, base, quote, base, quote]
        bound = ""
        if since is not None:
            bound = "WHERE rate_date >= ?"
            params.append(since)
        rows = self._db.execute(
            f"""
            WITH candidates AS (
                SELECT rate_date, rate, ? AS source, 0 AS priority,
                       updated_at AS observed_at
                  FROM {EXCHANGE_RATE_OVERRIDES.full_name}
                 WHERE from_currency = ? AND to_currency = ?
                UNION ALL
                SELECT rate_date, rate, source_type, 1, loaded_at
                  FROM {EXCHANGE_RATES.full_name}
                 WHERE from_currency = ? AND to_currency = ?
            )
            SELECT rate_date, rate, source FROM candidates
            {bound}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY rate_date ORDER BY priority, observed_at DESC, source
            ) = 1
            ORDER BY rate_date DESC
            """,  # noqa: S608  # TableRef + parameterized values
            params,
        ).fetchall()
        return [
            ResolvedRate(base, quote, row[0], row[0], row[1], str(row[2]))
            for row in rows
        ]

    # ------------------------------- overrides -------------------------------

    def set_override(
        self,
        from_currency: str,
        to_currency: str,
        on: date,
        rate: Decimal,
        *,
        note: str | None,
    ) -> AuditEvent:
        """Record the user's own rate for one pair and date, outranking the cache.

        Validates here rather than leaving it to the table, matching
        ``PriceService.set_mark``: the CHECK constraint and the DECIMAL scale
        both express themselves as untyped DuckDB errors a surface can only
        render as a traceback.
        """
        base = self._require_currency(from_currency)
        quote = self._require_currency(to_currency)
        _require_storable(rate)
        if note is not None:
            # DuckDB VARCHAR is unbounded, so the bound has to be the
            # application's — and every later correction copies the note into its
            # audit before/after image, so one oversized string is stored
            # repeatedly rather than once.
            validate_note_text(note)
        return ExchangeRateOverridesRepo(self._db).set(
            base, quote, on, rate=rate, note=note, actor=self._actor
        )

    def delete_override(self, from_currency: str, to_currency: str, on: date) -> bool:
        """Remove one override, returning ``True`` if a row was actually deleted.

        Reporting the no-op matters: a silent success reads as "the override is
        gone" when there was never one — the same observable state for the wrong
        reason.
        """
        event = ExchangeRateOverridesRepo(self._db).delete(
            self._require_currency(from_currency),
            self._require_currency(to_currency),
            on,
            actor=self._actor,
        )
        return event is not None

    def _require_currency(self, value: str) -> str:
        """Canonicalize and refuse a code that would match nothing.

        A malformed code is not a rejected input — it is a write that succeeds,
        reports success, and joins no conversion, forever and without a symptom.
        Both writers and both readers share this one canonicalization, so an
        override stored under one spelling cannot become unreachable under
        another.
        """
        candidate = _canonical(value)
        try:
            validate_currency_code(candidate)
        except ValueError as exc:
            raise UserError(
                f"{value!r} is not an ISO-4217 currency code.",
                code=error_codes.FX_CURRENCY_INVALID,
            ) from exc
        return candidate

    # -------------------------------- storage --------------------------------

    def _stored_rate(
        self, base: str, quote: str, day: date
    ) -> tuple[Decimal, str] | None:
        """The best rate already on disk for one exact day, or ``None``.

        Override first: a correction outranks every cached provider rate for its
        own pair and date, and this is the single place that ordering is
        expressed — both the exact-date lookup and the weekend fallback go
        through here, so the two cannot drift apart.
        """
        override = self._db.execute(
            f"SELECT rate FROM {EXCHANGE_RATE_OVERRIDES.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE from_currency = ? AND to_currency = ? AND rate_date = ?",
            [base, quote, day],
        ).fetchone()
        if override is not None:
            return override[0], OVERRIDE_SOURCE

        # Newest write wins when two providers priced the same day; source_type
        # breaks the tie so the choice is deterministic rather than whatever the
        # scan happened to reach first.
        cached = self._db.execute(
            f"SELECT rate, source_type FROM {EXCHANGE_RATES.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE from_currency = ? AND to_currency = ? AND rate_date = ? "
            "ORDER BY loaded_at DESC, source_type LIMIT 1",
            [base, quote, day],
        ).fetchone()
        if cached is None:
            return None
        return cached[0], str(cached[1])

    def _store(self, observation: RateObservation) -> int:
        """Append one observation to the cache, returning rows actually written.

        ``on_conflict="ignore"`` keeps the row already stored for a key: the
        table is append-only because a rate a provider published for a date is a
        historical fact. The count is rows the insert WROTE, never rows offered —
        a weekend request re-offers Friday's row every time, so counting the
        frame would make this climb steadily through a completely stalled feed.
        """
        frame = pl.DataFrame(
            [
                {
                    "from_currency": observation.from_currency,
                    "to_currency": observation.to_currency,
                    "rate_date": observation.rate_date,
                    "rate": observation.rate,
                    "source_type": observation.source_type,
                }
            ],
            schema=_RAW_RATE_SCHEMA,
        )
        written = self._db.ingest_dataframe(
            EXCHANGE_RATES.full_name, frame, on_conflict="ignore"
        )
        FX_RATE_ROWS_WRITTEN_TOTAL.labels(source_type=observation.source_type).inc(
            written
        )
        return written

    # --------------------------------- fetch ---------------------------------

    def _fetch(self, base: str, quote: str, on: date) -> RateObservation:
        """One live call, or a ``RateUnavailableError`` naming which absence it is."""
        if self._adapter is None:
            FX_RATE_RESOLUTION_TOTAL.labels(outcome="unavailable").inc()
            raise RateUnavailableError(
                f"No stored {base}/{quote} rate for {on.isoformat()}, and no rate "
                "provider is configured.",
                code=error_codes.FX_RATE_UNAVAILABLE,
                hint="Record the rate yourself with 'moneybin fx override'.",
            )

        started = time.monotonic()
        try:
            observation = self._adapter.fetch(base, quote, on)
        except FeedError as exc:
            FX_RATE_RESOLUTION_TOTAL.labels(outcome="unavailable").inc()
            raise RateUnavailableError(
                f"No stored {base}/{quote} rate for {on.isoformat()}, and the rate "
                "provider could not be reached.",
                code=error_codes.FX_RATE_UNAVAILABLE,
                hint="Retry when the network is back, or record the rate yourself "
                "with 'moneybin fx override'.",
            ) from exc
        finally:
            FX_RATE_FETCH_DURATION_SECONDS.labels(
                source_type=self._adapter.source_type
            ).observe(time.monotonic() - started)

        if observation is None:
            raise self._absence(base, quote, on)
        return observation

    def _absence(self, base: str, quote: str, on: date) -> RateUnavailableError:
        """Tell a currency the provider never prices from a date it happens to lack.

        ``fetch`` answers ``None`` for both, and the fixes are different: an
        unsupported currency needs a manual override and always will, while a
        supported pair missing one date needs a different date. Reporting only
        "no rate available" sends the user to the wrong one.
        """
        FX_RATE_RESOLUTION_TOTAL.labels(outcome="unavailable").inc()
        unsupported = self._unsupported(base, quote)
        if unsupported:
            names = ", ".join(sorted(unsupported))
            return RateUnavailableError(
                f"The rate provider publishes no series for {names}.",
                code=error_codes.FX_CURRENCY_UNSUPPORTED,
                hint="Record the rate yourself with 'moneybin fx override' — this "
                "pair will not become available by retrying.",
            )
        return RateUnavailableError(
            f"The rate provider published no {base}/{quote} rate for {on.isoformat()}.",
            code=error_codes.FX_RATE_UNAVAILABLE,
            hint="Try a nearby date, or record the rate yourself with "
            "'moneybin fx override'.",
        )

    def _unsupported(self, base: str, quote: str) -> set[str]:
        """Which of the two currencies the provider prices on no date at all.

        An unreadable list answers "none": claiming a currency is unsupported on
        the strength of a dropped connection would send the user to a permanent
        remedy for a transient failure.
        """
        if self._adapter is None:
            return set()
        try:
            published = self._adapter.supported_currencies()
        except FeedError:
            logger.info("Could not read the provider's currency list")
            return set()
        return {code for code in (base, quote) if code not in published}


def _canonical(value: str) -> str:
    """Trim and upper a currency code so one spelling reaches every lookup."""
    return value.strip().upper()


def _require_storable(rate: Decimal) -> None:
    """Refuse a rate ``DECIMAL(18,8)`` would silently alter, or the CHECK would reject.

    Order matters. The finite check runs first because comparing a Decimal NaN
    with ``<=`` raises ``InvalidOperation``, so a positivity test placed above it
    could never see one. Magnitude runs before precision because quantizing a
    value that large overflows the decimal context before the scale check could
    answer — and it reads the absolute value, since it runs before the sign rule
    and must not hand ``quantize`` the one input it cannot process.
    """
    if not rate.is_finite():
        raise UserError(
            "An exchange rate must be a finite number.",
            code=error_codes.FX_OVERRIDE_RATE_INVALID,
        )
    if abs(rate) > MAX_STORED_RATE:
        raise UserError(
            f"An exchange rate carries at most {RATE_PRECISION - RATE_SCALE} digits "
            "before the decimal point; this rate is larger than that.",
            code=error_codes.FX_OVERRIDE_RATE_INVALID,
        )
    if rate <= 0:
        raise UserError(
            "An exchange rate must be positive. A zero rate would convert every "
            "balance in that currency to nothing, and this correction outranks "
            "the provider, so nothing downstream would contradict it.",
            code=error_codes.FX_OVERRIDE_RATE_INVALID,
        )
    if rate != rate.quantize(RATE_QUANTUM):
        raise UserError(
            f"An exchange rate is stored to {RATE_SCALE} decimal places; this rate "
            "carries more precision than that, so storing it would record a "
            "different number than the one reported back. Round it first.",
            code=error_codes.FX_OVERRIDE_RATE_INVALID,
        )


def _outcome_for(source: str) -> str:
    """Which layer a stored rate came from, for the resolution counter."""
    return "override" if source == OVERRIDE_SOURCE else "cached"


def _last_publication_day(day: date) -> date:
    """The Friday a weekend resolves back to; the day itself on a weekday.

    Only the weekend is treated as a certainty, and that is the whole point. A
    weekday with no stored rate is ambiguous — the provider may have been closed
    for a holiday, or nobody has fetched that date yet — and a general
    "nearest earlier stored day" fallback cannot tell those apart. It would
    answer an ordinary Tuesday with Monday's rate: a real number, for the wrong
    day, presented as if it were Tuesday's. A weekend carries no such ambiguity,
    because no reference rate is ever published on one.
    """
    if day.weekday() < 5:
        return day
    return day - timedelta(days=day.weekday() - 4)
