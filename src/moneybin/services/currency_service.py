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
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import polars as pl

from moneybin import error_codes
from moneybin.connectors.feed_errors import FeedError, FeedUnreachableError
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

#: The sentinel for a pair that prices itself. No layer answered, so naming the
#: currency here would put a value in `source` that is neither a provider's
#: `source_type` nor the override sentinel — and every surface renders that
#: field as provenance.
IDENTITY_SOURCE = "identity"

#: How far back a provider may resolve a requested date and still be answering
#: about it. A weekend costs two days, ECB's worst case is the four-day Easter
#: closure, and the longest stretch any reference-rate market observes — the
#: Lunar New Year cluster — stays under two weeks. Past that the answer is a
#: stale or misdirected response rather than a publication-day hop.
MAX_BACKWARD_RESOLUTION_DAYS = 14

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
    ``source_type`` *or* the ``"override"`` / ``"identity"`` sentinel, and no
    sentinel ever appears in ``raw.exchange_rates.source_type``. Sharing the
    name would claim the two are joinable, which is exactly the alias failure
    `database.md`'s one-concept-one-name rule guards against — read in the
    other direction.
    """

    from_currency: str
    to_currency: str
    requested_date: date
    rate_date: date
    rate: Decimal
    source: str

    def as_provenance(self) -> dict[str, Any]:
        """This rate as the response envelope publishes it (Requirement 10).

        ``rate`` stays a ``Decimal`` because the envelope keeps money-shaped
        values numeric; the two dates become ISO strings, and both are kept
        because a weekend conversion priced at Friday's rate is only auditable
        when the reader can see the two differ.
        """
        return {
            "from_currency": self.from_currency,
            "to_currency": self.to_currency,
            "requested_date": self.requested_date.isoformat(),
            "rate_date": self.rate_date.isoformat(),
            "rate": self.rate,
            "source": self.source,
        }


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
        base = require_currency(from_currency)
        quote = require_currency(to_currency)

        if base == quote:
            # No layer is consulted, so no outcome is counted: an identity pair
            # is the common case once a display currency is set, and counting it
            # would swamp the override/cached/fetched ratio this counter exists
            # to expose.
            return ResolvedRate(base, quote, on, on, Decimal(1), IDENTITY_SOURCE)

        stored = self._stored_rate(base, quote, on)
        if stored is not None:
            rate, source = stored
            FX_RATE_RESOLUTION_TOTAL.labels(outcome=_outcome_for(source)).inc()
            return ResolvedRate(base, quote, on, on, rate, source)

        published = last_publication_day(on)
        if published != on:
            stored = self._stored_rate(base, quote, published)
            if stored is not None:
                rate, source = stored
                FX_RATE_RESOLUTION_TOTAL.labels(outcome=_outcome_for(source)).inc()
                return ResolvedRate(base, quote, on, published, rate, source)

        # Known gap: `_store` files the row under the day the provider PUBLISHED,
        # so a weekday holiday — which `last_publication_day` deliberately does
        # not hop — misses both lookups above on every later call. The same
        # question re-fetches, and offline it fails outright even though its
        # answer is on disk under another date. Closing it needs a stored
        # requested-to-published mapping; do NOT close it by widening the lookup
        # to the nearest earlier stored day, which would answer an ordinary
        # Tuesday with Monday's rate as if it were Tuesday's (Requirement 12,
        # and `last_publication_day`'s docstring).
        observation = self._fetch(base, quote, on)
        self._store(observation)

        # The provider can also resolve backwards — a weekday holiday reaches
        # here, because only a weekend is hopped before the fetch. An override
        # outranks the provider on its own date however that date is reached,
        # so the correction has to be consulted again once the provider names
        # the day it actually priced.
        override = self._override_rate(base, quote, observation.rate_date)
        if override is not None:
            FX_RATE_RESOLUTION_TOTAL.labels(outcome="override").inc()
            return ResolvedRate(
                base, quote, on, observation.rate_date, override, OVERRIDE_SOURCE
            )

        FX_RATE_RESOLUTION_TOTAL.labels(outcome="fetched").inc()
        return ResolvedRate(
            base,
            quote,
            on,
            observation.rate_date,
            _as_stored(observation.rate),
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
        base = require_currency(from_currency)
        quote = require_currency(to_currency)
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
        base = require_currency(from_currency)
        quote = require_currency(to_currency)
        if base == quote:
            # `resolve_rate` answers an identity pair from `IDENTITY_SOURCE`
            # without reading this table, so a row stored here would be listed
            # by `list_rates` and honoured by nothing. Refusing the write is the
            # only place that gap closes: the read cannot consult the override
            # instead, because a correction to a pair that is 1 by definition
            # would silently restate every balance already in that currency.
            raise UserError(
                f"{base} prices itself at exactly 1, so there is no "
                "correction to record for it.",
                code=error_codes.FX_OVERRIDE_PAIR_INVALID,
            )
        _require_storable(rate)
        if note is not None:
            # DuckDB VARCHAR is unbounded, so the bound has to be the
            # application's — and every later correction copies the note into its
            # audit before/after image, so one oversized string is stored
            # repeatedly rather than once.
            validate_note_text(note)
        event = ExchangeRateOverridesRepo(self._db).set(
            base, quote, on, rate=rate, note=note, actor=self._actor
        )
        from moneybin.services.fx_accounting_refresh import (  # noqa: PLC0415
            restate_fx_accounting,
        )

        restate_fx_accounting(self._db)
        return event

    def delete_override(self, from_currency: str, to_currency: str, on: date) -> bool:
        """Remove one override, returning ``True`` if a row was actually deleted.

        Reporting the no-op matters: a silent success reads as "the override is
        gone" when there was never one — the same observable state for the wrong
        reason.
        """
        event = ExchangeRateOverridesRepo(self._db).delete(
            require_currency(from_currency),
            require_currency(to_currency),
            on,
            actor=self._actor,
        )
        if event is None:
            return False
        from moneybin.services.fx_accounting_refresh import (  # noqa: PLC0415
            restate_fx_accounting,
        )

        restate_fx_accounting(self._db)
        return True

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
        override = self._override_rate(base, quote, day)
        if override is not None:
            return override, OVERRIDE_SOURCE

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

    def _override_rate(self, base: str, quote: str, day: date) -> Decimal | None:
        """The user's own rate for one exact day, or ``None``.

        Its own method because two paths need it: ``_stored_rate``'s ordered
        lookup, and the post-fetch check for a provider that resolved back to a
        day the user has already corrected.
        """
        row = self._db.execute(
            f"SELECT rate FROM {EXCHANGE_RATE_OVERRIDES.full_name} "  # noqa: S608  # TableRef + parameterized values
            "WHERE from_currency = ? AND to_currency = ? AND rate_date = ?",
            [base, quote, day],
        ).fetchone()
        return None if row is None else row[0]

    def _store(self, observation: RateObservation) -> int:
        """Append one observation to the cache, returning rows actually written."""
        return self.store_observations((observation,))

    def store_observations(self, observations: Sequence[RateObservation]) -> int:
        """Append observations to the cache, returning rows actually written.

        ``on_conflict="ignore"`` keeps the row already stored for a key: the
        table is append-only because a rate a provider published for a date is a
        historical fact. The count is rows the insert WROTE, never rows offered —
        a weekend request re-offers Friday's row every time, and a backfill
        re-offers a whole cached span, so counting the frame would make this
        climb steadily through a completely stalled feed.

        Public because the backfill arrives with a span rather than a single
        rate, and a second write path into an append-only table would be a
        second place for the quantization and the counter to drift.
        """
        if not observations:
            return 0
        frame = pl.DataFrame(
            [
                {
                    "from_currency": observation.from_currency,
                    "to_currency": observation.to_currency,
                    "rate_date": observation.rate_date,
                    "rate": _as_stored(observation.rate),
                    "source_type": observation.source_type,
                }
                for observation in observations
            ],
            schema=_RAW_RATE_SCHEMA,
        )
        written = self._db.ingest_dataframe(
            EXCHANGE_RATES.full_name, frame, on_conflict="ignore"
        )
        # Attributed to the source the rows came from. A mixed batch is not a
        # shape any caller produces — one call fetches one pair from one
        # provider — so the first observation's source names the whole frame.
        FX_RATE_ROWS_WRITTEN_TOTAL.labels(source_type=observations[0].source_type).inc(
            written
        )
        return written

    # --------------------------------- fetch ---------------------------------

    def _fetch(self, base: str, quote: str, on: date) -> RateObservation:
        """One live call, or a ``RateUnavailableError`` naming which absence it is.

        The requested date rides the ``hint``, never the ``message``. Text-mode
        ``handle_cli_errors`` sends ``message`` to ``logger.error`` on the
        strength of it being a fixed MoneyBin string, and the file handler is
        unfiltered — so a date interpolated there persists to
        ``cli_YYYY-MM-DD.log``, which ``FxRatePayload`` classifies ``TXN_DATE``
        precisely because one is asked about a day when money moved on it. The
        ``hint`` goes straight to stderr and rides the JSON envelope, so the user
        still learns which day failed on both surfaces.
        """
        if self._adapter is None:
            FX_RATE_RESOLUTION_TOTAL.labels(outcome="unavailable").inc()
            raise RateUnavailableError(
                f"No stored {base}/{quote} rate for the requested date, and no "
                "rate provider is configured.",
                code=error_codes.FX_RATE_UNAVAILABLE,
                hint=f"Record the {on.isoformat()} rate yourself with "
                "'moneybin fx set'.",
            )

        started = time.monotonic()
        try:
            observation = self._adapter.fetch(base, quote, on)
        except FeedUnreachableError as exc:
            FX_RATE_RESOLUTION_TOTAL.labels(outcome="unavailable").inc()
            raise RateUnavailableError(
                f"No stored {base}/{quote} rate for the requested date, and the "
                "rate provider could not be reached.",
                code=error_codes.FX_RATE_UNAVAILABLE,
                hint=f"Retry when the network is back, or record the "
                f"{on.isoformat()} rate yourself with 'moneybin fx set'.",
            ) from exc
        except FeedError as exc:
            # Split from the branch above because the remedies differ. The
            # provider answered — with a 5xx, a quota refusal, or a body the
            # adapter could not read — so telling the user to wait for the
            # network would have them retrying a connection that is already
            # working, for as long as the provider stays broken.
            FX_RATE_RESOLUTION_TOTAL.labels(outcome="unavailable").inc()
            raise RateUnavailableError(
                f"No stored {base}/{quote} rate for the requested date, and the "
                "rate provider did not return one.",
                code=error_codes.FX_RATE_UNAVAILABLE,
                hint=f"The provider is answering but not with a usable rate. "
                f"Try again later, or record the {on.isoformat()} rate yourself "
                "with 'moneybin fx set'.",
            ) from exc
        finally:
            FX_RATE_FETCH_DURATION_SECONDS.labels(
                source_type=self._adapter.source_type
            ).observe(time.monotonic() - started)

        if observation is None:
            raise self._absence(base, quote, on)
        if not is_storable_after_rounding(observation.rate):
            # `resolve_rate` stores this observation outside the `FeedError`
            # handling above, so an unstorable rate reaches DuckDB: one that
            # quantizes to zero trips `CHECK (rate > 0)`, an oversized one
            # overflows `_as_stored`, and a NaN panics the frame builder in
            # native code. None of those is something `classify_user_error`
            # recognises, so a corrupt response would surface as a traceback
            # rather than the typed failure every other unusable answer gets.
            # `PriceService.pull` guards the same class for the same reason.
            FX_RATE_RESOLUTION_TOTAL.labels(outcome="unavailable").inc()
            raise RateUnavailableError(
                f"No stored {base}/{quote} rate for the requested date, and the "
                "rate provider did not return a usable one.",
                code=error_codes.FX_RATE_UNAVAILABLE,
                hint="The provider answered with a rate MoneyBin cannot store. "
                f"Record the {on.isoformat()} rate yourself with "
                "'moneybin fx set'.",
            )
        if observation.rate_date > on:
            # `RateObservation` documents the hop as resolving *back* to the last
            # published day, so a later one is not a rate that exists yet. The
            # immediate error is pricing `on` with another day's number; the
            # lasting one is that `_store` files rows under `rate_date`, so the
            # row lands on a future date and `_stored_rate` then answers that day
            # from cache with a rate the provider never published for it — ahead
            # of the real publication, on a table only an override can displace.
            #
            # Here rather than in the adapter because the bound is the
            # Protocol's, not one provider's response shape: `on` and the write
            # that gets corrupted both live at this layer, so one guard covers
            # every adapter instead of each having to remember its own.
            FX_RATE_RESOLUTION_TOTAL.labels(outcome="unavailable").inc()
            raise RateUnavailableError(
                f"No stored {base}/{quote} rate for the requested date, and the "
                "rate provider answered for a later day.",
                code=error_codes.FX_RATE_UNAVAILABLE,
                hint="The provider priced a day after the one asked about. "
                f"Record the {on.isoformat()} rate yourself with "
                "'moneybin fx set'.",
            )
        if (on - observation.rate_date).days > MAX_BACKWARD_RESOLUTION_DAYS:
            # The check above bounds the other direction. Backwards is the
            # legitimate one — a closed market resolves to its last publication
            # — but only across a closure, never across years. Left open, a
            # stale or misdirected answer prices `on` from another era, and
            # `_store` files it under its own old date, where it then answers
            # that date from cache on every later call.
            FX_RATE_RESOLUTION_TOTAL.labels(outcome="unavailable").inc()
            raise RateUnavailableError(
                f"No stored {base}/{quote} rate for the requested date, and the "
                "rate provider answered for a much earlier day.",
                code=error_codes.FX_RATE_UNAVAILABLE,
                hint="The provider answered for a day too far before the one "
                f"asked about to be its publication day. Record the "
                f"{on.isoformat()} rate yourself with 'moneybin fx set'.",
            )
        return observation

    def _absence(self, base: str, quote: str, on: date) -> RateUnavailableError:
        """Tell a currency the provider never prices from a date it happens to lack.

        ``fetch`` answers ``None`` for both, and the fixes are different: an
        unsupported currency needs a manual override and always will, while a
        supported pair missing one date needs a different date. Reporting only
        "no rate available" sends the user to the wrong one.

        The date stays out of the message here for the same reason as in
        ``_fetch``. The unsupported-currency branch carries no date at all —
        that failure is about the currency, and naming a day would suggest
        another one might work.
        """
        FX_RATE_RESOLUTION_TOTAL.labels(outcome="unavailable").inc()
        unsupported = self._unsupported(base, quote)
        if unsupported:
            names = ", ".join(sorted(unsupported))
            return RateUnavailableError(
                f"The rate provider publishes no series for {names}.",
                code=error_codes.FX_CURRENCY_UNSUPPORTED,
                hint="Record the rate yourself with 'moneybin fx set' — this "
                "pair will not become available by retrying.",
            )
        return RateUnavailableError(
            f"The rate provider published no {base}/{quote} rate for the "
            "requested date.",
            code=error_codes.FX_RATE_UNAVAILABLE,
            hint=f"Nothing was published for {on.isoformat()}. Try a nearby "
            "date, or record the rate yourself with 'moneybin fx set'.",
        )

    def _unsupported(self, base: str, quote: str) -> set[str]:
        """Which of the two currencies the provider prices on no date at all.

        An unreadable list collapses to "none" here, unlike in the backfill: the
        caller's other branch already tells the user to try a nearby date or
        record the rate, which is the right advice for a lookup whose support is
        unknown. The backfill's fall-through is not, so it reads the ``None``.
        """
        return unsupported_currencies(self._adapter, base, quote) or set()


def build_currency_service(db: Database, *, actor: str = "system") -> CurrencyService:
    """Wire a CurrencyService with the real rate adapter.

    Kept out of ``CurrencyService.__init__`` so tests inject fakes without a
    production seam, matching ``build_price_service``. No credential is read:
    Frankfurter is keyless, so unlike the price feeds there is nothing here that
    can fail because a token is missing.
    """
    from moneybin.connectors.rates.frankfurter import (  # noqa: PLC0415  # httpx is not cold-start cheap
        FrankfurterRateAdapter,
    )

    return CurrencyService(db, adapter=FrankfurterRateAdapter(), actor=actor)


def build_cache_only_currency_service(db: Database) -> CurrencyService:
    """Wire a CurrencyService that resolves from stored rates and never fetches.

    For read paths — report execution above all. Rates are gathered during the
    refresh cascade, which already holds the exclusive per-profile writer lock; a
    read opens the database read-only, so fetching here would take that lock
    behind a read-only-looking command and fail whenever a sync held it.

    Passing no adapter is what enforces that, and this factory exists so the
    invariant has a name a reader can grep for rather than living in an omitted
    keyword argument at each call site.
    """
    return CurrencyService(db)


def canonical_currency(value: str) -> str:
    """Trim and upper a currency code so one spelling reaches every lookup.

    Public because surfaces need it too: a command that echoes back the pair it
    just wrote must spell it the way storage did, and a second ``.upper()`` at
    the call site is a copy that drifts the moment this one changes.
    """
    return value.strip().upper()


def require_currency(value: str) -> str:
    """Canonicalize and refuse a code that would match nothing.

    A malformed code is not a rejected input — it is a write that succeeds,
    reports success, and joins no conversion, forever and without a symptom.
    Both writers, both readers, and the reports layer's display target share
    this one canonicalization, so an override stored under one spelling cannot
    become unreachable under another.
    """
    candidate = canonical_currency(value)
    try:
        validate_currency_code(candidate)
    except ValueError as exc:
        # The rejected value rides the `hint`, never the `message`, for the
        # reason `_fetch`'s docstring gives: text-mode `handle_cli_errors` sends
        # `message` to `logger.error` on the strength of its being a fixed
        # MoneyBin string, and the file handler is unfiltered. A currency
        # argument is free text, so a mis-paste would otherwise persist verbatim
        # to `cli_YYYY-MM-DD.log`.
        raise UserError(
            "A currency must be given as a three-letter ISO-4217 code.",
            code=error_codes.FX_CURRENCY_INVALID,
            hint=f"{value!r} is not one. Use a code like 'USD' or 'EUR'.",
        ) from exc
    return candidate


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


def unsupported_currencies(adapter: RateAdapter | None, *codes: str) -> set[str] | None:
    """Which of ``codes`` the provider prices on no date at all, or ``None``.

    Every caller asks about a *pair*, and either side can be the one the
    provider has never carried — a profile whose home currency is unpublished
    reaches this with a perfectly ordinary base. Reading one side is what makes
    such a profile report an empty result forever with nothing to act on, so
    the check is shared rather than re-derived per caller.

    ``None`` means the list itself could not be read, and is deliberately not
    the empty set: claiming a currency is unsupported on the strength of a
    dropped connection would send the user to a permanent remedy for a
    transient failure, but so would answering "none unsupported" to a caller
    that reads an empty answer as proof the pair is fine. Only the caller knows
    which of those two mistakes its branch would make, so the distinction is
    handed back rather than resolved here. Adapters memoize the list, so asking
    about many pairs still costs one call.
    """
    if adapter is None:
        return set()
    try:
        published = adapter.supported_currencies()
    except FeedError:
        logger.info("Could not read the provider's currency list")
        return None
    return {code for code in codes if code not in published}


def is_storable_after_rounding(rate: Decimal) -> bool:
    """Whether ``DECIMAL(18,8)`` can hold this rate at all, once quantized.

    Deliberately not ``_require_storable``, which the override path uses. That
    one also refuses excess precision, because a user who typed nine places has
    to be told the ninth will not be kept. A provider is *allowed* more places
    than the column holds — quantizing them is what ``_as_stored`` exists for —
    so the only question here is whether anything survives the rounding.

    The order is ``_require_storable``'s, for its reasons: a ``NaN`` raises
    ``InvalidOperation`` when compared, so it is excluded first, and a value
    past the magnitude bound overflows the decimal context when quantized, so
    magnitude runs before the rounded value is computed.
    """
    if not rate.is_finite():
        return False
    if abs(rate) > MAX_STORED_RATE:
        return False
    return _as_stored(rate) > 0


def _as_stored(rate: Decimal) -> Decimal:
    """The rate as ``DECIMAL(18,8)`` keeps it.

    A provider free to publish more places than the column holds would otherwise
    have its extra digits dropped on the way in and kept on the way out — so the
    fetch that seeded the cache would answer with a number every later cached
    read contradicts, for the same pair and the same day.
    """
    return rate.quantize(RATE_QUANTUM, rounding=ROUND_HALF_UP)


def _outcome_for(source: str) -> str:
    """Which layer a stored rate came from, for the resolution counter."""
    return "override" if source == OVERRIDE_SOURCE else "cached"


def last_publication_day(day: date) -> date:
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
