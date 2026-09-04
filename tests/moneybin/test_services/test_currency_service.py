"""Rate resolution: precedence, weekend fallback, and failing loud.

Requirement 12 is the spine of this file: a rate that cannot be resolved raises.
Substituting today's rate, or 1.0, converts money at a number no provider ever
published — and does it silently, which is the failure mode every test here
exists to make impossible.

The precedence tests are ordered the way resolution is: an override outranks the
provider cache, an exact cached row outranks the network, and a *weekend*
resolves back to the preceding Friday from cache alone. A weekday gap
deliberately does not: see
``test_a_weekday_gap_is_not_answered_from_an_earlier_cached_day``.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from decimal import Decimal

import pytest
from prometheus_client import REGISTRY

from moneybin import error_codes
from moneybin.connectors.rates.errors import (
    RateFeedAPIError,
    RateFeedUnreachableError,
)
from moneybin.connectors.rates.protocol import RateAdapter, RateObservation
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.services import currency_service
from moneybin.services.currency_service import CurrencyService, RateUnavailableError

_FRI = date(2026, 3, 13)
_SAT = date(2026, 3, 14)
_SUN = date(2026, 3, 15)
_MON = date(2026, 3, 16)
_TUE = date(2026, 3, 17)


@pytest.mark.parametrize("day", [_FRI, _MON, _TUE])
def test_last_publication_day_keeps_weekdays(day: date) -> None:
    assert currency_service.last_publication_day(day) == day


@pytest.mark.parametrize("day", [_SAT, _SUN])
def test_last_publication_day_returns_friday_for_weekends(day: date) -> None:
    assert currency_service.last_publication_day(day) == _FRI


class _OfflineAdapter:
    """Every call fails at the network, never with a None the caller can route."""

    source_type = "frankfurter"

    def __init__(self) -> None:
        self.calls = 0

    def fetch(
        self, from_currency: str, to_currency: str, on: date
    ) -> RateObservation | None:
        self.calls += 1
        raise RateFeedUnreachableError("rate feed unreachable: ConnectError")

    def fetch_range(
        self, from_currency: str, to_currency: str, start: date, end: date
    ) -> tuple[RateObservation, ...]:
        self.calls += 1
        raise RateFeedUnreachableError("rate feed unreachable: ConnectError")

    def supported_currencies(self) -> frozenset[str]:
        raise RateFeedUnreachableError("rate feed unreachable: ConnectError")


class _BrokenAdapter:
    """The network is fine; the provider answers with something unusable."""

    source_type = "frankfurter"

    def fetch(
        self, from_currency: str, to_currency: str, on: date
    ) -> RateObservation | None:
        raise RateFeedAPIError("Exchange rate feed returned a non-object body")

    def fetch_range(
        self, from_currency: str, to_currency: str, start: date, end: date
    ) -> tuple[RateObservation, ...]:
        raise RateFeedAPIError("Exchange rate feed returned a non-object body")

    def supported_currencies(self) -> frozenset[str]:
        return frozenset({"USD", "EUR"})


class _StubAdapter:
    """Answers one canned observation, and publishes a fixed currency list."""

    source_type = "frankfurter"

    def __init__(
        self,
        obs: RateObservation | None,
        *,
        supported: frozenset[str] = frozenset({"USD", "EUR", "GBP", "JPY"}),
    ) -> None:
        self._obs = obs
        self._supported = supported
        self.calls = 0

    def fetch(
        self, from_currency: str, to_currency: str, on: date
    ) -> RateObservation | None:
        self.calls += 1
        return self._obs

    def fetch_range(
        self, from_currency: str, to_currency: str, start: date, end: date
    ) -> tuple[RateObservation, ...]:
        self.calls += 1
        return () if self._obs is None else (self._obs,)

    def supported_currencies(self) -> frozenset[str]:
        return self._supported


def _cache(
    db: Database,
    *,
    from_currency: str = "USD",
    to_currency: str = "EUR",
    rate_date: date = _MON,
    rate: str = "0.92000000",
    source_type: str = "frankfurter",
) -> None:
    """Write one provider row straight to the cache, bypassing the service."""
    db.execute(
        """
        INSERT INTO raw.exchange_rates
            (from_currency, to_currency, rate_date, rate, source_type)
        VALUES (?, ?, ?, ?, ?)
        """,
        [from_currency, to_currency, rate_date, Decimal(rate), source_type],
    )


def _outcome(outcome: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "moneybin_fx_rate_resolution_total", {"outcome": outcome}
        )
        or 0.0
    )


def test_an_override_outranks_a_cached_provider_rate(db: Database) -> None:
    adapter = _StubAdapter(None)
    service = CurrencyService(db, adapter=adapter, actor="test")
    _cache(db, rate="0.92000000")
    service.set_override("USD", "EUR", _MON, Decimal("0.95"), note=None)

    resolved = service.resolve_rate("USD", "EUR", _MON)

    assert resolved.rate == Decimal("0.95")
    assert resolved.source == "override"
    assert adapter.calls == 0


def test_a_provider_refresh_never_overwrites_an_override(db: Database) -> None:
    """The override answers every later call, so no refetch can quietly replace it."""
    adapter = _StubAdapter(
        RateObservation("USD", "EUR", _MON, Decimal("0.91"), "frankfurter")
    )
    service = CurrencyService(db, adapter=adapter, actor="test")
    service.set_override("USD", "EUR", _MON, Decimal("0.95"), note=None)

    service.resolve_rate("USD", "EUR", _MON)
    again = service.resolve_rate("USD", "EUR", _MON)

    assert again.rate == Decimal("0.95")
    assert adapter.calls == 0, "an override answers without reaching the provider"


def test_a_cached_rate_is_not_refetched(db: Database) -> None:
    adapter = _StubAdapter(None)
    service = CurrencyService(db, adapter=adapter)
    _cache(db, rate="0.92000000")

    resolved = service.resolve_rate("USD", "EUR", _MON)

    assert resolved.rate == Decimal("0.92000000")
    assert resolved.source == "frankfurter"
    assert adapter.calls == 0


def test_a_fetched_rate_is_stored_so_the_next_call_reads_the_cache(
    db: Database,
) -> None:
    """One network call per (pair, date) — the cache is what makes that true."""
    adapter = _StubAdapter(
        RateObservation("USD", "EUR", _MON, Decimal("0.92"), "frankfurter")
    )
    service = CurrencyService(db, adapter=adapter)

    first = service.resolve_rate("USD", "EUR", _MON)
    second = service.resolve_rate("USD", "EUR", _MON)

    assert first.rate == second.rate == Decimal("0.92000000")
    assert adapter.calls == 1, "the second call must be answered from storage"
    row = db.execute(
        "SELECT rate, source_type FROM raw.exchange_rates WHERE rate_date = ?", [_MON]
    ).fetchone()
    assert row == (Decimal("0.92000000"), "frankfurter")


def test_a_weekend_resolves_to_the_recorded_business_day(db: Database) -> None:
    """2026-03-15 is a Sunday. The Friday rate answers, and the result says so."""
    adapter = _StubAdapter(
        RateObservation("USD", "EUR", _FRI, Decimal("0.92010"), "frankfurter")
    )

    resolved = CurrencyService(db, adapter=adapter).resolve_rate("USD", "EUR", _SUN)

    assert resolved.requested_date == _SUN
    assert resolved.rate_date == _FRI
    assert resolved.rate == Decimal("0.92010000")


def test_a_cached_friday_answers_a_weekend_without_reaching_the_provider(
    db: Database,
) -> None:
    """A weekend is a calendar fact, not a guess — ECB never publishes on one.

    Without this, every weekend-dated conversion reaches the network forever: the
    provider resolves the request back to Friday and returns a row already
    stored, which the append-only insert then drops. The cache would hold the
    answer and never be allowed to give it.
    """
    adapter = _StubAdapter(None)
    service = CurrencyService(db, adapter=adapter)
    _cache(db, rate_date=_FRI, rate="0.92010000")

    for requested in (_SAT, _SUN):
        resolved = service.resolve_rate("USD", "EUR", requested)
        assert resolved.requested_date == requested
        assert resolved.rate_date == _FRI
        assert resolved.rate == Decimal("0.92010000")
    assert adapter.calls == 0


def test_an_override_on_the_friday_still_wins_a_weekend_request(db: Database) -> None:
    """Precedence survives the weekend hop, or the correction silently lapses.

    The user corrects Friday's rate; a Saturday transaction is priced with
    Friday's rate. If the weekend fallback read only the provider cache, their
    correction would hold for Friday and be ignored one calendar day later —
    the same rate, the same pair, the wrong number.
    """
    adapter = _StubAdapter(None)
    service = CurrencyService(db, adapter=adapter, actor="test")
    _cache(db, rate_date=_FRI, rate="0.92010000")
    service.set_override("USD", "EUR", _FRI, Decimal("0.95"), note="bank rate")

    resolved = service.resolve_rate("USD", "EUR", _SAT)

    assert resolved.rate == Decimal("0.95")
    assert resolved.source == "override"
    assert resolved.rate_date == _FRI
    assert adapter.calls == 0


def test_a_weekday_gap_is_not_answered_from_an_earlier_cached_day(
    db: Database,
) -> None:
    """A missing weekday is not proof the provider published nothing that day.

    A general "nearest earlier cached day" lookback cannot tell a holiday from a
    date nobody has fetched yet, so on an ordinary Tuesday it would answer with
    Monday's rate — a real number, for the wrong day, reported as if it were
    Tuesday's. Offline, the honest answer is that we do not know.
    """
    service = CurrencyService(db, adapter=_OfflineAdapter())
    _cache(db, rate_date=_MON, rate="0.92000000")

    with pytest.raises(RateUnavailableError):
        service.resolve_rate("USD", "EUR", _TUE)


def test_an_override_wins_the_day_a_holiday_fetch_resolves_back_to(
    db: Database,
) -> None:
    """Precedence survives the *provider's* hop, not only the weekend one.

    Only a weekend is walked back before the fetch, so a weekday the provider
    was closed for reaches the network — and Frankfurter answers it with the
    preceding business day. If that answer were taken at face value, a
    correction the user made for that very day would be ignored the moment it
    was reached from a holiday instead of directly: the same pair, the same
    published day, the wrong number.
    """
    adapter = _StubAdapter(
        RateObservation("USD", "EUR", _MON, Decimal("0.92"), "frankfurter")
    )
    service = CurrencyService(db, adapter=adapter, actor="test")
    service.set_override("USD", "EUR", _MON, Decimal("0.95"), note="bank rate")

    resolved = service.resolve_rate("USD", "EUR", _TUE)

    assert resolved.rate == Decimal("0.95")
    assert resolved.source == "override"
    assert resolved.requested_date == _TUE
    assert resolved.rate_date == _MON


def test_a_provider_date_after_the_requested_day_is_not_a_rate(
    db: Database,
) -> None:
    """Resolution runs backwards only, so a later day is a broken answer.

    ``RateObservation`` documents the hop as "a weekend or holiday resolves back
    to the last published day" — there is no forward case, because a reference
    rate for a day that has not been published yet does not exist. A proxy, a
    cached answer, or a provider bug can still send one.

    Refusing it matters twice over, and the second is the durable half. The
    immediate error is pricing ``on`` with a later day's rate. The lasting one is
    that ``resolve_rate`` stores what it fetched under ``observation.rate_date``,
    so the row lands on a *future* date — and ``_stored_rate`` reads by that
    column, so when the day arrives the cache answers it with a rate the
    provider never published for it, ahead of the real publication. The table is
    append-only, so only an override can displace it afterwards.
    """
    adapter = _StubAdapter(
        RateObservation("USD", "EUR", _TUE, Decimal("0.92"), "frankfurter")
    )
    service = CurrencyService(db, adapter=adapter, actor="test")

    with pytest.raises(RateUnavailableError) as caught:
        service.resolve_rate("USD", "EUR", _MON)

    assert caught.value.code == error_codes.FX_RATE_UNAVAILABLE
    row = db.execute("SELECT COUNT(*) FROM raw.exchange_rates").fetchone()
    assert row is not None and row[0] == 0, "a forward-dated rate is not cached"


def test_a_provider_date_far_before_the_requested_day_is_not_a_rate(
    db: Database,
) -> None:
    """A backward hop follows a publication calendar; it is not an open reach.

    The forward guard above refuses a day that has not been published yet. The
    backward direction needs its own floor, because the legitimate hop is a
    closed market — a weekend, or the longest holiday cluster a reference-rate
    provider observes — and nothing past that resolves to anything. Without a
    floor a misdirected or stale-cached response dated years earlier is
    accepted, quantized, stored, and reported as the answer for ``on``.

    The fixture is decades stale so that only this bound can catch it: the rate
    itself is storable and the date is ahead of nothing, so the rounding check
    and the forward check both pass it through untouched.
    """
    adapter = _StubAdapter(
        RateObservation("USD", "EUR", date(1999, 1, 4), Decimal("0.92"), "frankfurter")
    )
    service = CurrencyService(db, adapter=adapter, actor="test")

    with pytest.raises(RateUnavailableError) as caught:
        service.resolve_rate("USD", "EUR", _MON)

    assert caught.value.code == error_codes.FX_RATE_UNAVAILABLE
    row = db.execute("SELECT COUNT(*) FROM raw.exchange_rates").fetchone()
    assert row is not None and row[0] == 0, "a stale-dated rate is not cached"


def test_a_rejected_currency_code_does_not_reach_the_logged_message(
    db: Database,
) -> None:
    """The rejected value rides the ``hint``, never the ``message``.

    Text-mode ``handle_cli_errors`` sends ``message`` to ``logger.error`` on the
    strength of its being a fixed MoneyBin string, and the file handler is
    unfiltered — so anything interpolated there persists to
    ``cli_YYYY-MM-DD.log``. ``from_currency`` is free text the user types, so a
    mis-paste puts arbitrary content on that path. ``_fetch`` already keeps the
    requested date out of ``message`` for this exact reason; validation was the
    one branch that did not.

    The ``hint`` still names what was rejected — it goes straight to stderr and
    rides the JSON envelope — so a caller who passed two codes can tell which
    one failed.
    """
    service = CurrencyService(db, adapter=None, actor="test")

    with pytest.raises(UserError) as caught:
        service.resolve_rate("PASTED-9876543210", "EUR", _MON)

    assert caught.value.code == error_codes.FX_CURRENCY_INVALID
    assert "PASTED-9876543210" not in caught.value.message
    assert caught.value.hint is not None
    assert "PASTED-9876543210" in caught.value.hint


def test_a_fetched_rate_is_reported_at_the_precision_it_is_stored_at(
    db: Database,
) -> None:
    """The seeding call and every cached call afterwards must agree.

    ``raw.exchange_rates.rate`` is ``DECIMAL(18,8)``, so a provider publishing
    more places than that has them dropped on the way in. Reporting the
    unrounded number back would make the fetch that seeded the cache the one
    answer no later read can reproduce.
    """
    adapter = _StubAdapter(
        RateObservation("USD", "EUR", _MON, Decimal("0.1234567891"), "frankfurter")
    )
    service = CurrencyService(db, adapter=adapter, actor="test")

    fetched = service.resolve_rate("USD", "EUR", _MON)
    cached = service.resolve_rate("USD", "EUR", _MON)

    assert fetched.rate == cached.rate == Decimal("0.12345679")


def test_an_identity_pair_reports_a_source_that_is_not_a_currency(
    db: Database,
) -> None:
    """``source`` is provenance, and every surface renders it as such.

    Naming the currency there would put a value in the field that is neither a
    provider's ``source_type`` nor the override sentinel, so `fx rate USD USD`
    would report its provenance as "USD".
    """
    resolved = CurrencyService(db, adapter=_StubAdapter(None)).resolve_rate(
        "USD", "USD", _MON
    )

    assert resolved.source == "identity"


def test_a_missing_rate_while_offline_raises_rather_than_guessing(
    db: Database,
) -> None:
    """Never today's rate, never 1.0 — the whole point of Requirement 12."""
    service = CurrencyService(db, adapter=_OfflineAdapter())

    with pytest.raises(RateUnavailableError) as caught:
        service.resolve_rate("USD", "EUR", _MON)

    assert caught.value.code == error_codes.FX_RATE_UNAVAILABLE
    assert "1.0" not in str(caught.value)


def test_an_unsupported_currency_is_a_different_failure_from_a_missing_rate(
    db: Database,
) -> None:
    """The two absences have different remedies, so they carry different codes.

    ``fetch`` answers None for both, which is why the adapter publishes
    ``supported_currencies()``. A pair the provider never prices needs a manual
    override; a supported pair missing one date needs a different date. Collapsing
    both into "no rate available" sends the user to the wrong fix.
    """
    service = CurrencyService(db, adapter=_StubAdapter(None))

    with pytest.raises(RateUnavailableError) as caught:
        service.resolve_rate("USD", "XXX", _MON)

    assert caught.value.code == error_codes.FX_CURRENCY_UNSUPPORTED


def test_a_supported_pair_missing_one_date_reports_the_rate_as_unavailable(
    db: Database,
) -> None:
    """The other half of the pair above — same None from fetch, different code."""
    service = CurrencyService(db, adapter=_StubAdapter(None))

    with pytest.raises(RateUnavailableError) as caught:
        service.resolve_rate("USD", "GBP", _MON)

    assert caught.value.code == error_codes.FX_RATE_UNAVAILABLE


@pytest.mark.parametrize(
    ("build_adapter", "quote"),
    [
        pytest.param(lambda: None, "EUR", id="no-provider-configured"),
        pytest.param(_OfflineAdapter, "EUR", id="provider-unreachable"),
        pytest.param(lambda: _StubAdapter(None), "GBP", id="pair-missing-the-date"),
    ],
)
def test_an_unresolvable_rate_keeps_the_date_out_of_the_logged_message(
    db: Database,
    build_adapter: Callable[[], RateAdapter | None],
    quote: str,
) -> None:
    """The date the user asked about must not reach the durable log.

    In text mode ``handle_cli_errors`` sends ``UserError.message`` to
    ``logger.error``, and the file handler is unfiltered, so the message
    persists to ``cli_YYYY-MM-DD.log``. That call site justifies itself on the
    grounds that ``message`` "IS a fixed MoneyBin string" — an FX date
    interpolated into it breaks exactly that invariant. ``SanitizedLogFormatter``
    is no backstop: its account pattern wants eight consecutive digits and
    ``2026-03-16`` is hyphenated, so nothing masks it.

    The date is not dropped, only moved. ``hint`` goes straight to stderr and is
    never logged (``cli.md`` "Secrets in Error Output"), and it rides the JSON
    envelope as ``ErrorDetail.hint`` — so both surfaces still tell the user which
    day failed.

    Parametrized over all three paths that reach the raise with a date in hand,
    because a fix applied to one message and not its two siblings leaks from the
    others while looking done.
    """
    service = CurrencyService(db, adapter=build_adapter())

    with pytest.raises(RateUnavailableError) as caught:
        service.resolve_rate("USD", quote, _MON)

    asked = _MON.isoformat()
    assert asked not in caught.value.message
    assert caught.value.hint is not None
    assert asked in caught.value.hint


def test_a_broken_provider_is_not_reported_as_an_unreachable_network(
    db: Database,
) -> None:
    """A corrupt 200 must not send the user to wait for a network that is fine.

    The adapter now raises rather than reporting a malformed body as an absent
    series. That fix is only half done if the service then renders every feed
    failure as "could not be reached": a provider answering steady garbage would
    have the user retrying a working connection forever, which is the same wrong
    remedy the adapter fix removed, one layer up.
    """
    service = CurrencyService(db, adapter=_BrokenAdapter())

    with pytest.raises(RateUnavailableError) as caught:
        service.resolve_rate("USD", "EUR", _MON)

    assert caught.value.code == error_codes.FX_RATE_UNAVAILABLE
    assert "could not be reached" not in caught.value.message
    assert caught.value.hint is not None
    assert "network" not in caught.value.hint


def test_an_identity_pair_needs_neither_storage_nor_network(db: Database) -> None:
    adapter = _StubAdapter(None)

    resolved = CurrencyService(db, adapter=adapter).resolve_rate("USD", "USD", _MON)

    assert resolved.rate == Decimal("1")
    assert resolved.rate_date == _MON
    assert adapter.calls == 0
    row = db.execute("SELECT COUNT(*) FROM raw.exchange_rates").fetchone()
    assert row is not None and row[0] == 0


@pytest.mark.parametrize(
    "rate",
    [
        pytest.param(Decimal("0"), id="zero"),
        pytest.param(Decimal("-0.87138"), id="negative"),
        pytest.param(Decimal("0.000000001"), id="rounds-to-zero-at-eight-places"),
        pytest.param(Decimal("1E+30"), id="wider-than-the-column"),
        pytest.param(Decimal("NaN"), id="not-a-number"),
    ],
)
def test_a_fetched_rate_the_column_cannot_hold_fails_as_a_provider_would(
    db: Database, rate: Decimal
) -> None:
    """A provider fault must arrive typed, not as whatever DuckDB raised.

    ``set_override`` refuses these values outright; this is the fetch-path half
    of that rule, and ``PriceService.pull`` hardens the same class for the same
    reason. Without it the value reaches ``_store``: a rate that quantizes to
    exactly 0 trips ``CHECK (rate > 0)`` and an oversized one fails the DECIMAL
    conversion, both as a ``duckdb`` error that ``classify_user_error`` does not
    recognise — so a corrupt response surfaces as a traceback instead of the
    ``RateUnavailableError`` every other unusable answer produces.

    ``NaN`` pins the ordering rather than a fifth failure: comparing it with
    ``>`` raises ``InvalidOperation``, so a magnitude test placed above the
    finite test could never see one.
    """
    adapter = _StubAdapter(
        RateObservation(
            from_currency="USD",
            to_currency="EUR",
            rate_date=_MON,
            rate=rate,
            source_type="frankfurter",
        )
    )

    with pytest.raises(RateUnavailableError) as caught:
        CurrencyService(db, adapter=adapter).resolve_rate("USD", "EUR", _MON)

    assert caught.value.code == error_codes.FX_RATE_UNAVAILABLE
    row = db.execute("SELECT COUNT(*) FROM raw.exchange_rates").fetchone()
    assert row is not None and row[0] == 0, "an unstorable rate is not half-stored"


def test_convert_returns_cents_and_the_rate_behind_them(db: Database) -> None:
    """Requirement 10: a converted figure is never shown without its rate."""
    service = CurrencyService(db, adapter=_StubAdapter(None))
    _cache(db, rate="0.92000000")

    converted, resolved = service.convert(Decimal("100.00"), "USD", "EUR", _MON)

    assert converted == Decimal("92.00")
    assert resolved.rate == Decimal("0.92000000")
    assert resolved.rate_date == _MON


def test_convert_rounds_half_up_rather_than_to_even(db: Database) -> None:
    """Decimal's default is ROUND_HALF_EVEN, which is not how money rounds here.

    1.00 at 0.125 is exactly 0.125 — the tie. Banker's rounding answers 0.12;
    the accounting convention answers 0.13. Left at the default, half of all
    ties round the other way and every converted total drifts.
    """
    service = CurrencyService(db, adapter=_StubAdapter(None))
    _cache(db, rate="0.12500000")

    converted, _ = service.convert(Decimal("1.00"), "USD", "EUR", _MON)

    assert converted == Decimal("0.13")


def test_convert_refuses_rather_than_returning_the_amount_unconverted(
    db: Database,
) -> None:
    """An unconverted amount in a converted total is a silent misstatement."""
    service = CurrencyService(db, adapter=_OfflineAdapter())

    with pytest.raises(RateUnavailableError):
        service.convert(Decimal("100.00"), "USD", "EUR", _MON)


@pytest.mark.parametrize("rate", ["0", "-0.93"])
def test_set_override_refuses_a_non_positive_rate(db: Database, rate: str) -> None:
    """The table's CHECK would refuse it as an untyped DuckDB error; refuse it here."""
    service = CurrencyService(db, adapter=_StubAdapter(None), actor="test")

    with pytest.raises(UserError) as caught:
        service.set_override("USD", "EUR", _MON, Decimal(rate), note=None)

    assert caught.value.code == error_codes.FX_OVERRIDE_RATE_INVALID


def test_set_override_refuses_a_rate_the_column_would_silently_round(
    db: Database,
) -> None:
    """DECIMAL(18,8) stores 8 places. A 9th would report stored and not be."""
    service = CurrencyService(db, adapter=_StubAdapter(None), actor="test")

    with pytest.raises(UserError) as caught:
        service.set_override("USD", "EUR", _MON, Decimal("0.123456789"), note=None)

    assert caught.value.code == error_codes.FX_OVERRIDE_RATE_INVALID


def test_set_override_refuses_a_pair_that_prices_itself(db: Database) -> None:
    """An identity pair answers 1 before any override is read.

    Accepting the write would store an audited correction that ``list_rates``
    displays and ``resolve_rate`` can never honour — the succeeds-and-joins-
    nothing failure ``require_currency`` refuses a malformed code for, reached
    through a pair that is spelled perfectly well.

    The two codes are spelled differently on purpose: the rule is about the
    canonical pair, so a check placed above canonicalization would let ``usd``
    through against ``USD``.
    """
    service = CurrencyService(db, adapter=_StubAdapter(None), actor="test")

    with pytest.raises(UserError) as caught:
        service.set_override("USD", " usd ", _MON, Decimal(2), note=None)

    assert caught.value.code == error_codes.FX_OVERRIDE_PAIR_INVALID
    assert service.list_rates("USD", "USD") == []


@pytest.mark.parametrize("code", ["US", "USDX"])
def test_set_override_refuses_a_malformed_currency_code(
    db: Database, code: str
) -> None:
    """A malformed code writes successfully and matches no conversion, forever."""
    service = CurrencyService(db, adapter=_StubAdapter(None), actor="test")

    with pytest.raises(UserError) as caught:
        service.set_override(code, "EUR", _MON, Decimal("0.93"), note=None)

    assert caught.value.code == error_codes.FX_CURRENCY_INVALID


def test_a_lowercase_override_is_found_by_an_uppercase_lookup(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One canonical spelling, or a correction is unreachable from the read path."""
    restated: list[Database] = []
    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
        restated.append,
    )
    service = CurrencyService(db, adapter=_StubAdapter(None), actor="test")
    service.set_override(" usd ", "eur", _MON, Decimal("0.95"), note=None)

    assert service.resolve_rate("USD", "EUR", _MON).rate == Decimal("0.95")
    assert len(restated) == 1


def test_delete_override_returns_the_date_to_the_provider_rate(db: Database) -> None:
    service = CurrencyService(db, adapter=_StubAdapter(None), actor="test")
    _cache(db, rate="0.92000000")
    service.set_override("USD", "EUR", _MON, Decimal("0.95"), note="typo")

    assert service.delete_override("USD", "EUR", _MON) is True

    resolved = service.resolve_rate("USD", "EUR", _MON)
    assert resolved.rate == Decimal("0.92000000")
    assert resolved.source == "frankfurter"


def test_delete_override_reports_that_there_was_nothing_to_delete(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A silent success reads as 'the override is gone' when there never was one."""
    restated: list[Database] = []
    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
        restated.append,
    )
    service = CurrencyService(db, adapter=_StubAdapter(None), actor="test")

    assert service.delete_override("USD", "EUR", _MON) is False
    assert restated == []


def test_override_remains_saved_when_fx_restatement_fails(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    failure = UserError(
        "The setting was saved, but derived FX accounting could not be rebuilt.",
        code=error_codes.REFRESH_MODEL_FAILED,
    )

    def fail_restatement(_db: Database) -> None:
        raise failure

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
        fail_restatement,
    )
    service = CurrencyService(db, adapter=_StubAdapter(None), actor="test")

    with pytest.raises(UserError) as caught:
        service.set_override("USD", "EUR", _MON, Decimal("0.95"), note=None)

    assert caught.value.code == error_codes.REFRESH_MODEL_FAILED
    assert service.resolve_rate("USD", "EUR", _MON).rate == Decimal("0.95000000")


def test_list_rates_shows_the_override_that_won_each_date(db: Database) -> None:
    """The series a user checks must be the rates that applied, not every candidate."""
    service = CurrencyService(db, adapter=_StubAdapter(None), actor="test")
    _cache(db, rate_date=_FRI, rate="0.92010000")
    _cache(db, rate_date=_MON, rate="0.92000000")
    service.set_override("USD", "EUR", _MON, Decimal("0.95"), note=None)

    series = service.list_rates("USD", "EUR")

    assert [(r.rate_date, r.rate, r.source) for r in series] == [
        (_MON, Decimal("0.95000000"), "override"),
        (_FRI, Decimal("0.92010000"), "frankfurter"),
    ]


def test_list_rates_honours_a_since_bound(db: Database) -> None:
    service = CurrencyService(db, adapter=_StubAdapter(None), actor="test")
    _cache(db, rate_date=_FRI, rate="0.92010000")
    _cache(db, rate_date=_MON, rate="0.92000000")

    series = service.list_rates("USD", "EUR", since=_MON)

    assert [r.rate_date for r in series] == [_MON]


def test_each_resolution_records_which_layer_answered(db: Database) -> None:
    """A rising 'fetched' share against flat 'cached' means the cache is not reused."""
    before = {
        outcome: _outcome(outcome) for outcome in ("override", "cached", "fetched")
    }
    adapter = _StubAdapter(
        RateObservation("USD", "GBP", _MON, Decimal("0.79"), "frankfurter")
    )
    service = CurrencyService(db, adapter=adapter, actor="test")

    service.resolve_rate("USD", "GBP", _MON)  # fetched
    service.resolve_rate("USD", "GBP", _MON)  # cached
    service.set_override("USD", "EUR", _MON, Decimal("0.95"), note=None)
    service.resolve_rate("USD", "EUR", _MON)  # override

    assert _outcome("fetched") == before["fetched"] + 1
    assert _outcome("cached") == before["cached"] + 1
    assert _outcome("override") == before["override"] + 1


# ----------------------------- bulk cache writes -----------------------------


def test_many_observations_are_stored_in_one_write(db: Database) -> None:
    """The backfill arrives with a span of rates, not one."""
    service = CurrencyService(db, adapter=None)

    written = service.store_observations((
        RateObservation("EUR", "USD", _MON, Decimal("1.1555"), "frankfurter"),
        RateObservation("EUR", "USD", _TUE, Decimal("1.1641"), "frankfurter"),
    ))

    assert written == 2
    assert service.resolve_rate("EUR", "USD", _MON).rate == Decimal("1.15550000")


def test_storing_a_rate_already_cached_writes_nothing(db: Database) -> None:
    """Append-only: a published rate for a date is a fact, never rewritten.

    The count is what a refresh reports, so it must be rows actually WRITTEN.
    Counting rows offered would make a completely stalled feed look productive,
    because a re-run offers the whole span back every time.
    """
    _cache(
        db, from_currency="EUR", to_currency="USD", rate_date=_MON, rate="1.15550000"
    )
    service = CurrencyService(db, adapter=None)

    written = service.store_observations((
        RateObservation("EUR", "USD", _MON, Decimal("9.99"), "frankfurter"),
    ))

    assert written == 0
    assert service.resolve_rate("EUR", "USD", _MON).rate == Decimal("1.15550000")


def test_storing_nothing_is_not_a_write(db: Database) -> None:
    """A pair the provider had nothing new for must not open a write at all."""
    assert CurrencyService(db, adapter=None).store_observations(()) == 0
