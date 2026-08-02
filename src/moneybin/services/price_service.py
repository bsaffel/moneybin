"""Turns a held position into stored price observations.

Two jobs the adapters deliberately do not do:

1. **Decide which securities to fetch, and over what window.** Derived from open
   positions in ``core.dim_holdings``. Fetching the whole catalog across its full
   history exhausts a provider's rate limit on every sync and stores rows no
   report reads.
2. **Derive the provider key, and decide whether that derivation is certain
   enough to act on silently.** The adapters take a key and never infer one, so
   the binding-certainty judgement lives here in one place.

Both feed keys bind through ``SecurityLinksRepo``, the same audited path the
Plaid resolver uses, so a binding is reversible and reviewable rather than
recomputed from a heuristic on every pull. Reads stay in this service per the
repo convention; only mutations go through a ``*Repo``.

See ``docs/specs/investments-price-feeds.md``.
"""

from __future__ import annotations

import difflib
import logging
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Protocol

import duckdb
import polars as pl

from moneybin import error_codes
from moneybin.connectors.prices.errors import PriceFeedError
from moneybin.connectors.prices.protocol import (
    PriceFetchResult,
    PriceObservation,
    SecurityRef,
)
from moneybin.errors import UserError
from moneybin.metrics.registry import (
    PRICE_REFRESH_DURATION_SECONDS,
    PRICE_REFRESH_SECURITIES_TOTAL,
    PRICE_ROWS_WRITTEN_TOTAL,
)
from moneybin.repositories.security_link_decisions_repo import (
    SecurityLinkDecisionsRepo,
)
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.repositories.security_price_repo import SecurityPriceRepo
from moneybin.services._validators import validate_currency_code, validate_note_text
from moneybin.tables import (
    AUDIT_LOG,
    DIM_HOLDINGS,
    FCT_SECURITY_PRICES,
    SECURITIES,
    SECURITY_LINK_DECISIONS,
    SECURITY_LINKS,
    SECURITY_PRICES,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from moneybin.connectors.prices.tiingo import TickerMetadata
    from moneybin.database import Database

logger = logging.getLogger(__name__)

TIINGO_SOURCE_TYPE = "tiingo"
COINGECKO_SOURCE_TYPE = "coingecko"
TIINGO_REF_KIND = "tiingo_ticker"
COINGECKO_REF_KIND = "coingecko_slug"

# Security types Tiingo prices. Crypto routes to CoinGecko instead; cash and
# other carry no market quote at all.
_TIINGO_SECURITY_TYPES = frozenset({"equity", "etf", "mutual_fund", "bond"})

# Matches SecurityResolver's name cutoff — the same question ("do these two
# strings name the same issuer?") must not have two different answers.
_NAME_AGREEMENT_CUTOFF = 0.85

# A share class written as a letter followed by digits: "Class A1", "Class R6".
# No English word takes that shape, so matching it cannot swallow an abbreviation
# the way a plain length test would — see _discriminators_differ.
_CLASS_LABEL = re.compile(r"^[a-z]\d{1,2}$")

# Corporate-form suffixes carry no identifying information: "Apple Inc." and
# "Apple Inc" are one issuer, and confirming that difference is exactly the queue
# noise the spec forbids.
_CORPORATE_SUFFIXES = frozenset({
    "inc",
    "incorporated",
    "corp",
    "corporation",
    "co",
    "company",
    "ltd",
    "limited",
    "plc",
    "llc",
    "lp",
    "sa",
    "nv",
    "ag",
    "se",
    "the",
    "trust",
    "fund",
    "etf",
    "class",
})

# How far back a pull reaches when the caller names no window. Four days absorbs
# a weekend plus a holiday, which is what an incremental refresh needs; a deeper
# backfill is an explicit `since`, never a silent default that re-requests years
# of history on every sync.
_DEFAULT_LOOKBACK_DAYS = 4

# What a stored close can actually be. Both price columns —
# raw.security_prices.close and app.security_price_overrides.close — are
# DECIMAL(28, 10), so the representable set is one shared property of the
# storage: named once here, and applied by both write paths.
#
# PRICE_QUANTUM is also the smallest positive close. Anything below it quantizes
# to exactly 0 — silently, and *after* an adapter's own `close <= 0` guard has
# passed on the full-precision value. The stored CHECK (close > 0) then rejects
# the row, and on the pull path DuckDB aborts the whole multi-row insert, losing
# every well-priced security batched with it. So the pull path drops such an
# observation one at a time and reports it as that security's outcome, while
# set_mark refuses it outright: a mark is one deliberate value, and storing a
# number other than the one echoed back is worse than declining it. Widening the
# column instead would trade a visible refusal for a stored price no downstream
# total could represent either.
PRICE_PRECISION = 28
PRICE_SCALE = 10
PRICE_WHOLE_DIGITS = PRICE_PRECISION - PRICE_SCALE
PRICE_QUANTUM = Decimal(1).scaleb(-PRICE_SCALE)
MAX_STORED_PRICE = Decimal(10) ** PRICE_WHOLE_DIGITS - PRICE_QUANTUM

_RAW_PRICE_SCHEMA = {
    "provider_security_key": pl.Utf8,
    "price_date": pl.Date,
    "quote_currency": pl.Utf8,
    "source_type": pl.Utf8,
    "source_origin": pl.Utf8,
    "close": pl.Decimal(PRICE_PRECISION, PRICE_SCALE),
    "price_basis": pl.Utf8,
}

# app.security_links.reversed_by is a closed vocabulary ('auto' | 'user' |
# 'system'). A reversal this service performs to retire a binding whose catalog
# value moved is machine bookkeeping, so it records 'auto' — and must stay
# distinguishable from 'user', which is a judgement about the pairing that a
# later pull must not silently overturn.
_AUTO_REVERSAL = "auto"

# The audit action a `system audit undo` of a feed-key binding leaves behind.
# BaseRepo.undo_event names an undo "<original action>.undo" and carries the
# deleted row in its before_value, which is the only surviving record that the
# binding existed — undoing an INSERT removes the app.security_links row itself.
_LINK_INSERT_UNDO = "security_link.insert.undo"


@dataclass(frozen=True, slots=True)
class HeldSecurity:
    """One security with an open position, plus the catalog attributes a key needs."""

    security_id: str
    name: str
    security_type: str
    quote_currency: str
    ticker: str | None
    exchange: str | None
    coingecko_id: str | None


@dataclass(frozen=True, slots=True)
class PriceRow:
    """One resolved price from core.fct_security_prices."""

    price_date: date
    quote_currency: str
    close: Decimal
    source_type: str
    price_basis: str


@dataclass(frozen=True, slots=True)
class PricesResult:
    """The resolved series for one security, newest first."""

    security_id: str
    rows: tuple[PriceRow, ...]


@dataclass(frozen=True, slots=True)
class UnpricedSecurity:
    """A held security no observation was stored for, and why."""

    security_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class FailedSource:
    """One price source that failed as a whole, and the message to act on."""

    source_type: str
    message: str


@dataclass(frozen=True, slots=True)
class PullResult:
    """What one refresh did. Partial success is the normal outcome."""

    rows_written: int
    observations: int
    securities_priced: int
    queued_for_review: int
    unpriced: tuple[UnpricedSecurity, ...]
    failed_sources: tuple[FailedSource, ...] = ()


class _PriceAdapter(Protocol):
    """The adapter surface this service drives."""

    source_type: str
    price_basis: str

    def fetch(
        self, securities: Sequence[SecurityRef], start: date, end: date
    ) -> PriceFetchResult: ...


class _TiingoLike(_PriceAdapter, Protocol):
    """Tiingo additionally answers "what does this symbol name?"."""

    def fetch_metadata(self, ticker: str) -> TickerMetadata | None: ...


@dataclass(frozen=True, slots=True)
class _Derivation:
    """The outcome of deriving one feed key.

    ``provider_ticker`` / ``provider_name`` carry what the provider said about
    the symbol, so a queued review can record the divergence that caused it.
    They stay ``None`` when the provider was never consulted — a ticker that is
    ambiguous inside our own catalog is refused before any round-trip.
    """

    ref_value: str | None
    review_reason: str | None = None
    unpriced_reason: str | None = None
    provider_ticker: str | None = None
    provider_name: str | None = None


@dataclass(frozen=True, slots=True)
class _Binding:
    """An accepted feed-key link, paired with who decided it.

    ``decided_by`` is not incidental: only an auto-derived key may be retired
    when the catalog moves, so every staleness judgement needs it.
    """

    ref_value: str
    decided_by: str


class PriceService:
    """Refreshes stored prices for held securities."""

    def __init__(
        self,
        db: Database,
        *,
        tiingo: _TiingoLike | None = None,
        coingecko: _PriceAdapter | None = None,
        actor: str = "system",
        today: date | None = None,
    ) -> None:
        """Initialize with injected adapters; ``today`` is a test seam only.

        The default is the UTC date, not ``date.today()``. Every provider day this
        service reasons about is a UTC day — CoinGecko's close for D is the 00:00
        UTC point of D+1 — so a host-local clock disagrees with the data whenever
        the host is not on UTC. East of UTC the local date turns over first, and
        ``pull`` would name a UTC day whose close does not exist yet; west of UTC
        it turns over last, and a completed UTC day goes unfetched until local
        catches up. The first is the dangerous one: raw.security_prices is
        append-only with on_conflict="ignore", so a partial close written once
        owns that date permanently.
        """
        self._db = db
        self._tiingo = tiingo
        self._coingecko = coingecko
        self._actor = actor
        self._today = today or datetime.now(UTC).date()
        self._links = SecurityLinksRepo(db)
        self._decisions = SecurityLinkDecisionsRepo(db)

    # ------------------------------- reads -------------------------------

    def held_securities(
        self, security_ids: Sequence[str] | None = None
    ) -> list[HeldSecurity]:
        """Every security with a non-zero open position, with its catalog attributes.

        A zero quantity is a closed position, so its price answers no question. A
        negative quantity is a short — still open, still needs a market value.
        """
        sql = f"""
            SELECT DISTINCT
                s.security_id, s.name, s.security_type, s.currency_code,
                s.ticker, s.exchange, s.coingecko_id
            FROM {SECURITIES.full_name} AS s
            JOIN {DIM_HOLDINGS.full_name} AS h ON h.security_id = s.security_id
            WHERE h.quantity <> 0
            ORDER BY s.security_id
        """  # noqa: S608  # TableRef constants only
        try:
            rows = self._db.execute(sql).fetchall()
        except duckdb.CatalogException:
            # A database whose core models have never been built has no holdings
            # to price; that is an empty refresh, not a failure.
            logger.warning("core.dim_holdings is absent — no held securities to price")
            return []
        wanted = set(security_ids) if security_ids is not None else None
        return [
            HeldSecurity(
                security_id=str(row[0]),
                name=str(row[1]),
                security_type=str(row[2]),
                quote_currency=str(row[3] or "USD"),
                ticker=_clean(row[4]),
                exchange=_clean(row[5]),
                coingecko_id=_clean(row[6]),
            )
            for row in rows
            if wanted is None or str(row[0]) in wanted
        ]

    # ------------------------------- pull -------------------------------

    def pull(
        self,
        *,
        security_ids: Sequence[str] | None = None,
        since: date | None = None,
    ) -> PullResult:
        """Refresh stored prices for held securities over the requested window."""
        # Never today. raw.security_prices is append-only with on_conflict="ignore"
        # and price_date in its primary key, so whoever writes a date first owns it
        # permanently — a midday pull that stored an in-progress close would make
        # the evening pull carrying the settled close a silent no-op, and that
        # date's valuation wrong forever. data-extraction.md forbids partial-day
        # extraction for this reason. CoinGecko is already incapable of it (its
        # close for date D is the 00:00 UTC point of D+1, which does not exist
        # yet); this gives the equity path the same bound, and makes both
        # providers agree on the newest date a pull can produce — which
        # investment_price_disagreement compares them on.
        end = self._today - timedelta(days=1)
        if since is not None and since > end:
            # An inverted window is a usage error, and every provider expresses it
            # as something else: Tiingo is handed start > end, CoinGecko matches no
            # observation at all, and both come back as "this security could not be
            # priced". The user reads a feed outage where they mistyped a date. Refuse
            # before the first request so no quota answers the question either.
            raise UserError(
                f"--since {since.isoformat()} is after the last complete day. "
                f"A pull never requests today's in-progress close, so the newest "
                f"date it can return is {end.isoformat()}.",
                code=error_codes.INVESTMENT_DATE_RANGE_INVALID,
            )
        start = since or end - timedelta(days=_DEFAULT_LOOKBACK_DAYS)
        held = self.held_securities(security_ids)
        unpriced: list[UnpricedSecurity] = []
        queued = 0
        by_source: dict[str, list[tuple[HeldSecurity, str]]] = {}
        # Sources whose key derivation hit a whole-batch condition, keyed to the
        # message that says how to fix it.
        derivation_failures: dict[str, str] = {}

        for security in held:
            source_type, adapter = self._route(security)
            if adapter is None:
                unpriced.append(
                    UnpricedSecurity(security.security_id, "no_price_source")
                )
                PRICE_REFRESH_SECURITIES_TOTAL.labels(
                    source_type=source_type, outcome="skipped"
                ).inc()
                continue
            if source_type in derivation_failures:
                # Already answered for this source. Asking again buys the same
                # error and spends another request — and on a rate limit, deepens
                # the very limit that caused it.
                unpriced.append(
                    UnpricedSecurity(security.security_id, "price_feed_error")
                )
                PRICE_REFRESH_SECURITIES_TOTAL.labels(
                    source_type=source_type, outcome="failed"
                ).inc()
                continue
            try:
                derivation = self._feed_key(security, source_type, adapter)
            except PriceFeedError as exc:
                # Deriving a key can call the provider — Tiingo's metadata lookup
                # — so the same whole-batch conditions that `adapter.fetch()`
                # raises reach this loop too, and on a first pull, when nothing
                # is bound yet, this is where a missing token surfaces first.
                # Uncontained it leaves pull() before `_store()` ever runs,
                # discarding the rows of every source that needed no credential.
                # The message is not logged: only the count and the condition
                # are, per the no-PII rule.
                logger.warning(
                    f"{source_type} feed-key derivation failed "
                    f"({type(exc).__name__}) — other sources continue"
                )
                derivation_failures[source_type] = str(exc)
                unpriced.append(
                    UnpricedSecurity(security.security_id, "price_feed_error")
                )
                PRICE_REFRESH_SECURITIES_TOTAL.labels(
                    source_type=source_type, outcome="failed"
                ).inc()
                continue
            if derivation.ref_value is None:
                PRICE_REFRESH_SECURITIES_TOTAL.labels(
                    source_type=source_type, outcome="skipped"
                ).inc()
                if derivation.review_reason is not None:
                    if self._queue_review(security, source_type, derivation):
                        queued += 1
                    unpriced.append(
                        UnpricedSecurity(security.security_id, "queued_for_review")
                    )
                else:
                    unpriced.append(
                        UnpricedSecurity(
                            security.security_id,
                            derivation.unpriced_reason or "no_feed_key",
                        )
                    )
                continue
            by_source.setdefault(source_type, []).append((
                security,
                derivation.ref_value,
            ))

        observations: list[PriceObservation] = []
        priced_keys: set[tuple[str, str]] = set()
        failed_sources: list[FailedSource] = []
        for source_type, entries in by_source.items():
            adapter = self._adapter_for(source_type)
            if adapter is None:  # pragma: no cover — routed above
                continue
            refs = [
                SecurityRef(
                    provider_security_key=key, quote_currency=sec.quote_currency
                )
                for sec, key in entries
            ]
            try:
                with PRICE_REFRESH_DURATION_SECONDS.labels(
                    source_type=source_type
                ).time():
                    result = adapter.fetch(refs, start, end)
            except PriceFeedError as exc:
                # A whole-batch condition — an absent credential, a rate limit, an
                # unreachable host. By construction it says nothing about any
                # other source, and the observations already collected are still
                # good, so this source drops out and the refresh carries on. The
                # alternative loses a credential-free feed's rows to a missing
                # token it never needed. The message is not logged: only the
                # count and the condition are, per the no-PII rule.
                logger.warning(
                    f"{source_type} price fetch failed for {len(refs)} "
                    f"securities ({type(exc).__name__}) — other sources continue"
                )
                failed_sources.append(FailedSource(source_type, str(exc)))
                for sec, _key in entries:
                    unpriced.append(
                        UnpricedSecurity(sec.security_id, "price_feed_error")
                    )
                    PRICE_REFRESH_SECURITIES_TOTAL.labels(
                        source_type=source_type, outcome="failed"
                    ).inc()
                continue
            # Both halves of what DECIMAL(28,10) cannot hold, checked here so one
            # unrepresentable quote costs one security instead of the batch.
            # Neither failure is loud on its own: a sub-quantum close quantizes to
            # exactly 0 and trips CHECK (close > 0), and an oversized one becomes
            # NULL in frame construction and trips NOT NULL. Both surface from
            # _store() as a duckdb.ConstraintException, which is outside the
            # per-source PriceFeedError containment above and outside what
            # classify_user_error recognises — so the whole pull dies and every
            # well-priced security fetched with it is discarded. set_mark refuses
            # the same two values outright; this is the pull-path half of that
            # rule, and it checks magnitude before precision for the reason
            # set_mark does: quantizing a number this large overflows the decimal
            # context before a precision test could answer.
            unstorable: dict[str, str] = {}
            for obs in result.observations:
                if obs.close > MAX_STORED_PRICE:
                    unstorable[obs.provider_security_key] = "close_above_storable_range"
                    continue
                if obs.close < PRICE_QUANTUM:
                    unstorable[obs.provider_security_key] = (
                        "close_below_storable_precision"
                    )
                    continue
                observations.append(obs)
                priced_keys.add((source_type, obs.provider_security_key))
            failed = {f.provider_security_key: f.reason for f in result.failures}
            for sec, key in entries:
                priced = (source_type, key) in priced_keys
                if key in failed:
                    unpriced.append(UnpricedSecurity(sec.security_id, failed[key]))
                elif key in unstorable and not priced:
                    unpriced.append(UnpricedSecurity(sec.security_id, unstorable[key]))
                # Exhaustive and disjoint over the fetched set: a security the
                # adapter answered for neither way returned no data without
                # calling it an error, which is a skip, not a failure.
                outcome = (
                    "failed"
                    if key in failed or (key in unstorable and not priced)
                    else "written"
                    if priced
                    else "skipped"
                )
                PRICE_REFRESH_SECURITIES_TOTAL.labels(
                    source_type=source_type, outcome=outcome
                ).inc()

        # A source can fail at derivation, at fetch, or both. Report it once,
        # preferring the fetch message already collected: contained failures are
        # only useful if the remedy still reaches the user.
        failed_sources.extend(
            FailedSource(source_type, message)
            for source_type, message in derivation_failures.items()
            if all(failed.source_type != source_type for failed in failed_sources)
        )

        written = self._store(observations)
        priced_securities = {
            sec.security_id
            for source_type, entries in by_source.items()
            for sec, key in entries
            if (source_type, key) in priced_keys
        }
        return PullResult(
            rows_written=written,
            observations=len(observations),
            securities_priced=len(priced_securities),
            queued_for_review=queued,
            unpriced=tuple(unpriced),
            failed_sources=tuple(failed_sources),
        )

    def resolve_security(self, ref: str) -> str:
        """Resolve a free-text security reference to a canonical ``security_id``.

        Delegates to ``InvestmentService.resolve_security`` rather than repeating
        the ladder: ``identifiers.md`` Guard 2 requires filters to bind to the id
        and resolution to happen once at the service boundary, and two resolvers
        for one question would drift.
        """
        from moneybin.services.investment_service import (  # noqa: PLC0415  # avoids an import cycle
            InvestmentService,
        )

        return InvestmentService(self._db).resolve_security(ref)

    # ---------------------------- user marks ----------------------------

    def resolve_quote_currency(
        self, security_id: str, explicit: str | None = None
    ) -> str:
        """The currency a mark must carry to reach this security's positions.

        ``core.dim_holdings`` values a position only where
        ``lp.quote_currency = UPPER(p.currency_code)``, so a mark quoted in any
        other currency is written, reported as a success, and joins to nothing.
        That failure is invisible when it happens and stays invisible, which is
        why the currency is derived rather than defaulted.

        A resolution ladder, not one lookup, mirroring the reference-resolution
        rule in ``surface-design.md`` — most specific evidence first, refuse on
        ambiguity, never guess:

        0. **What the caller said**, canonicalized and validated. Naming the
           currency is the escape hatch for every case below that refuses, so it
           is answered without consulting the holdings at all.
        1. **The open positions' currency**, when they agree. This is the value
           the join actually compares, so it is what makes the mark do its job.
        2. **Two open positions disagreeing** — refuse. One instrument held in
           two denominations is a question only the user can answer.
        3. **Nothing held, but the catalog declares a currency.** Marking a
           security before holding it is legitimate (a 409A valuation recorded
           ahead of the purchase), and ``app.securities.currency_code`` is a
           declared fact rather than a default. It is also what ``_held()``
           quotes provider prices in, so the mark lands in the series the
           provider will later write to instead of beside it.
        4. **Neither** — refuse.

        Every rung returns a canonical code, so callers never normalize: a
        surface that upper-cased its own input would be a second spelling rule
        beside this one, and the two would disagree the first time either moved.
        """
        if explicit is not None:
            return self._canonical_quote_currency(explicit)
        try:
            rows = self._db.execute(
                f"SELECT DISTINCT UPPER(currency_code) FROM {DIM_HOLDINGS.full_name} "  # noqa: S608  # TableRef constant
                "WHERE security_id = ? AND quantity <> 0 "
                "AND currency_code IS NOT NULL",
                [security_id],
            ).fetchall()
        except duckdb.CatalogException:
            # Core models never built — the same "nothing held" state as an empty
            # table, so fall through to the catalog rather than failing here.
            rows = []
        currencies = sorted(str(row[0]) for row in rows)
        if len(currencies) == 1:
            return currencies[0]
        if not currencies:
            declared = self._db.execute(
                f"SELECT UPPER(currency_code) FROM {SECURITIES.full_name} "  # noqa: S608  # TableRef constant
                "WHERE security_id = ? AND currency_code IS NOT NULL",
                [security_id],
            ).fetchone()
            if declared and declared[0]:
                return str(declared[0])
        detail = (
            f"is held in {' and '.join(currencies)}"
            if currencies
            else "has no open position and no catalog currency"
        )
        raise UserError(
            f"Cannot tell which currency to mark {security_id} in: it {detail}. "
            "Pass --currency to say which series this price belongs to — a mark "
            "only values a holding quoted in the same currency.",
            code=error_codes.INVESTMENT_PRICE_MARK_CURRENCY_AMBIGUOUS,
        )

    def set_mark(
        self,
        security_id: str,
        price_date: date,
        close: Decimal,
        *,
        quote_currency: str,
        note: str | None,
    ) -> None:
        """Record the user's own price for one security, date, and currency.

        Refuses a non-positive close. The guarantee "an unpriced holding is NULL,
        never zero" would otherwise have a hole on exactly the path a user
        controls: a genuinely worthless position is a ledger event — a disposal or
        write-off — not a zero price, and admitting zero here would make
        *worthless* and *unknown* two states every downstream total, report, and
        doctor check has to tell apart.
        """
        # Storability first, and specifically the finite check inside it: comparing
        # a Decimal NaN with `<=` raises InvalidOperation, so a guard placed after
        # the positivity test could never see one.
        self._require_storable(close)
        if close <= 0:
            raise ValueError(
                "A price mark must be positive. A worthless position is recorded "
                "as a disposal or write-off in the ledger, not as a zero price."
            )
        if note is not None:
            # DuckDB VARCHAR is unbounded, so the bound has to be the application's.
            # A mark is corrected in place, and every correction copies the note into
            # its audit before/after image — so one oversized string is stored and
            # re-stored, not stored once. `validate_note_text` is the bound the rest
            # of the codebase already applies to user note text; reusing it keeps one
            # limit rather than a second one that drifts. `None` still means "no
            # note"; an empty string is a slip and is refused as one.
            validate_note_text(note)
        SecurityPriceRepo(self._db).set(
            security_id,
            price_date,
            self._canonical_quote_currency(quote_currency),
            close=close,
            note=note,
            actor=self._actor,
        )

    def delete_mark(
        self, security_id: str, price_date: date, *, quote_currency: str
    ) -> bool:
        """Remove one mark, returning ``True`` if a row was actually deleted.

        Reporting the no-op matters: a silent success reads as "the override is
        gone" when there was never one, which is the same observable state for the
        wrong reason.
        """
        event = SecurityPriceRepo(self._db).delete(
            security_id,
            price_date,
            self._canonical_quote_currency(quote_currency),
            actor=self._actor,
        )
        return event is not None

    @staticmethod
    def _require_storable(close: Decimal) -> None:
        """Refuse a close ``DECIMAL(28, 10)`` would silently alter or reject.

        The success payload echoes the number the caller passed, so a value the
        column rounds is reported as stored and is not. The two failures differ:
        excess precision quantizes — below one quantum, all the way to zero,
        which then trips the table's own ``CHECK (close > 0)`` as an untyped
        DuckDB error the CLI can only render as a traceback — while excess
        magnitude fails the conversion outright. Both are usage errors, so both
        are refused here rather than left to the storage layer to express.
        """
        if not close.is_finite():
            raise UserError(
                "A price mark must be a finite number.",
                code=error_codes.INVESTMENT_PRICE_MARK_UNREPRESENTABLE,
            )
        # Magnitude first: quantizing a value this large would itself overflow
        # the decimal context before the precision check could answer. Read as an
        # absolute value, because this runs before the caller's positivity rule —
        # it has to, since the finite check above cannot follow a `<= 0` on NaN —
        # so every negative passes through here, and a signed bound would hand
        # `quantize` the one input it cannot answer.
        if abs(close) > MAX_STORED_PRICE:
            raise UserError(
                f"A price mark carries at most {PRICE_WHOLE_DIGITS} digits before "
                "the decimal point; this price is larger than that.",
                code=error_codes.INVESTMENT_PRICE_MARK_UNREPRESENTABLE,
            )
        if close != close.quantize(PRICE_QUANTUM):
            raise UserError(
                f"A price mark is stored to {PRICE_SCALE} decimal places; this "
                "price carries more precision than that, so storing it would "
                "record a different number than the one reported back. Round it "
                "first.",
                code=error_codes.INVESTMENT_PRICE_MARK_UNREPRESENTABLE,
            )

    def _canonical_quote_currency(self, value: str) -> str:
        """Normalize and validate a currency before it becomes part of a series key.

        A malformed code is not a rejected input — it is a mark that writes,
        reports success, and values nothing. ``core.dim_holdings`` matches a mark
        to a position on exact string equality, so ``USDX`` or a padded ``" USD "``
        joins to no position, forever and without a symptom.

        Both writers share this one canonicalization deliberately: ``set`` is the
        only way to create an override and ``delete`` the only way to remove one,
        so if they normalized differently a mark written under one spelling would
        be unreachable under the other.
        """
        candidate = value.strip().upper()
        try:
            validate_currency_code(candidate)
        except ValueError as exc:
            raise UserError(
                f"{value!r} is not an ISO-4217 currency code. A mark only values "
                "a holding quoted in exactly the same currency, so a code like "
                "this one would store successfully and match no position.",
                code=error_codes.INVESTMENT_PRICE_MARK_CURRENCY_INVALID,
            ) from exc
        return candidate

    def list_prices(
        self,
        security_id: str,
        *,
        since: date | None = None,
        source_type: str | None = None,
    ) -> PricesResult:
        """The resolved series for one security, newest first.

        Reads ``core.fct_security_prices`` rather than ``raw``: the question a user
        asks is which price applies, and that is the resolved winner per date, not
        every observation that competed for it.
        """
        clauses = ["security_id = ?"]
        params: list[object] = [security_id]
        if since is not None:
            clauses.append("price_date >= ?")
            params.append(since)
        if source_type is not None:
            clauses.append("source_type = ?")
            params.append(source_type)
        where = " AND ".join(clauses)
        try:
            rows = self._db.execute(
                f"SELECT price_date, quote_currency, close, source_type, price_basis "  # noqa: S608  # TableRef + parameterized values
                f"FROM {FCT_SECURITY_PRICES.full_name} WHERE {where} "
                "ORDER BY price_date DESC, quote_currency",
                params,
            ).fetchall()
        except duckdb.CatalogException:
            logger.warning(
                "core.fct_security_prices is absent — run 'moneybin refresh run'"
            )
            return PricesResult(security_id=security_id, rows=())
        return PricesResult(
            security_id=security_id,
            rows=tuple(
                PriceRow(
                    price_date=row[0],
                    quote_currency=str(row[1]),
                    close=row[2],
                    source_type=str(row[3]),
                    price_basis=str(row[4]),
                )
                for row in rows
            ),
        )

    # --------------------------- key derivation ---------------------------

    def _route(self, security: HeldSecurity) -> tuple[str, _PriceAdapter | None]:
        """Which provider prices this security type."""
        if security.security_type == "crypto":
            return COINGECKO_SOURCE_TYPE, self._coingecko
        if security.security_type in _TIINGO_SECURITY_TYPES:
            return TIINGO_SOURCE_TYPE, self._tiingo
        # cash and other carry no market quote; a sweep position's unit value is
        # its face value, not a traded price.
        return TIINGO_SOURCE_TYPE, None

    def _adapter_for(self, source_type: str) -> _PriceAdapter | None:
        if source_type == COINGECKO_SOURCE_TYPE:
            return self._coingecko
        return self._tiingo

    def _feed_key(
        self, security: HeldSecurity, source_type: str, adapter: _PriceAdapter
    ) -> _Derivation:
        """The provider key for this security, or why there isn't one.

        Rung 1 — an accepted binding wins outright and costs no provider call.
        The user already confirmed it, or a previous pull derived it with
        certainty; re-deriving would re-ask a settled question.
        """
        ref_kind = (
            COINGECKO_REF_KIND
            if source_type == COINGECKO_SOURCE_TYPE
            else TIINGO_REF_KIND
        )
        bound = self._bound_ref(security.security_id, ref_kind, source_type)
        stale = bound is not None and self._binding_is_stale(
            security, source_type, bound
        )
        if bound is not None and not stale:
            return _Derivation(ref_value=bound.ref_value)

        settled = self._review_settled(ref_kind, source_type, security)
        if settled is not None:
            return _Derivation(ref_value=None, unpriced_reason=settled)

        if source_type == COINGECKO_SOURCE_TYPE:
            derivation = self._coingecko_key(security)
        else:
            derivation = self._tiingo_key(security, adapter)

        if derivation.ref_value is not None and self._was_reversed_by_user(
            security.security_id, ref_kind, source_type, derivation.ref_value
        ):
            # The user reversed exactly this binding. Re-deriving reaches the same
            # conclusion from the same inputs, so re-binding silently would undo
            # their undo and restore the valuation they rejected. The reversal is
            # the signal the derivation itself cannot see.
            return _Derivation(
                ref_value=None,
                review_reason="binding_was_reversed",
                provider_ticker=derivation.provider_ticker,
                provider_name=derivation.provider_name,
            )

        if derivation.ref_value is not None:
            if stale:
                # The catalog value this key was derived from has moved, so the old
                # binding now points at a different company's series and must go.
                # It is retired HERE — after a replacement has been derived, and not
                # before — because it is the only key pricing this security, and
                # every way the DERIVATION can fail (no provider coverage for the
                # new symbol, a transient metadata error, an ambiguous match queued
                # for review) would otherwise strand a holding that was valued
                # yesterday with no accepted link and nothing saying why.
                #
                # The INSERT below can still fail, and then this security really is
                # left with zero accepted links. That is deliberate, not a gap in
                # the reasoning above, and the fix is not to reorder:
                #
                #   - Retiring buys nothing toward the insert succeeding —
                #     `_guard_uniqueness` keys on (source_type, ref_kind, ref_value),
                #     so freeing the OLD key cannot clear a conflict on the NEW one.
                #   - Keeping a known-stale key is worse than holding none. It
                #     prices this security from a symbol its own catalog row no
                #     longer claims, and it goes on blocking the security that
                #     legitimately holds that key from ever binding it.
                #   - Unpriced is visible and recoverable; mispriced is neither. The
                #     caller reports `feed_key_bound_elsewhere`, the held-but-
                #     unpriced check surfaces it, and the next pull re-derives.
                #   - Insert-then-retire also widens the crash window from "no
                #     accepted link, rebuilt by the next pull" to "two accepted
                #     links for one ref_kind, permanent" — and prep resolves both,
                #     so the security would carry another company's closes as its
                #     own. Pinned by
                #     test_a_stale_binding_is_retired_even_when_its_replacement_collides.
                self._retire_stale_binding(security.security_id, ref_kind, source_type)
            try:
                self._links.insert(
                    security_id=security.security_id,
                    ref_kind=ref_kind,
                    ref_value=derivation.ref_value,
                    source_type=source_type,
                    decided_by="auto",
                    actor=self._actor,
                )
            except UserError as exc:
                if exc.code != error_codes.MUTATION_CONSTRAINT_VIOLATION:
                    raise
                # Another security already holds this exact provider ref. Neither
                # app.securities.ticker nor .coingecko_id is unique, so two
                # catalog rows for one instrument — the same coin at two brokers —
                # is a legitimate state a user can reach through the documented
                # surface. That makes it this security's outcome, not a reason to
                # abandon everyone else's refresh.
                return _Derivation(
                    ref_value=None, unpriced_reason="feed_key_bound_elsewhere"
                )
        return derivation

    def _coingecko_key(self, security: HeldSecurity) -> _Derivation:
        """``app.securities.coingecko_id`` IS a CoinGecko slug by definition.

        Its column comment says so, so the value is exact by construction rather
        than inferred — there is nothing uncertain to surface a confirm for.
        """
        if security.coingecko_id is None:
            return _Derivation(ref_value=None, unpriced_reason="no_coingecko_id")
        return _Derivation(ref_value=security.coingecko_id)

    def _tiingo_key(
        self, security: HeldSecurity, adapter: _PriceAdapter
    ) -> _Derivation:
        """Derive a Tiingo ticker, binding only on a near-certain signal.

        A ticker is not an identifier: the same symbol names different securities
        across exchanges (BHP on NYSE and ASX), share classes collide (GOOG /
        GOOGL), and symbols are recycled after a delisting. So three things must
        hold before this binds silently — the symbol names one catalog entry, the
        provider knows it, and the provider agrees about what it is.
        """
        if security.ticker is None:
            return _Derivation(ref_value=None, unpriced_reason="no_ticker")
        if self._ticker_is_shared(security.ticker):
            return _Derivation(
                ref_value=None, review_reason="ticker_not_unique_in_catalog"
            )
        meta = self._metadata(adapter, security.ticker)
        if meta is None:
            # Not an ambiguity — no Tiingo series covers this security. Nothing to
            # propose, so no queue row; the held-but-unpriced check surfaces it.
            return _Derivation(ref_value=None, unpriced_reason="no_provider_coverage")
        # From here every outcome carries the provider's own answer: it is the
        # evidence a reviewer needs, and it is the only place it exists.
        said = {"provider_ticker": security.ticker, "provider_name": meta.name}
        if _exchanges_contradict(security.exchange, meta.exchange_code):
            return _Derivation(
                ref_value=None, review_reason="exchange_contradiction", **said
            )
        if not _names_agree(security.name, meta.name):
            return _Derivation(ref_value=None, review_reason="name_divergence", **said)
        return _Derivation(ref_value=security.ticker, **said)

    def _metadata(self, adapter: _PriceAdapter, ticker: str) -> TickerMetadata | None:
        fetch_metadata = getattr(adapter, "fetch_metadata", None)
        if fetch_metadata is None:  # pragma: no cover — Tiingo always has it
            return None
        return fetch_metadata(ticker)

    def _ticker_is_shared(self, ticker: str) -> bool:
        """Whether more than one catalog entry carries this ticker.

        ``app.securities.ticker`` is commented "nullable, not unique (tickers get
        reused)", so this is a real condition rather than a defensive one.
        """
        row = self._db.execute(
            f"SELECT COUNT(*) FROM {SECURITIES.full_name} "  # noqa: S608  # TableRef constant
            "WHERE UPPER(TRIM(ticker)) = ?",
            [ticker.strip().upper()],
        ).fetchone()
        return row is not None and int(row[0]) > 1

    def _bound_ref(
        self, security_id: str, ref_kind: str, source_type: str
    ) -> _Binding | None:
        """The accepted binding for this security, or ``None``.

        The reverse of ``SecurityLinksRepo.lookup``, which answers ref → security.
        ``decided_by`` travels with the value because staleness depends on it: a
        caller that cannot see who decided cannot tell a moved catalog value from
        a deliberate override. Ordered so a user's decision outranks an auto one,
        and by ``ref_value`` after that, so the answer never depends on storage
        order if both ever exist.
        """
        try:
            row = self._db.execute(
                f"SELECT ref_value, decided_by FROM {SECURITY_LINKS.full_name} "  # noqa: S608  # TableRef constant
                "WHERE status = 'accepted' AND security_id = ? AND ref_kind = ? "
                "AND source_type = ? "
                "ORDER BY decided_by = 'auto', ref_value LIMIT 1",
                [security_id, ref_kind, source_type],
            ).fetchone()
        except duckdb.CatalogException:
            return None
        return _Binding(str(row[0]), str(row[1])) if row is not None else None

    def _catalog_ref(self, security: HeldSecurity, source_type: str) -> str | None:
        """The catalog value a feed key for this source is derived from."""
        return (
            security.coingecko_id
            if source_type == COINGECKO_SOURCE_TYPE
            else security.ticker
        )

    def _binding_is_stale(
        self, security: HeldSecurity, source_type: str, bound: _Binding
    ) -> bool:
        """Whether an auto-derived binding no longer matches the catalog it came from.

        Only ``auto`` bindings are checked. A user-confirmed binding deliberately
        may differ from the ticker — provider symbol formats diverge from ours
        (``BRK.B`` against ``BRK-B``), and overriding that would re-ask a question
        the user already answered. It would also strand the pull: retirement can
        only reverse an ``auto`` row, so calling a user row stale retires nothing
        and then re-derives, leaving two accepted rows for one ref_kind.
        """
        if bound.decided_by != "auto":
            return False
        catalog = self._catalog_ref(security, source_type)
        if catalog is None:
            # Nothing to compare against. The binding is the only key there is.
            return False
        return catalog.strip().upper() != bound.ref_value.strip().upper()

    def _retire_stale_binding(
        self, security_id: str, ref_kind: str, source_type: str
    ) -> None:
        """Reverse the accepted auto-binding whose catalog value moved."""
        row = self._db.execute(
            f"SELECT link_id FROM {SECURITY_LINKS.full_name} "  # noqa: S608  # TableRef constant
            "WHERE status = 'accepted' AND security_id = ? AND ref_kind = ? "
            "AND source_type = ? AND decided_by = 'auto' LIMIT 1",
            [security_id, ref_kind, source_type],
        ).fetchone()
        if row is None:  # pragma: no cover — guarded by _binding_is_stale
            return
        self._links.reverse(
            link_id=str(row[0]), reversed_by=_AUTO_REVERSAL, actor=self._actor
        )

    def _was_reversed_by_user(
        self, security_id: str, ref_kind: str, source_type: str, ref_value: str
    ) -> bool:
        """Whether the user undid this exact binding.

        Scoped to the identical ``ref_value``: a derivation that now produces a
        *different* key is new information and may still bind.

        Two mechanisms, because undoing a binding leaves two different traces.
        ``system audit undo`` — the path a user has today — DELETEs the row, so
        nothing survives in ``app.security_links`` and the audit log is the only
        record. ``SecurityLinksRepo.reverse`` leaves a ``reversed`` row instead;
        no surface calls it on a feed key yet, so that arm is checked for the
        surfaces that will. Reversals this service made itself record ``auto``
        and are excluded — retiring a binding whose catalog value moved is
        bookkeeping, not a judgement about the pairing.
        """
        try:
            reversed_row = self._db.execute(
                f"SELECT 1 FROM {SECURITY_LINKS.full_name} "  # noqa: S608  # TableRef constant
                "WHERE status = 'reversed' AND security_id = ? AND ref_kind = ? "
                "AND source_type = ? AND ref_value = ? "
                "AND reversed_by IS DISTINCT FROM ? LIMIT 1",
                [security_id, ref_kind, source_type, ref_value, _AUTO_REVERSAL],
            ).fetchone()
            if reversed_row is not None:
                return True
            undone = self._db.execute(
                f"SELECT 1 FROM {AUDIT_LOG.full_name} "  # noqa: S608  # TableRef constant
                "WHERE is_undo AND action = ? "
                "AND json_extract_string(before_value, '$.security_id') = ? "
                "AND json_extract_string(before_value, '$.ref_kind') = ? "
                "AND json_extract_string(before_value, '$.source_type') = ? "
                "AND json_extract_string(before_value, '$.ref_value') = ? LIMIT 1",
                [
                    _LINK_INSERT_UNDO,
                    security_id,
                    ref_kind,
                    source_type,
                    ref_value,
                ],
            ).fetchone()
        except duckdb.CatalogException:
            return False
        return undone is not None

    def _review_settled(
        self, ref_kind: str, source_type: str, security: HeldSecurity
    ) -> str | None:
        """Why a fresh proposal for this pairing is blocked, or ``None``.

        Two states block it, for the same reason: without them a second pull
        files a duplicate row for a question already asked, and a queue that
        grows on every sync trains people to accept without reading — the
        failure the spec's near-empty-queue rule exists to prevent.

        ``rejected`` counts, matching ``SecurityLinkDecisionsRepo.list_rejected``
        — "the never-re-propose set" the Plaid path already honours. A *reversed*
        rejection does not count, because reversing a decision is what re-opens
        the proposal.
        """
        ref_value = self._catalog_ref(security, source_type)
        if ref_value is None:
            return None
        try:
            row = self._db.execute(
                f"SELECT status FROM {SECURITY_LINK_DECISIONS.full_name} "  # noqa: S608  # TableRef constant
                "WHERE ref_kind = ? AND ref_value = ? AND source_type = ? "
                "AND candidate_security_id = ? AND (status = 'pending' OR "
                "(status = 'rejected' AND reversed_at IS NULL)) LIMIT 1",
                [ref_kind, ref_value, source_type, security.security_id],
            ).fetchone()
        except duckdb.CatalogException:
            return None
        if row is None:
            return None
        return "queued_for_review" if str(row[0]) == "pending" else "feed_key_rejected"

    def _queue_review(
        self, security: HeldSecurity, source_type: str, derivation: _Derivation
    ) -> bool:
        """File one pending decision for an ambiguous derivation. ``True`` if filed."""
        ref_kind = (
            COINGECKO_REF_KIND
            if source_type == COINGECKO_SOURCE_TYPE
            else TIINGO_REF_KIND
        )
        ref_value = (
            security.coingecko_id
            if source_type == COINGECKO_SOURCE_TYPE
            else security.ticker
        )
        if ref_value is None:  # pragma: no cover — a review always has a ref
            return False
        self._decisions.insert(
            ref_kind=ref_kind,
            ref_value=ref_value,
            source_type=source_type,
            candidate_security_id=security.security_id,
            # The provider's own answer, never the catalog's. These columns are
            # documented as the provider's values and SecurityResolver fills them
            # that way; echoing the catalog back would show the reviewer two
            # identical names and hide the very divergence under review.
            provider_ticker=derivation.provider_ticker,
            provider_name=derivation.provider_name,
            match_reason=derivation.review_reason,
            match_signals={
                "signal": derivation.review_reason,
                "catalog_exchange": security.exchange,
            },
            decided_by="auto",
            actor=self._actor,
        )
        return True

    # ------------------------------- write -------------------------------

    def _store(self, observations: Sequence[PriceObservation]) -> int:
        """Append observations to ``raw.security_prices``, returning rows written.

        ``on_conflict="ignore"`` keeps the observation already stored for a key:
        the table is append-only because a historical close is an immutable fact.
        The count is rows the insert ACTUALLY wrote, never rows offered — a
        provider re-reports the same close until its date advances, so counting
        the batch would make this climb steadily through a fully stalled feed.
        """
        if not observations:
            return 0
        by_source: dict[str, list[PriceObservation]] = {}
        for obs in observations:
            by_source.setdefault(obs.source_type, []).append(obs)
        # Written one source at a time so PRICE_ROWS_WRITTEN_TOTAL can carry a
        # truthful per-source count. ingest_dataframe returns one total for the
        # whole frame, so a mixed batch could only attribute it by guessing — and
        # on a re-pull, where most rows are duplicates, that guess is wrong in the
        # exact case the counter exists to expose.
        total = 0
        for source_type, rows in by_source.items():
            frame = pl.DataFrame(
                [
                    {
                        "provider_security_key": obs.provider_security_key,
                        "price_date": obs.price_date,
                        "quote_currency": obs.quote_currency.upper(),
                        "source_type": obs.source_type,
                        # Single-tenant feeds have no connection that produced
                        # them; '' is what raw.security_prices expects (NOT NULL).
                        "source_origin": "",
                        "close": obs.close,
                        "price_basis": obs.price_basis,
                    }
                    for obs in rows
                ],
                schema=_RAW_PRICE_SCHEMA,
            )
            written = self._db.ingest_dataframe(
                SECURITY_PRICES.full_name, frame, on_conflict="ignore"
            )
            PRICE_ROWS_WRITTEN_TOTAL.labels(source_type=source_type).inc(written)
            total += written
        return total


def _clean(value: object) -> str | None:
    """Trim a catalog string; empty or whitespace reads as absent."""
    if not isinstance(value, str):
        return None
    return value.strip() or None


def _exchanges_contradict(catalog: str | None, provider: str | None) -> bool:
    """Whether two exchange labels name demonstrably different venues.

    Either side absent means the comparison cannot discriminate, so it does not
    contradict — an absent signal never votes. This is a deliberately weak test:
    it catches ASX-vs-NYSE, and lets NASDAQ-vs-NASDAQ-GS through, because
    treating every label difference as a contradiction would queue a review for
    every security whose exchange string is spelled differently.
    """
    if catalog is None or provider is None:
        return False
    left, right = catalog.strip().upper(), provider.strip().upper()
    if not left or not right:
        return False
    return not (left.startswith(right) or right.startswith(left))


def _names_agree(catalog: str, provider: str) -> bool:
    """Whether two names plausibly denote the same issuer.

    Corporate-form suffixes are dropped first: "Apple Inc." and "Apple Inc" are
    one issuer, and asking a user to confirm that difference is the queue noise
    the spec forbids. What survives is compared at SecurityResolver's cutoff, so
    "do these name the same thing?" has one answer across the codebase.

    A name can reduce to nothing — every word of "The Trust" or "Class A Fund" is
    a suffix. That is an absence of evidence, not agreement: reading it as
    agreement turns this check, one of the three signals gating a silent bind,
    into an unconditional pass and leaves the exchange test authorizing the
    binding alone. So an empty side falls back to the literal names, which still
    agree when they are the same string, and otherwise refuses.

    A differing share class is decided before the fuzzy test, which cannot see it
    — see ``_discriminators_differ``.
    """
    if _normalized_name(catalog) == _normalized_name(provider):
        return True
    left, right = _name_tokens(catalog), _name_tokens(provider)
    if not left or not right:
        return False
    if left == right:
        return True
    if _discriminators_differ(left, right):
        return False
    ratio = difflib.SequenceMatcher(None, " ".join(left), " ".join(right)).ratio()
    return ratio >= _NAME_AGREEMENT_CUTOFF


def _discriminators_differ(left: list[str], right: list[str]) -> bool:
    """Whether the two token lists disagree on any single-character token.

    Share class is what a single character carries here: "class" is itself a
    corporate suffix, so "Class A" reduces to a bare "a". One character inside a
    twenty-character string moves SequenceMatcher's ratio by about 0.05 — well
    inside the cutoff — so without this the fuzzy test reads two share classes of
    one issuer as one security. They are not: Berkshire's A and B differ by three
    orders of magnitude, so binding across them prices the holding wrongly and
    keeps doing so, silently, on every later refresh.

    Length one rather than two deliberately. "cl" abbreviates the marker word
    itself, and refusing on a two-character difference would split "CL B" from
    "Class B" — the same class written two ways — and put ordinary securities in
    the review queue forever.

    Length alone therefore misses the numbered classes: "Class A1" against
    "Class A2", "Class R5" against "Class R6". Those are as distinct as A and B
    and differ by one character in a long string, so the fuzzy test clears them
    just as easily. _CLASS_LABEL admits them by shape instead of by length — a
    letter followed by digits is never a word, so it cannot catch "cl" the way
    widening the length test would. Alphabetic multi-character classes ("Inv"
    against "Ins", "I" against "II") remain outside both tests.
    """
    return any(
        len(token) == 1 or _CLASS_LABEL.match(token) is not None
        for token in set(left) ^ set(right)
    )


def _normalized_name(name: str) -> str:
    """The name reduced to its alphanumeric words, for a literal-equality test."""
    return " ".join(re.findall(r"[a-z0-9]+", name.lower()))


def _name_tokens(name: str) -> list[str]:
    """Significant lowercase tokens, corporate-form suffixes removed."""
    words = re.findall(r"[a-z0-9]+", name.lower())
    return [w for w in words if w not in _CORPORATE_SUFFIXES]


def build_price_service(db: Database, *, actor: str = "system") -> PriceService:
    """Wire a PriceService with the real adapters and the OS keychain.

    Kept out of ``PriceService.__init__`` so tests inject fakes without a
    production seam, matching ``connectors/gsheet/service_factory.py``.
    """
    from moneybin.connectors.prices.coingecko import (  # noqa: PLC0415  # httpx is not cold-start cheap
        CoinGeckoPriceAdapter,
    )
    from moneybin.connectors.prices.tiingo import TiingoPriceAdapter  # noqa: PLC0415
    from moneybin.secrets import (
        SecretStore,  # noqa: PLC0415  # keyring import is deferred too
    )

    # One clock for the whole pull. The service derives its complete-day cutoff
    # from this date and CoinGecko measures its `days` span against it, so a pull
    # that happens to straddle UTC midnight cannot end up asking for a span that
    # disagrees with the window it filters the response against.
    today = datetime.now(UTC).date()
    return PriceService(
        db,
        tiingo=TiingoPriceAdapter(secrets=SecretStore()),
        coingecko=CoinGeckoPriceAdapter(today=today),
        actor=actor,
        today=today,
    )
