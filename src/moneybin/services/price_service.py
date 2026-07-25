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
from datetime import date, timedelta
from typing import TYPE_CHECKING, Protocol

import duckdb
import polars as pl

from moneybin.connectors.prices.protocol import (
    PriceFetchResult,
    PriceObservation,
    SecurityRef,
)
from moneybin.metrics.registry import PRICE_ROWS_WRITTEN_TOTAL
from moneybin.repositories.security_link_decisions_repo import (
    SecurityLinkDecisionsRepo,
)
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.tables import (
    DIM_HOLDINGS,
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

_RAW_PRICE_SCHEMA = {
    "provider_security_key": pl.Utf8,
    "price_date": pl.Date,
    "quote_currency": pl.Utf8,
    "source_type": pl.Utf8,
    "source_origin": pl.Utf8,
    "close": pl.Decimal(28, 10),
    "price_basis": pl.Utf8,
}


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
class UnpricedSecurity:
    """A held security no observation was stored for, and why."""

    security_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class PullResult:
    """What one refresh did. Partial success is the normal outcome."""

    rows_written: int
    observations: int
    securities_priced: int
    queued_for_review: int
    unpriced: tuple[UnpricedSecurity, ...]


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
    """The outcome of deriving one feed key."""

    ref_value: str | None
    review_reason: str | None = None
    unpriced_reason: str | None = None


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
        """Initialize with injected adapters; ``today`` is a test seam only."""
        self._db = db
        self._tiingo = tiingo
        self._coingecko = coingecko
        self._actor = actor
        self._today = today or date.today()
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
        start = since or self._today - timedelta(days=_DEFAULT_LOOKBACK_DAYS)
        held = self.held_securities(security_ids)
        unpriced: list[UnpricedSecurity] = []
        queued = 0
        by_source: dict[str, list[tuple[HeldSecurity, str]]] = {}

        for security in held:
            source_type, adapter = self._route(security)
            if adapter is None:
                unpriced.append(
                    UnpricedSecurity(security.security_id, "no_price_source")
                )
                continue
            derivation = self._feed_key(security, source_type, adapter)
            if derivation.ref_value is None:
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
            result = adapter.fetch(refs, start, self._today)
            observations.extend(result.observations)
            for obs in result.observations:
                priced_keys.add((source_type, obs.provider_security_key))
            failed = {f.provider_security_key: f.reason for f in result.failures}
            for sec, key in entries:
                if key in failed:
                    unpriced.append(UnpricedSecurity(sec.security_id, failed[key]))

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
        if bound is not None:
            return _Derivation(ref_value=bound)

        if self._review_pending(ref_kind, source_type, security):
            return _Derivation(ref_value=None, unpriced_reason="queued_for_review")

        if source_type == COINGECKO_SOURCE_TYPE:
            derivation = self._coingecko_key(security)
        else:
            derivation = self._tiingo_key(security, adapter)

        if derivation.ref_value is not None:
            self._links.insert(
                security_id=security.security_id,
                ref_kind=ref_kind,
                ref_value=derivation.ref_value,
                source_type=source_type,
                decided_by="auto",
                actor=self._actor,
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
        if _exchanges_contradict(security.exchange, meta.exchange_code):
            return _Derivation(ref_value=None, review_reason="exchange_contradiction")
        if not _names_agree(security.name, meta.name):
            return _Derivation(ref_value=None, review_reason="name_divergence")
        return _Derivation(ref_value=security.ticker)

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
    ) -> str | None:
        """The accepted ref bound to this security, or ``None``.

        The reverse of ``SecurityLinksRepo.lookup``, which answers ref → security.
        """
        try:
            row = self._db.execute(
                f"SELECT ref_value FROM {SECURITY_LINKS.full_name} "  # noqa: S608  # TableRef constant
                "WHERE status = 'accepted' AND security_id = ? AND ref_kind = ? "
                "AND source_type = ? LIMIT 1",
                [security_id, ref_kind, source_type],
            ).fetchone()
        except duckdb.CatalogException:
            return None
        return str(row[0]) if row is not None else None

    def _review_pending(
        self, ref_kind: str, source_type: str, security: HeldSecurity
    ) -> bool:
        """Whether this security already has a pending decision awaiting a human.

        Without this a second pull files a duplicate row for the same question,
        and a queue that grows on every sync trains people to accept without
        reading — the failure the spec's near-empty-queue rule exists to prevent.
        """
        if security.ticker is None and security.coingecko_id is None:
            return False
        ref_value = (
            security.coingecko_id
            if source_type == COINGECKO_SOURCE_TYPE
            else security.ticker
        )
        if ref_value is None:
            return False
        try:
            row = self._db.execute(
                f"SELECT COUNT(*) FROM {SECURITY_LINK_DECISIONS.full_name} "  # noqa: S608  # TableRef constant
                "WHERE status = 'pending' AND ref_kind = ? AND ref_value = ? "
                "AND source_type = ? AND candidate_security_id = ?",
                [ref_kind, ref_value, source_type, security.security_id],
            ).fetchone()
        except duckdb.CatalogException:
            return False
        return row is not None and int(row[0]) > 0

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
            provider_ticker=security.ticker,
            provider_name=security.name,
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
    """
    left, right = _name_tokens(catalog), _name_tokens(provider)
    if not left or not right:
        return True
    if left == right:
        return True
    ratio = difflib.SequenceMatcher(None, " ".join(left), " ".join(right)).ratio()
    return ratio >= _NAME_AGREEMENT_CUTOFF


def _name_tokens(name: str) -> list[str]:
    """Significant lowercase tokens, corporate-form suffixes removed."""
    words = re.findall(r"[a-z0-9]+", name.lower())
    return [w for w in words if w not in _CORPORATE_SUFFIXES]
