"""Tests for PriceService: feed-key derivation and the pull orchestration.

No test here opens a socket. The adapters are replaced by in-memory fakes that
record what they were asked for, so these tests assert MoneyBin's decisions —
which securities get fetched, which keys bind silently, which route to review —
rather than any provider's behaviour, which test_connectors/test_prices covers.
"""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

from moneybin.connectors.prices.protocol import (
    PriceFetchFailure,
    PriceFetchResult,
    PriceObservation,
    SecurityRef,
)
from moneybin.connectors.prices.tiingo import TickerMetadata
from moneybin.database import Database
from moneybin.services.price_service import PriceService
from moneybin.tables import SECURITIES, SECURITY_LINK_DECISIONS, SECURITY_LINKS

if TYPE_CHECKING:
    from collections.abc import Sequence

_TODAY = date(2026, 7, 24)

_HOLDINGS_DDL = """
CREATE OR REPLACE TABLE core.dim_holdings (
    account_id VARCHAR,
    security_id VARCHAR,
    quantity DECIMAL(28, 10),
    currency_code VARCHAR
);
"""


class _FakeTiingo:
    """Records the refs it was asked for; serves a canned close per security."""

    source_type = "tiingo"
    price_basis = "raw"

    def __init__(
        self,
        *,
        metadata: dict[str, TickerMetadata | None] | None = None,
        close: Decimal = Decimal("212.55"),
    ) -> None:
        self.metadata = metadata or {}
        self.close = close
        # Which dates each fetch returns. More than one lets a re-pull produce a
        # batch that is genuinely partial — some rows already stored, some new —
        # which is the only shape that exercises per-source write counting.
        self.dates: list[date] = [_TODAY]
        self.fetched: list[SecurityRef] = []
        self.metadata_calls: list[str] = []
        self.windows: list[tuple[date, date]] = []
        self.fail_keys: set[str] = set()

    def fetch_metadata(self, ticker: str) -> TickerMetadata | None:
        self.metadata_calls.append(ticker)
        return self.metadata.get(ticker)

    def fetch(
        self, securities: Sequence[SecurityRef], start: date, end: date
    ) -> PriceFetchResult:
        self.fetched.extend(securities)
        self.windows.append((start, end))
        observations: list[PriceObservation] = []
        failures: list[PriceFetchFailure] = []
        for ref in securities:
            if ref.provider_security_key in self.fail_keys:
                failures.append(PriceFetchFailure(ref.provider_security_key, "boom"))
                continue
            observations.extend(
                PriceObservation(
                    provider_security_key=ref.provider_security_key,
                    price_date=day,
                    quote_currency=ref.quote_currency,
                    close=self.close,
                    source_type=self.source_type,
                    price_basis=self.price_basis,
                )
                for day in self.dates
            )
        return PriceFetchResult(tuple(observations), tuple(failures))


class _FakeCoinGecko(_FakeTiingo):
    """Same recording fake; CoinGecko has no metadata endpoint to consult."""

    source_type = "coingecko"

    def fetch_metadata(self, ticker: str) -> TickerMetadata | None:  # pragma: no cover
        raise AssertionError("CoinGecko needs no metadata round-trip")


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Database:
    """App schema from init_schemas plus a writable core.dim_holdings."""
    database = Database(
        tmp_path / "prices.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    database.execute("CREATE SCHEMA IF NOT EXISTS core")
    database.execute(_HOLDINGS_DDL)
    return database


def _seed_security(
    db: Database,
    *,
    security_id: str,
    name: str,
    security_type: str = "equity",
    ticker: str | None = None,
    exchange: str | None = None,
    coingecko_id: str | None = None,
    currency_code: str = "USD",
) -> None:
    db.execute(
        f"INSERT INTO {SECURITIES.full_name} "  # noqa: S608  # TableRef constant
        "(security_id, name, security_type, ticker, exchange, coingecko_id, "
        "currency_code) VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            security_id,
            name,
            security_type,
            ticker,
            exchange,
            coingecko_id,
            currency_code,
        ],
    )


def _hold(db: Database, security_id: str, *, quantity: str = "10") -> None:
    db.execute(
        "INSERT INTO core.dim_holdings (account_id, security_id, quantity, "
        "currency_code) VALUES (?, ?, ?, ?)",
        ["acct1", security_id, Decimal(quantity), "USD"],
    )


def _service(
    db: Database, tiingo: _FakeTiingo, coingecko: _FakeCoinGecko | None = None
) -> PriceService:
    return PriceService(
        db,
        tiingo=tiingo,
        coingecko=coingecko or _FakeCoinGecko(),
        today=_TODAY,
    )


def _decisions(db: Database) -> list[tuple[str, str, str]]:
    rows = db.execute(
        f"SELECT ref_kind, ref_value, match_reason "  # noqa: S608  # TableRef constant
        f"FROM {SECURITY_LINK_DECISIONS.full_name} WHERE status = 'pending' "
        "ORDER BY ref_value"
    ).fetchall()
    return [(str(r[0]), str(r[1]), str(r[2])) for r in rows]


def _rows_written(source_type: str) -> float:
    """Public read of the write counter — no private attribute access.

    Matches tests/moneybin/test_metrics/test_instruments.py; the alternative
    ``._value.get()`` needs a pyright suppression for protected access.
    """
    return (
        REGISTRY.get_sample_value(
            "moneybin_price_rows_written_total", {"source_type": source_type}
        )
        or 0.0
    )


def _links(db: Database) -> list[tuple[str, str, str]]:
    rows = db.execute(
        f"SELECT ref_kind, ref_value, security_id "  # noqa: S608  # TableRef constant
        f"FROM {SECURITY_LINKS.full_name} WHERE status = 'accepted' ORDER BY ref_value"
    ).fetchall()
    return [(str(r[0]), str(r[1]), str(r[2])) for r in rows]


# --------------------------------------------------------------------------
# Which securities get fetched at all
# --------------------------------------------------------------------------


def test_only_held_securities_are_fetched(db: Database) -> None:
    """Fetching the whole catalog burns rate limit and stores rows no report reads.

    Both tickers resolve at the provider, so the ONLY thing keeping the unheld one
    out is the open-position filter. Omitting ENE's metadata would exclude it for
    lack of coverage instead, and the test would pass with that filter deleted.
    """
    _seed_security(db, security_id="s_held", name="Apple Inc", ticker="AAPL")
    _seed_security(db, security_id="s_gone", name="Enron Corp", ticker="ENE")
    _hold(db, "s_held")
    tiingo = _FakeTiingo(
        metadata={
            "AAPL": TickerMetadata("Apple Inc", None),
            "ENE": TickerMetadata("Enron Corp", None),
        }
    )

    _service(db, tiingo).pull()

    assert [ref.provider_security_key for ref in tiingo.fetched] == ["AAPL"]


def test_a_fully_closed_position_is_not_fetched(db: Database) -> None:
    """A zero-quantity row is a closed position; its price answers no question."""
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _hold(db, "s1", quantity="0")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})

    _service(db, tiingo).pull()

    assert not tiingo.fetched


def test_a_short_position_is_still_fetched(db: Database) -> None:
    """A negative quantity is an open position and needs a market value."""
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _hold(db, "s1", quantity="-5")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})

    _service(db, tiingo).pull()

    assert [ref.provider_security_key for ref in tiingo.fetched] == ["AAPL"]


# --------------------------------------------------------------------------
# Feed-key derivation — what binds silently vs what routes to review
# --------------------------------------------------------------------------


def test_an_accepted_binding_is_reused_without_any_provider_call(db: Database) -> None:
    """Rung 1: never re-ask. A confirmed binding costs no metadata round-trip."""
    _seed_security(db, security_id="s1", name="Totally Different", ticker="AAPL")
    _hold(db, "s1")
    db.execute(
        f"INSERT INTO {SECURITY_LINKS.full_name} "  # noqa: S608  # TableRef constant
        "(link_id, security_id, ref_kind, ref_value, source_type, status, "
        "decided_by, decided_at) VALUES (?, ?, ?, ?, ?, 'accepted', 'user', "
        "CURRENT_TIMESTAMP)",
        ["l1", "s1", "tiingo_ticker", "AAPL", "tiingo"],
    )
    tiingo = _FakeTiingo()

    _service(db, tiingo).pull()

    assert [ref.provider_security_key for ref in tiingo.fetched] == ["AAPL"]
    assert tiingo.metadata_calls == []


def test_a_coingecko_slug_binds_without_a_metadata_round_trip(db: Database) -> None:
    """app.securities.coingecko_id IS a CoinGecko slug by definition.

    Nothing is inferred, so there is nothing to verify and nothing to confirm.
    """
    _seed_security(
        db,
        security_id="s1",
        name="Bitcoin",
        security_type="crypto",
        coingecko_id="bitcoin",
    )
    _hold(db, "s1")
    coingecko = _FakeCoinGecko()

    _service(db, _FakeTiingo(), coingecko).pull()

    assert [ref.provider_security_key for ref in coingecko.fetched] == ["bitcoin"]
    assert _links(db) == [("coingecko_slug", "bitcoin", "s1")]


def test_an_exact_ticker_with_agreeing_name_binds_silently(db: Database) -> None:
    """The certain case: one catalog candidate and the provider agrees on identity."""
    _seed_security(
        db, security_id="s1", name="Apple Inc", ticker="AAPL", exchange="NASDAQ"
    )
    _hold(db, "s1")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", "NASDAQ")})

    _service(db, tiingo).pull()

    assert _links(db) == [("tiingo_ticker", "AAPL", "s1")]
    assert _decisions(db) == []


def test_a_ticker_shared_by_two_catalog_entries_routes_to_review(db: Database) -> None:
    """Tickers get reused; a non-unique symbol cannot name one instrument."""
    _seed_security(db, security_id="s_nyse", name="BHP Group Ltd", ticker="BHP")
    _seed_security(db, security_id="s_asx", name="BHP Group Limited", ticker="BHP")
    _hold(db, "s_nyse")
    _hold(db, "s_asx")
    tiingo = _FakeTiingo(metadata={"BHP": TickerMetadata("BHP Group Ltd", "NYSE")})

    result = _service(db, tiingo).pull()

    assert _links(db) == []
    assert not tiingo.fetched
    assert [reason for _, _, reason in _decisions(db)] == [
        "ticker_not_unique_in_catalog",
        "ticker_not_unique_in_catalog",
    ]
    assert result.queued_for_review == 2


def test_an_exchange_contradiction_routes_to_review(db: Database) -> None:
    """Our catalog says ASX, Tiingo says NYSE — that is a different listing."""
    _seed_security(
        db, security_id="s1", name="BHP Group Ltd", ticker="BHP", exchange="ASX"
    )
    _hold(db, "s1")
    tiingo = _FakeTiingo(metadata={"BHP": TickerMetadata("BHP Group Ltd", "NYSE")})

    _service(db, tiingo).pull()

    assert _links(db) == []
    assert [reason for _, _, reason in _decisions(db)] == ["exchange_contradiction"]


def test_a_diverging_provider_name_routes_to_review(db: Database) -> None:
    """A recycled symbol reads as a match until you compare what it names."""
    _seed_security(db, security_id="s1", name="Ubiquiti Inc", ticker="UI")
    _hold(db, "s1")
    tiingo = _FakeTiingo(metadata={"UI": TickerMetadata("Unibail Rodamco SE", None)})

    _service(db, tiingo).pull()

    assert _links(db) == []
    assert [reason for _, _, reason in _decisions(db)] == ["name_divergence"]


def test_a_missing_corporate_suffix_is_not_a_divergence(db: Database) -> None:
    """A catalog "Apple" and a provider "Apple Inc" are the same issuer.

    The queue must stay near-empty in normal operation — a review entry per held
    position trains people to accept without reading.

    The fixture drops the suffix entirely rather than only its punctuation:
    "Apple Inc." vs "Apple Inc" is already equal once the token regex strips
    punctuation, so it would pass with the suffix list deleted and prove nothing
    about it.
    """
    _seed_security(db, security_id="s1", name="Apple", ticker="AAPL")
    _hold(db, "s1")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})

    _service(db, tiingo).pull()

    assert _links(db) == [("tiingo_ticker", "AAPL", "s1")]
    assert _decisions(db) == []


def test_a_security_with_no_ticker_queues_nothing(db: Database) -> None:
    """No match at all means nothing to propose, so no queue row."""
    _seed_security(db, security_id="s1", name="Private Placement A")
    _hold(db, "s1")
    tiingo = _FakeTiingo()

    result = _service(db, tiingo).pull()

    assert _decisions(db) == []
    assert [u.security_id for u in result.unpriced] == ["s1"]


def test_a_ticker_tiingo_does_not_know_queues_nothing(db: Database) -> None:
    """An unknown symbol is a coverage gap, not an ambiguity to adjudicate."""
    _seed_security(db, security_id="s1", name="Delisted Co", ticker="ZZZZ")
    _hold(db, "s1")
    tiingo = _FakeTiingo(metadata={"ZZZZ": None})

    result = _service(db, tiingo).pull()

    assert _decisions(db) == []
    assert _links(db) == []
    assert [u.security_id for u in result.unpriced] == ["s1"]


def test_a_pending_review_is_not_re_queued_on_the_next_pull(db: Database) -> None:
    """A second pull must not file a duplicate decision for the same ref."""
    _seed_security(db, security_id="s1", name="BHP Group Ltd", ticker="BHP")
    _seed_security(db, security_id="s2", name="BHP Group Limited", ticker="BHP")
    _hold(db, "s1")
    _hold(db, "s2")
    tiingo = _FakeTiingo(metadata={"BHP": TickerMetadata("BHP Group Ltd", "NYSE")})
    service = _service(db, tiingo)

    service.pull()
    first = len(_decisions(db))
    service.pull()

    assert len(_decisions(db)) == first


# --------------------------------------------------------------------------
# The pull itself
# --------------------------------------------------------------------------


def test_a_pull_writes_the_observations_to_raw(db: Database) -> None:
    """The provider key is stored, not the canonical id — staging resolves it."""
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _hold(db, "s1")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})

    result = _service(db, tiingo).pull()

    row = db.execute(
        "SELECT provider_security_key, price_date, close, source_type, price_basis "
        "FROM raw.security_prices"
    ).fetchone()
    assert row == ("AAPL", _TODAY, Decimal("212.55"), "tiingo", "raw")
    assert result.rows_written == 1


def test_a_repeated_pull_writes_no_new_rows(db: Database) -> None:
    """raw.security_prices is append-only; a re-reported close keeps the first row.

    rows_written must count rows the insert ACTUALLY wrote, so a flat counter
    exposes a stalled feed instead of climbing through one.
    """
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _hold(db, "s1")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})
    service = _service(db, tiingo)

    service.pull()
    second = service.pull()

    assert second.rows_written == 0
    count = db.execute("SELECT COUNT(*) FROM raw.security_prices").fetchone()
    assert count is not None
    assert count[0] == 1


def test_one_failed_security_does_not_lose_the_others(db: Database) -> None:
    """Partial success is the normal outcome of a refresh over many securities."""
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _seed_security(db, security_id="s2", name="Microsoft Corp", ticker="MSFT")
    _hold(db, "s1")
    _hold(db, "s2")
    tiingo = _FakeTiingo(
        metadata={
            "AAPL": TickerMetadata("Apple Inc", None),
            "MSFT": TickerMetadata("Microsoft Corp", None),
        }
    )
    tiingo.fail_keys = {"MSFT"}

    result = _service(db, tiingo).pull()

    assert result.rows_written == 1
    assert [u.security_id for u in result.unpriced] == ["s2"]


def test_since_bounds_the_requested_window(db: Database) -> None:
    """A deeper backfill is the user's explicit ask, not a silent default."""
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _hold(db, "s1")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})

    _service(db, tiingo).pull(since=date(2026, 1, 1))

    assert tiingo.windows == [(date(2026, 1, 1), _TODAY)]


def test_the_default_window_ends_today_and_never_later(db: Database) -> None:
    """A future price_date would let today's value come from tomorrow's close."""
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _hold(db, "s1")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})

    _service(db, tiingo).pull()

    start, end = tiingo.windows[0]
    assert end == _TODAY
    assert start < end


def test_restricting_to_named_securities_skips_the_rest(db: Database) -> None:
    """`--securities` is how a user refreshes one position without a full sweep."""
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _seed_security(db, security_id="s2", name="Microsoft Corp", ticker="MSFT")
    _hold(db, "s1")
    _hold(db, "s2")
    tiingo = _FakeTiingo(
        metadata={
            "AAPL": TickerMetadata("Apple Inc", None),
            "MSFT": TickerMetadata("Microsoft Corp", None),
        }
    )

    _service(db, tiingo).pull(security_ids=["s2"])

    assert [ref.provider_security_key for ref in tiingo.fetched] == ["MSFT"]


def test_the_written_counter_stays_truthful_on_a_partial_write(db: Database) -> None:
    """A batch where only some rows are new must count exactly the new ones.

    PRICE_ROWS_WRITTEN_TOTAL exists so a flat counter exposes a stalled feed, and
    ingest_dataframe returns one total for the whole frame. Attributing that total
    only when EVERY row was new records zero for a source that did write — and a
    re-pull, where yesterday's close is already stored, is exactly that shape.

    The fixture is deliberately mixed: two sources, and the second pull re-offers
    the stored date plus one new date, so `written` is neither 0 nor the batch size.
    """
    _seed_security(db, security_id="s_eq", name="Apple Inc", ticker="AAPL")
    _seed_security(
        db,
        security_id="s_btc",
        name="Bitcoin",
        security_type="crypto",
        coingecko_id="bitcoin",
    )
    _hold(db, "s_eq")
    _hold(db, "s_btc")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})
    coingecko = _FakeCoinGecko()
    service = _service(db, tiingo, coingecko)

    service.pull()
    before = _rows_written("tiingo")
    # Re-offer the stored date plus one earlier date: 1 of 2 rows is new.
    tiingo.dates = [_TODAY - timedelta(days=1), _TODAY]
    second = service.pull()

    assert second.rows_written == 1
    assert _rows_written("tiingo") - before == 1


def test_each_source_is_counted_under_its_own_label(db: Database) -> None:
    """A mixed batch must not attribute one source's rows to another."""
    _seed_security(db, security_id="s_eq", name="Apple Inc", ticker="AAPL")
    _seed_security(
        db,
        security_id="s_btc",
        name="Bitcoin",
        security_type="crypto",
        coingecko_id="bitcoin",
    )
    _hold(db, "s_eq")
    _hold(db, "s_btc")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})
    before = (_rows_written("tiingo"), _rows_written("coingecko"))

    _service(db, tiingo, _FakeCoinGecko()).pull()

    assert _rows_written("tiingo") - before[0] == 1
    assert _rows_written("coingecko") - before[1] == 1


# --------------------------------------------------------------------------
# User price marks
# --------------------------------------------------------------------------


def _core_prices_ddl(db: Database) -> None:
    """core.fct_security_prices is SQLMesh-built in production; stub the shape."""
    db.execute(
        "CREATE OR REPLACE TABLE core.fct_security_prices ("
        "security_id VARCHAR, price_date DATE, quote_currency VARCHAR, "
        "close DECIMAL(28,10), source_type VARCHAR, price_basis VARCHAR, "
        "updated_at TIMESTAMP)"
    )


def test_setting_a_mark_stores_it_with_override_provenance(db: Database) -> None:
    """A mark is the user's own number, and its source must say so."""
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    service.set_mark("s1", date(2026, 6, 30), Decimal("42.50"), note="409A")

    row = db.execute(
        "SELECT security_id, price_date, quote_currency, close, note "
        "FROM app.security_price_overrides"
    ).fetchone()
    assert row == ("s1", date(2026, 6, 30), "USD", Decimal("42.50"), "409A")


def test_setting_a_mark_twice_replaces_the_value(db: Database) -> None:
    """A correction replaces the number; the mark stays one row for its date."""
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    service.set_mark("s1", date(2026, 6, 30), Decimal("42.50"), note="first")
    service.set_mark("s1", date(2026, 6, 30), Decimal("51.00"), note="revised")

    rows = db.execute("SELECT close, note FROM app.security_price_overrides").fetchall()
    assert rows == [(Decimal("51.00"), "revised")]


def test_a_nonpositive_mark_is_refused(db: Database) -> None:
    """The never-zero rule must hold on the user-controlled path too.

    A genuinely worthless position is a ledger event — a disposal or write-off —
    not a zero price, which would make worthless and unknown two states every
    downstream total has to tell apart.
    """
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    with pytest.raises(ValueError, match="positive"):
        service.set_mark("s1", date(2026, 6, 30), Decimal("0"), note=None)

    count = db.execute("SELECT COUNT(*) FROM app.security_price_overrides").fetchone()
    assert count is not None
    assert count[0] == 0


def test_deleting_a_mark_returns_the_date_to_provider_valuation(db: Database) -> None:
    """Without delete a mark is unreachable: it outranks every provider row."""
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())
    service.set_mark("s1", date(2026, 6, 30), Decimal("42.50"), note=None)

    deleted = service.delete_mark("s1", date(2026, 6, 30))

    assert deleted is True
    count = db.execute("SELECT COUNT(*) FROM app.security_price_overrides").fetchone()
    assert count is not None
    assert count[0] == 0


def test_deleting_an_absent_mark_reports_that_nothing_was_removed(
    db: Database,
) -> None:
    """A silent success would read as "the override is gone" when none existed."""
    _seed_security(db, security_id="s1", name="Private Co")

    assert _service(db, _FakeTiingo()).delete_mark("s1", date(2026, 6, 30)) is False


def test_listing_prices_reads_the_resolved_series(db: Database) -> None:
    """The user wants to see which source won, not every raw observation."""
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _core_prices_ddl(db)
    db.execute(
        "INSERT INTO core.fct_security_prices VALUES "
        "('s1', DATE '2026-07-23', 'USD', 212.55, 'tiingo', 'raw', NOW()), "
        "('s1', DATE '2026-07-24', 'USD', 214.00, 'override', 'raw', NOW()), "
        "('s2', DATE '2026-07-24', 'USD', 99.00, 'tiingo', 'raw', NOW())"
    )

    result = _service(db, _FakeTiingo()).list_prices("s1")

    assert [(r.price_date, r.close, r.source_type) for r in result.rows] == [
        (date(2026, 7, 24), Decimal("214.00"), "override"),
        (date(2026, 7, 23), Decimal("212.55"), "tiingo"),
    ]


def test_listing_prices_can_filter_by_source(db: Database) -> None:
    """Comparing what a feed said against what a mark overrode is the use case."""
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _core_prices_ddl(db)
    db.execute(
        "INSERT INTO core.fct_security_prices VALUES "
        "('s1', DATE '2026-07-23', 'USD', 212.55, 'tiingo', 'raw', NOW()), "
        "('s1', DATE '2026-07-24', 'USD', 214.00, 'override', 'raw', NOW())"
    )

    result = _service(db, _FakeTiingo()).list_prices("s1", source_type="override")

    assert [r.source_type for r in result.rows] == ["override"]


def test_listing_prices_can_bound_the_start_date(db: Database) -> None:
    """A long-held position has years of closes; the default must not dump them all."""
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _core_prices_ddl(db)
    db.execute(
        "INSERT INTO core.fct_security_prices VALUES "
        "('s1', DATE '2020-01-02', 'USD', 100.00, 'tiingo', 'raw', NOW()), "
        "('s1', DATE '2026-07-24', 'USD', 214.00, 'tiingo', 'raw', NOW())"
    )

    result = _service(db, _FakeTiingo()).list_prices("s1", since=date(2026, 1, 1))

    assert [r.price_date for r in result.rows] == [date(2026, 7, 24)]
