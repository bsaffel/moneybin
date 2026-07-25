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

from moneybin.connectors.prices.errors import PriceFeedAuthError
from moneybin.connectors.prices.protocol import (
    PriceFetchFailure,
    PriceFetchResult,
    PriceObservation,
    SecurityRef,
)
from moneybin.connectors.prices.tiingo import TickerMetadata
from moneybin.database import Database
from moneybin.repositories.security_link_decisions_repo import (
    SecurityLinkDecisionsRepo,
)
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.price_service import (
    COINGECKO_REF_KIND,
    COINGECKO_SOURCE_TYPE,
    TIINGO_REF_KIND,
    TIINGO_SOURCE_TYPE,
    PriceService,
)
from moneybin.services.undo_service import UndoService
from moneybin.tables import (
    AUDIT_LOG,
    SECURITIES,
    SECURITY_LINK_DECISIONS,
    SECURITY_LINKS,
)
from tests.moneybin.price_model_helpers import ref_kind_mapping

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
        # Per-key override, for the case where one security's quote is the thing
        # under test and the rest of the batch must stay well-behaved.
        self.close_by_key: dict[str, Decimal] = {}
        # Which dates each fetch returns. More than one lets a re-pull produce a
        # batch that is genuinely partial — some rows already stored, some new —
        # which is the only shape that exercises per-source write counting.
        self.dates: list[date] = [_TODAY]
        self.fetched: list[SecurityRef] = []
        self.metadata_calls: list[str] = []
        self.windows: list[tuple[date, date]] = []
        self.fail_keys: set[str] = set()
        # A whole-batch condition — auth, rate limit, unreachable — as opposed to
        # fail_keys, which models one security the provider could not answer for.
        self.raises: Exception | None = None

    def fetch_metadata(self, ticker: str) -> TickerMetadata | None:
        self.metadata_calls.append(ticker)
        return self.metadata.get(ticker)

    def fetch(
        self, securities: Sequence[SecurityRef], start: date, end: date
    ) -> PriceFetchResult:
        self.fetched.extend(securities)
        self.windows.append((start, end))
        if self.raises is not None:
            raise self.raises
        observations: list[PriceObservation] = []
        failures: list[PriceFetchFailure] = []
        for ref in securities:
            if ref.provider_security_key in self.fail_keys:
                failures.append(PriceFetchFailure(ref.provider_security_key, "boom"))
                continue
            close = self.close_by_key.get(ref.provider_security_key, self.close)
            observations.extend(
                PriceObservation(
                    provider_security_key=ref.provider_security_key,
                    price_date=day,
                    quote_currency=ref.quote_currency,
                    close=close,
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


def _scalar(db: Database, sql: str) -> str:
    """The first column of a query that must return exactly one row."""
    row = db.execute(sql).fetchone()
    assert row is not None, f"expected a row from: {sql}"
    return str(row[0])


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


def test_a_name_that_reduces_to_no_tokens_is_not_treated_as_agreement(
    db: Database,
) -> None:
    """No tokens is no evidence, and no evidence is not agreement.

    Every word of "The Trust" is a corporate-form suffix, so the token list is
    empty. Reading that as agreement makes the issuer check — one of the three
    signals required before a silent bind — an unconditional pass, leaving the
    exchange check alone to authorize the binding. Both exchanges are NULL here
    precisely so the exchange check cannot contradict: this fixture isolates the
    name signal, and nothing else can refuse the bind.
    """
    _seed_security(db, security_id="s_trust", name="The Trust", ticker="TRST")
    _hold(db, "s_trust")
    tiingo = _FakeTiingo(
        metadata={"TRST": TickerMetadata("Bharat Heavy Electricals", None)}
    )

    _service(db, tiingo).pull()

    assert _links(db) == []
    assert _decisions(db) == [(TIINGO_REF_KIND, "TRST", "name_divergence")]


def test_a_rejected_feed_key_is_not_re_proposed_on_the_next_pull(
    db: Database,
) -> None:
    """A rejected decision is the never-re-propose set, on this path as on Plaid's.

    Suppressing only `pending` re-files the identical question on every pull, so
    the queue grows without bound and trains the user to accept without reading
    — the failure `_review_pending` exists to prevent.
    """
    _seed_security(db, security_id="s_bhp_a", name="BHP Group", ticker="BHP")
    _seed_security(db, security_id="s_bhp_b", name="BHP Billiton", ticker="BHP")
    _hold(db, "s_bhp_a")
    service = _service(db, _FakeTiingo())
    service.pull()
    decision_id = _scalar(
        db,
        f"SELECT decision_id FROM {SECURITY_LINK_DECISIONS.full_name} "  # noqa: S608  # TableRef constant
        "WHERE status = 'pending'",
    )
    SecurityLinkDecisionsRepo(db).update_status(
        decision_id, status="rejected", decided_by="user", actor="test"
    )

    service.pull()

    assert _decisions(db) == []


def test_an_undone_binding_is_not_silently_recreated(db: Database) -> None:
    """Undoing an auto-binding must stick, or the undo is theatre.

    The user undoes a binding because the position was valued from the wrong
    listing. Re-deriving it on the next pull reaches the same conclusion from the
    same inputs and re-binds silently, so the wrong valuation returns with no
    confirm and no queue row. The undo is the signal the derivation lacks.

    Driven through `system audit undo` because that is the path a user has
    today — and it DELETEs the link row, so the audit log is the only surviving
    evidence the binding ever existed.
    """
    _seed_security(db, security_id="s_soh", name="Sonic Healthcare", ticker="SOH")
    _hold(db, "s_soh")
    tiingo = _FakeTiingo(metadata={"SOH": TickerMetadata("Sonic Healthcare", None)})
    service = _service(db, tiingo)
    service.pull()
    operation_id = _scalar(
        db,
        f"SELECT operation_id FROM {AUDIT_LOG.full_name} "  # noqa: S608  # TableRef constant
        "WHERE action = 'security_link.insert'",
    )
    UndoService(db).undo(operation_id, actor="test")
    assert _links(db) == []

    service.pull()

    assert _links(db) == []
    assert _decisions(db) == [(TIINGO_REF_KIND, "SOH", "binding_was_reversed")]


def test_a_reversed_binding_is_not_silently_recreated(db: Database) -> None:
    """The same refusal through the reversal path, which leaves the row in place.

    Separate from the undo test because the two leave different evidence — a
    `reversed` row here, an audit entry and no row there — so one fixture cannot
    isolate both arms. No surface reverses a feed-key link yet; this holds the
    behaviour for the ones that will.
    """
    _seed_security(db, security_id="s_soh", name="Sonic Healthcare", ticker="SOH")
    _hold(db, "s_soh")
    tiingo = _FakeTiingo(metadata={"SOH": TickerMetadata("Sonic Healthcare", None)})
    service = _service(db, tiingo)
    service.pull()
    link_id = _scalar(
        db,
        f"SELECT link_id FROM {SECURITY_LINKS.full_name} "  # noqa: S608  # TableRef constant
        "WHERE status = 'accepted'",
    )
    SecurityLinksRepo(db).reverse(link_id=link_id, reversed_by="user", actor="test")

    service.pull()

    assert _links(db) == []
    assert _decisions(db) == [(TIINGO_REF_KIND, "SOH", "binding_was_reversed")]


def test_correcting_a_typod_ticker_stops_the_old_symbols_prices(
    db: Database,
) -> None:
    """A bound ref outlives the catalog value it was derived from.

    `SecuritiesRepo.set` writes only `app.securities`; nothing cascades to
    `app.security_links`. So a ticker typo fixed after the first pull leaves the
    position fetching — and valuing from — the wrong company's closes forever,
    reported as `valued`, with no surface revealing the mismatch.
    """
    _seed_security(db, security_id="s_typo", name="AppTech Payments", ticker="AAPL")
    _hold(db, "s_typo")
    tiingo = _FakeTiingo(
        metadata={
            "AAPL": TickerMetadata("AppTech Payments", None),
            "AAPU": TickerMetadata("AppTech Payments", None),
        }
    )
    service = _service(db, tiingo)
    service.pull()
    db.execute(
        f"UPDATE {SECURITIES.full_name} SET ticker = ? WHERE security_id = ?",  # noqa: S608  # TableRef constant
        ["AAPU", "s_typo"],
    )
    tiingo.fetched.clear()

    service.pull()

    assert [ref.provider_security_key for ref in tiingo.fetched] == ["AAPU"]


def test_a_queued_review_records_what_the_provider_said_not_the_catalog(
    db: Database,
) -> None:
    """The reviewer's only evidence is the divergence, so it has to be stored.

    `provider_ticker`/`provider_name` are documented as the provider's values and
    `SecurityResolver` fills them that way. Writing the catalog's own name into
    them shows the reviewer two identical names and no reason to decide either
    way — and makes the column mean the opposite of what it means for every
    Plaid-authored row in the same table.
    """
    _seed_security(db, security_id="s_bhp", name="BHP Group Ltd", ticker="BHP")
    _hold(db, "s_bhp")
    tiingo = _FakeTiingo(
        metadata={"BHP": TickerMetadata("Bharat Heavy Electricals", "NSE")}
    )

    _service(db, tiingo).pull()

    row = db.execute(
        f"SELECT provider_ticker, provider_name FROM "  # noqa: S608  # TableRef constant
        f"{SECURITY_LINK_DECISIONS.full_name} WHERE status = 'pending'"
    ).fetchone()
    assert row is not None
    assert (str(row[0]), str(row[1])) == ("BHP", "Bharat Heavy Electricals")


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


def _refresh_outcomes(source_type: str, outcome: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "moneybin_price_refresh_securities_total",
            {"source_type": source_type, "outcome": outcome},
        )
        or 0.0
    )


def test_a_priced_security_counts_as_written(db: Database) -> None:
    _seed_security(db, security_id="s_eq", name="Apple Inc", ticker="AAPL")
    _hold(db, "s_eq")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})
    before = _refresh_outcomes("tiingo", "written")

    _service(db, tiingo).pull()

    assert _refresh_outcomes("tiingo", "written") - before == 1


def test_a_provider_failure_counts_as_failed_not_skipped(db: Database) -> None:
    """The two outcomes route to different remedies and must not be conflated.

    A failure means the provider was asked and refused — retry, or check the
    credential. A skip means MoneyBin never asked, because no feed key derived.
    """
    _seed_security(db, security_id="s_eq", name="Apple Inc", ticker="AAPL")
    _hold(db, "s_eq")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})
    tiingo.fail_keys = {"AAPL"}
    before = (
        _refresh_outcomes("tiingo", "failed"),
        _refresh_outcomes("tiingo", "skipped"),
    )

    _service(db, tiingo).pull()

    assert _refresh_outcomes("tiingo", "failed") - before[0] == 1
    assert _refresh_outcomes("tiingo", "skipped") - before[1] == 0


def test_a_security_with_no_feed_key_counts_as_skipped(db: Database) -> None:
    """Never asked, so it cannot be a failure — the provider was never reached."""
    _seed_security(db, security_id="s_eq", name="Nameless Holding")
    _hold(db, "s_eq")
    before = (
        _refresh_outcomes("tiingo", "skipped"),
        _refresh_outcomes("tiingo", "failed"),
    )

    _service(db, _FakeTiingo()).pull()

    assert _refresh_outcomes("tiingo", "skipped") - before[0] == 1
    assert _refresh_outcomes("tiingo", "failed") - before[1] == 0


def test_the_refresh_duration_is_timed_per_source(db: Database) -> None:
    """Latency is the signal a provider is degrading before it starts failing.

    Asserts the observation count rather than the elapsed value: the duration of
    a fake fetch is meaningless, but whether the fetch was timed at all is not.
    """
    _seed_security(db, security_id="s_eq", name="Apple Inc", ticker="AAPL")
    _hold(db, "s_eq")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})
    before = (
        REGISTRY.get_sample_value(
            "moneybin_price_refresh_duration_seconds_count", {"source_type": "tiingo"}
        )
        or 0.0
    )

    _service(db, tiingo).pull()

    after = (
        REGISTRY.get_sample_value(
            "moneybin_price_refresh_duration_seconds_count", {"source_type": "tiingo"}
        )
        or 0.0
    )
    assert after - before == 1


def test_a_security_type_with_no_provider_counts_as_skipped(db: Database) -> None:
    """Cash and other carry no market quote, so no provider is ever asked.

    This routes through a different branch than a missing feed key — the adapter
    is None before derivation is even attempted — and reaches its own increment
    site, so it needs its own fixture.
    """
    _seed_security(db, security_id="s_cash", name="Sweep", security_type="cash")
    _hold(db, "s_cash")
    before = (
        _refresh_outcomes("tiingo", "skipped"),
        _refresh_outcomes("tiingo", "written"),
    )

    result = _service(db, _FakeTiingo()).pull()

    assert [u.reason for u in result.unpriced] == ["no_price_source"]
    assert _refresh_outcomes("tiingo", "skipped") - before[0] == 1
    assert _refresh_outcomes("tiingo", "written") - before[1] == 0


def test_a_fetch_returning_nothing_without_an_error_counts_as_skipped(
    db: Database,
) -> None:
    """Asked, answered with neither a price nor a failure — that is not a failure.

    Real providers do this: the Tiingo adapter drops a non-positive close and
    reports no error, so a window containing only such bars comes back empty.
    Counting it as failed would send the reader to check a credential that is
    fine; counting it as written would claim a price that does not exist.
    """
    _seed_security(db, security_id="s_eq", name="Apple Inc", ticker="AAPL")
    _hold(db, "s_eq")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})
    tiingo.dates = []  # fetched, but the window yields no usable bar
    before = (
        _refresh_outcomes("tiingo", "skipped"),
        _refresh_outcomes("tiingo", "failed"),
        _refresh_outcomes("tiingo", "written"),
    )

    _service(db, tiingo).pull()

    assert tiingo.fetched, "the provider must actually have been asked"
    assert _refresh_outcomes("tiingo", "skipped") - before[0] == 1
    assert _refresh_outcomes("tiingo", "failed") - before[1] == 0
    assert _refresh_outcomes("tiingo", "written") - before[2] == 0


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
# Partial failure — one bad security must not discard the whole refresh
#
# Each test here isolates a different way one security's problem escaped as an
# exception and took the entire pull with it. They are separated because the
# containment for each lives at a different point in pull(): the derivation
# loop, the per-source fetch, and the write.
# --------------------------------------------------------------------------


def test_a_credential_failure_in_one_source_keeps_the_other_sources_rows(
    db: Database,
) -> None:
    """A missing Tiingo token must not throw away crypto that needed no credential.

    Whole-batch adapter errors are `UserError` subclasses, so before containment
    this propagated out of `pull()` — and because `_store` runs after the source
    loop, the CoinGecko observations already fetched were discarded with it. The
    crypto half of this portfolio never required the token that failed.
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
    tiingo.raises = PriceFeedAuthError("No Tiingo API token is stored")

    result = _service(db, tiingo, _FakeCoinGecko()).pull()

    assert result.rows_written == 1
    assert result.securities_priced == 1
    assert ("s_eq", "price_feed_error") in [
        (u.security_id, u.reason) for u in result.unpriced
    ]


def test_a_failed_source_reports_the_message_that_says_how_to_fix_it(
    db: Database,
) -> None:
    """Containing the abort must not turn a fixable error into a silent one.

    Before containment the user at least saw "No Tiingo API token is stored" and
    a non-zero exit. Swallowing that would leave only per-security
    `price_feed_error` lines, which name no provider and no remedy — trading an
    over-loud failure for an invisible one.
    """
    _seed_security(db, security_id="s_eq", name="Apple Inc", ticker="AAPL")
    _hold(db, "s_eq")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})
    tiingo.raises = PriceFeedAuthError("No Tiingo API token is stored")

    result = _service(db, tiingo).pull()

    assert [(f.source_type, f.message) for f in result.failed_sources] == [
        ("tiingo", "No Tiingo API token is stored")
    ]


def test_a_feed_key_already_bound_to_another_security_does_not_abort_the_pull(
    db: Database,
) -> None:
    """`app.securities.coingecko_id` has no UNIQUE constraint, so two entries can share one.

    Both are legitimate catalog rows — the same coin held at two brokers. The
    second reaches `SecurityLinksRepo.insert`, whose `_guard_uniqueness` refuses
    the collision with a `UserError`; uncaught, that aborted the entire command
    before any adapter ran, so neither security got priced.
    """
    for security_id in ("s_btc_a", "s_btc_b"):
        _seed_security(
            db,
            security_id=security_id,
            name="Bitcoin",
            security_type="crypto",
            coingecko_id="bitcoin",
        )
        _hold(db, security_id)

    result = _service(db, _FakeTiingo(), _FakeCoinGecko()).pull()

    assert result.securities_priced == 1
    assert [u.reason for u in result.unpriced] == ["feed_key_bound_elsewhere"]


def test_a_close_too_small_to_store_is_dropped_without_losing_the_batch(
    db: Database,
) -> None:
    """Polars quantizes below 1e-10 to exactly zero, after the adapter's `> 0` guard.

    The stored column is DECIMAL(28,10), so a sub-penny token's real quote
    rounds to 0 and trips `raw.security_prices CHECK (close > 0)` —
    a `duckdb.ConstraintException` that `classify_user_error` does not
    recognise, surfacing as a traceback and losing every coin in the batch. The
    fix drops the unrepresentable row, so the well-behaved coin still lands.
    """
    _seed_security(
        db,
        security_id="s_dust",
        name="Dust Coin",
        security_type="crypto",
        coingecko_id="dust-coin",
    )
    _seed_security(
        db,
        security_id="s_btc",
        name="Bitcoin",
        security_type="crypto",
        coingecko_id="bitcoin",
    )
    _hold(db, "s_dust")
    _hold(db, "s_btc")
    coingecko = _FakeCoinGecko()
    coingecko.close_by_key = {"dust-coin": Decimal("1.2345E-11")}

    result = _service(db, _FakeTiingo(), coingecko).pull()

    assert result.rows_written == 1
    assert [u.reason for u in result.unpriced] == ["close_below_storable_precision"]


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


def test_every_source_the_service_writes_resolves_in_staging() -> None:
    """A source_type this service writes MUST be mapped in prep.stg_security_prices.

    The two halves ship in different files, and nothing else connects them.
    ``PriceService`` writes ``raw.security_prices`` rows tagged with its own
    ``source_type`` constants; ``prep.stg_security_prices`` resolves each row
    through a ``CASE p.source_type`` whose result is compared against
    ``app.security_links.ref_kind`` in an INNER JOIN. An unmapped source makes
    that CASE return NULL, the comparison UNKNOWN, and the join drops the row —
    with no error, no counter, and no way to recover it by accepting bindings
    later, because the failure is in the mapping rather than the binding.

    The staging module's own tripwire could not catch this: it reads the CASE and
    fires only when the CASE changes, so shipping a *writer* for an unmapped
    source left it green while every row written went nowhere. This test watches
    the other direction, and lives in the unit gate so it runs on every commit.
    """
    mapping = ref_kind_mapping()
    written = {TIINGO_SOURCE_TYPE, COINGECKO_SOURCE_TYPE}

    assert written <= mapping.keys(), (
        f"PriceService writes {sorted(written - mapping.keys())} but "
        f"prep.stg_security_prices maps only {sorted(mapping)} — every row written "
        f"for an unmapped source is discarded permanently by the INNER JOIN. Add "
        f"`WHEN '<source>' THEN '<ref_kind>'` to the model's CASE."
    )


def test_the_ref_kind_the_service_binds_matches_what_staging_expects() -> None:
    """Mapping the source is only half of it — the ref_kind must agree too.

    ``PriceService`` writes the binding into ``app.security_links.ref_kind``;
    staging joins on ``ref_kind = CASE ... END``. If the two disagree on the
    spelling, the source is mapped, the binding is accepted, and the row is
    still dropped — the same silent loss as an unmapped source, reached a
    different way.
    """
    mapping = ref_kind_mapping()

    assert mapping[TIINGO_SOURCE_TYPE] == TIINGO_REF_KIND
    assert mapping[COINGECKO_SOURCE_TYPE] == COINGECKO_REF_KIND
