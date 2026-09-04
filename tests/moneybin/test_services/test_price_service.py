"""Tests for PriceService: feed-key derivation and the pull orchestration.

No test here opens a socket. The adapters are replaced by in-memory fakes that
record what they were asked for, so these tests assert MoneyBin's decisions —
which securities get fetched, which keys bind silently, which route to review —
rather than any provider's behaviour, which test_connectors/test_prices covers.
"""

from __future__ import annotations

import re
import time as _time
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

import moneybin
from moneybin import error_codes
from moneybin.connectors.prices.errors import PriceFeedAuthError
from moneybin.connectors.prices.protocol import (
    PriceFetchFailure,
    PriceFetchResult,
    PriceObservation,
    SecurityRef,
)
from moneybin.connectors.prices.tiingo import TickerMetadata
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.limits import (
    NOTE_MAX_LEN,
)
from moneybin.price_sources import (
    FEED_KEY_ROLE,
    PRICE_SOURCES,
    REF_KIND_BY_SOURCE_TYPE,
)
from moneybin.repositories.security_link_decisions_repo import (
    SecurityLinkDecisionsRepo,
)
from moneybin.repositories.security_links_repo import SecurityLinksRepo
from moneybin.services.price_service import (
    _AUTO_REVERSAL,  # pyright: ignore[reportPrivateUsage]  # pinned against the model
    COINGECKO,
    MAX_STORED_PRICE,
    PRICE_QUANTUM,
    TIINGO,
    HeldSecurity,
    PriceService,
)
from moneybin.services.security_links_service import SecurityLinksService
from moneybin.services.undo_service import UndoService
from moneybin.tables import (
    AUDIT_LOG,
    SECURITIES,
    SECURITY_LINK_DECISIONS,
    SECURITY_LINKS,
)
from tests.moneybin.price_model_helpers import (
    historical_reversal_actor,
    ref_kind_mapping,
)

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
        # The same condition raised during key *derivation* instead of the fetch.
        # A missing token takes this shape on a first pull, when no security has
        # a binding yet and every one of them needs a metadata round-trip.
        self.raises_on_metadata: Exception | None = None

    def fetch_metadata(self, ticker: str) -> TickerMetadata | None:
        self.metadata_calls.append(ticker)
        if self.raises_on_metadata is not None:
            raise self.raises_on_metadata
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


def _hold(
    db: Database,
    security_id: str,
    *,
    quantity: str = "10",
    currency_code: str = "USD",
    account_id: str = "acct1",
) -> None:
    db.execute(
        "INSERT INTO core.dim_holdings (account_id, security_id, quantity, "
        "currency_code) VALUES (?, ?, ?, ?)",
        [account_id, security_id, Decimal(quantity), currency_code],
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

    Matches tests/moneybin/test_mcp/test_middleware.py::_tool_call_count; the
    alternative ``.labels(...)._value.get()`` needs a pyright suppression for
    protected access.
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


def test_a_share_class_difference_routes_to_review(db: Database) -> None:
    """Two share classes of one issuer are different securities at different prices.

    "Class" is itself a corporate suffix, so the only thing telling BRK.A from
    BRK.B survives tokenization as a bare "a" against a bare "b" — one character
    inside twenty, which the ratio scores 0.95 and reads as agreement. Binding on
    that prices a Class B holding at the Class A close, and Berkshire's two
    classes differ by roughly three orders of magnitude.

    A ticker resolving to the wrong share class is exactly the recycled- or
    mistyped-symbol case the three-signal gate exists to catch, so it belongs in
    the queue rather than in a silent binding.
    """
    _seed_security(
        db, security_id="s1", name="Berkshire Hathaway Class B", ticker="BRK.B"
    )
    _hold(db, "s1")
    tiingo = _FakeTiingo(
        metadata={"BRK.B": TickerMetadata("Berkshire Hathaway Class A", None)}
    )

    _service(db, tiingo).pull()

    assert _links(db) == []
    assert [reason for _, _, reason in _decisions(db)] == ["name_divergence"]


def test_a_numbered_share_class_difference_also_routes_to_review(db: Database) -> None:
    """A class label is not always one character: R5 and R6 are different funds.

    Retirement share classes ("R1".."R6") and numbered institutional classes
    ("A1"/"A2") differ by a single character in a long, otherwise identical
    name, so the ratio clears them exactly as "Class A" against "Class B" does —
    but the length-one test cannot see a two-character token, and the fuzzy
    branch then binds the wrong class's ticker and prices the holding from it on
    every later refresh.

    Fixtured separately from the Berkshire case because neither catches the
    other: that one has a one-character difference this rule ignores, this one a
    two-character difference the length test ignores.
    """
    _seed_security(
        db, security_id="s1", name="American Funds Growth Fund Class R6", ticker="RGAGX"
    )
    _hold(db, "s1")
    tiingo = _FakeTiingo(
        metadata={"RGAGX": TickerMetadata("American Funds Growth Fund Class R5", None)}
    )

    _service(db, tiingo).pull()

    assert _links(db) == []
    assert [reason for _, _, reason in _decisions(db)] == ["name_divergence"]


def test_a_matching_numbered_share_class_still_binds(db: Database) -> None:
    """The shape test must refuse a DIFFERENCE, not the presence of a number.

    Without this pair, refusing on every numbered token would pass the
    divergence test above and strand every R-class fund in the queue forever.

    The provider name carries one extra word rather than only a corporate
    suffix, deliberately. A suffix-only difference tokenizes to the same list
    and returns agreement before the discriminator is consulted, so the fixture
    would assert nothing about the rule it is named for.
    """
    _seed_security(
        db, security_id="s1", name="American Funds Growth Fund Class R6", ticker="RGAGX"
    )
    _hold(db, "s1")
    tiingo = _FakeTiingo(
        metadata={
            "RGAGX": TickerMetadata("American Funds Growth Fund America Class R6", None)
        }
    )

    _service(db, tiingo).pull()

    assert _links(db) == [("tiingo_ticker", "RGAGX", "s1")]
    assert _decisions(db) == []


def test_a_matching_share_class_still_binds(db: Database) -> None:
    """The discriminator must refuse a DIFFERENCE, not the presence of a class.

    Paired with the divergence above deliberately: a check that refused whenever
    either name carried a class token would pass that test and fail this one, and
    would strand every share-class security in the review queue permanently.
    """
    _seed_security(
        db, security_id="s1", name="Berkshire Hathaway Class B", ticker="BRK.B"
    )
    _hold(db, "s1")
    tiingo = _FakeTiingo(
        metadata={"BRK.B": TickerMetadata("Berkshire Hathaway Inc Class B", None)}
    )

    _service(db, tiingo).pull()

    assert _links(db) == [("tiingo_ticker", "BRK.B", "s1")]
    assert _decisions(db) == []


def test_an_abbreviated_class_marker_still_binds(db: Database) -> None:
    """An abbreviated marker and a spelled-out one name the same share class.

    Pins the discriminator at length one. Widening it to two would catch "cl"
    here and refuse — putting every security whose broker abbreviates the marker
    word into the queue permanently, which is the noise the whole suffix list
    exists to prevent.
    """
    _seed_security(db, security_id="s1", name="Berkshire Hathaway CL B", ticker="BRK.B")
    _hold(db, "s1")
    tiingo = _FakeTiingo(
        metadata={"BRK.B": TickerMetadata("Berkshire Hathaway Class B", None)}
    )

    _service(db, tiingo).pull()

    assert _links(db) == [("tiingo_ticker", "BRK.B", "s1")]
    assert _decisions(db) == []


def test_a_class_only_the_provider_names_routes_to_review(db: Database) -> None:
    """A discriminator on one side alone is still an unanswered question.

    A catalog name that omits the class says nothing about which class the
    ticker holds, so there is no agreement to bind on — only an absence. Pins
    the comparison as symmetric: reading the catalog's tokens alone would find
    nothing missing here and bind on a 0.95 ratio, which is exactly how a BRK.B
    holding ends up priced at the Class A close.
    """
    _seed_security(db, security_id="s1", name="Berkshire Hathaway", ticker="BRK.B")
    _hold(db, "s1")
    tiingo = _FakeTiingo(
        metadata={"BRK.B": TickerMetadata("Berkshire Hathaway Class A", None)}
    )

    _service(db, tiingo).pull()

    assert _links(db) == []
    assert [reason for _, _, reason in _decisions(db)] == ["name_divergence"]


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
    assert _decisions(db) == [(TIINGO.feed_ref_kind, "TRST", "name_divergence")]


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
    assert _decisions(db) == [(TIINGO.feed_ref_kind, "SOH", "binding_was_reversed")]


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
    assert _decisions(db) == [(TIINGO.feed_ref_kind, "SOH", "binding_was_reversed")]


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


def test_a_user_confirmed_binding_is_never_treated_as_stale(db: Database) -> None:
    """Provider symbol formats diverge from ours, and the user already settled it.

    ``BRK-B`` against a ``BRK.B`` catalog ticker is the exact case
    ``_binding_is_stale``'s own docstring says is allowed. Comparing the strings
    without consulting ``decided_by`` calls it stale anyway; retirement is scoped
    to ``auto`` so it retires nothing, and execution falls through to re-derive —
    leaving two accepted rows under one ref_kind. ``_bound_ref``'s unordered
    ``LIMIT 1`` can return either one afterwards, so a later pull can silently
    resume pricing from the symbol the user overrode.
    """
    _seed_security(db, security_id="s_brk", name="Berkshire Hathaway", ticker="BRK.B")
    _hold(db, "s_brk")
    db.execute(
        f"INSERT INTO {SECURITY_LINKS.full_name} "  # noqa: S608  # TableRef constant
        "(link_id, security_id, ref_kind, ref_value, source_type, status, "
        "decided_by, decided_at) VALUES (?, ?, ?, ?, ?, 'accepted', 'user', "
        "CURRENT_TIMESTAMP)",
        ["l_brk", "s_brk", "tiingo_ticker", "BRK-B", "tiingo"],
    )
    tiingo = _FakeTiingo(metadata={"BRK.B": TickerMetadata("Berkshire Hathaway", None)})

    _service(db, tiingo).pull()

    assert _links(db) == [(TIINGO.feed_ref_kind, "BRK-B", "s_brk")]
    assert [ref.provider_security_key for ref in tiingo.fetched] == ["BRK-B"]
    assert tiingo.metadata_calls == []


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

    # `since` moves the start only — the end stays at the last complete day, so a
    # backfill cannot reach into today's still-forming bar either.
    assert tiingo.windows == [(date(2026, 1, 1), _TODAY - timedelta(days=1))]


def test_the_window_ends_at_the_last_complete_day(db: Database) -> None:
    """Today's bar is still forming, and this table gives the first writer the key.

    `raw.security_prices` is append-only with `on_conflict="ignore"` and
    `price_date` in the primary key, so a midday pull that stores an in-progress
    close owns that date permanently — the evening pull carrying the settled
    close is silently dropped. `.claude/rules/data-extraction.md` forbids
    partial-day extraction for exactly this reason, and the CoinGecko adapter is
    already structurally incapable of it (today's crypto needs tomorrow's
    midnight point). The equity path had no equivalent bound.

    Requesting a future date would be the same defect one day further out.
    """
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _hold(db, "s1")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})

    _service(db, tiingo).pull()

    start, end = tiingo.windows[0]
    assert end == _TODAY - timedelta(days=1)
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


def test_an_auth_failure_deriving_a_feed_key_keeps_the_other_sources_rows(
    db: Database,
) -> None:
    """A pull derives keys before it fetches, and derivation asks Tiingo too.

    ``_feed_key`` reaches ``fetch_metadata`` for any security with no accepted
    binding — the state every security is in on a first run, which is exactly
    when no token is stored. That raises in the loop *above* the per-source
    containment, so nothing catches it before it leaves ``pull()`` and
    ``_store()`` never runs. CoinGecko needs no token and was never asked for
    one, so its rows must survive.
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
    tiingo = _FakeTiingo()
    tiingo.raises_on_metadata = PriceFeedAuthError("No Tiingo API token is stored")

    result = _service(db, tiingo, _FakeCoinGecko()).pull()

    assert result.rows_written == 1
    assert result.securities_priced == 1
    assert [(f.source_type, f.message) for f in result.failed_sources] == [
        ("tiingo", "No Tiingo API token is stored")
    ]


def test_a_derivation_failure_is_not_re_asked_for_every_security(
    db: Database,
) -> None:
    """An auth failure answers for the whole source, not for the one security.

    ``errors.py`` calls these whole-batch conditions. Re-deriving per security
    spends one doomed request each, and on a rate-limit error deepens the very
    limit that caused it.
    """
    for index in range(3):
        _seed_security(
            db, security_id=f"s{index}", name=f"Company {index}", ticker=f"TCK{index}"
        )
        _hold(db, f"s{index}")
    tiingo = _FakeTiingo()
    tiingo.raises_on_metadata = PriceFeedAuthError("No Tiingo API token is stored")

    _service(db, tiingo).pull()

    assert tiingo.metadata_calls == ["TCK0"]


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


def test_a_close_too_large_to_store_drops_only_its_own_security(
    db: Database,
) -> None:
    """The magnitude bound is the twin of the precision bound, on the same column.

    ``set_mark`` refuses a close above ``MAX_STORED_PRICE`` outright and checks
    magnitude BEFORE precision, because quantizing a number that large overflows
    the decimal context first. The pull path had only the lower half, so an
    oversized provider close reached frame construction and ``_store()`` — both
    outside the per-source ``PriceFeedError`` containment — and took down the
    whole pull, discarding every well-priced security fetched alongside it.

    That is strictly worse than the sub-precision case this mirrors: there the
    bad row was dropped and the good one still landed.
    """
    _seed_security(
        db,
        security_id="s_huge",
        name="Overflow Coin",
        security_type="crypto",
        coingecko_id="huge-coin",
    )
    _seed_security(
        db,
        security_id="s_btc",
        name="Bitcoin",
        security_type="crypto",
        coingecko_id="bitcoin",
    )
    _hold(db, "s_huge")
    _hold(db, "s_btc")
    coingecko = _FakeCoinGecko()
    coingecko.close_by_key = {"huge-coin": Decimal(10) ** 19}

    result = _service(db, _FakeTiingo(), coingecko).pull()

    assert result.rows_written == 1
    assert [u.reason for u in result.unpriced] == ["close_above_storable_range"]


@pytest.mark.parametrize("tz", ["Pacific/Kiritimati", "Pacific/Midway"])
def test_the_complete_day_cutoff_follows_the_utc_day_not_the_host(
    db: Database, monkeypatch: pytest.MonkeyPatch, tz: str
) -> None:
    """The cutoff must mean one complete PROVIDER day, on any host.

    Every day this service reasons about is a UTC day — CoinGecko's close for D
    is the 00:00 UTC point of D+1 — so a host-local clock disagrees with the data
    off UTC. East of UTC the local date rolls first and the cutoff names a UTC day
    whose close does not exist yet; on an append-only table with
    on_conflict="ignore" that partial value would own the date permanently. West
    of UTC a completed UTC day goes unfetched until local catches up.

    The two zones are UTC+14 and UTC-11, chosen so the assertion is never vacuous:
    Kiritimati's local date runs ahead of UTC whenever it is 10:00 UTC or later
    and Midway's runs behind before 11:00 UTC, so whatever the instant, at least
    one parametrization has a local date that differs from the UTC one. A
    host-local clock cannot pass both.

    Asserted through ``pull``'s refusal rather than the stored date, so this
    tests the behaviour a user meets. Today's UTC date is always after the last
    complete UTC day, so it must always be refused — but on a host running a day
    ahead of UTC the cutoff lands on today, the refusal never fires, and the pull
    proceeds to ask a provider for a day that has not closed.
    """
    monkeypatch.setenv("TZ", tz)
    _time.tzset()
    try:
        # Constructed WITHOUT `today`: the module-level helper injects a fixed
        # date, which is exactly the default this test has to exercise.
        service = PriceService(db, tiingo=_FakeTiingo(), coingecko=_FakeCoinGecko())

        with pytest.raises(UserError) as caught:
            service.pull(since=datetime.now(UTC).date())

        assert caught.value.code == error_codes.INVESTMENT_DATE_RANGE_INVALID
    finally:
        monkeypatch.undo()
        _time.tzset()


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

    service.set_mark(
        "s1", date(2026, 6, 30), Decimal("42.50"), quote_currency="USD", note="409A"
    )

    row = db.execute(
        "SELECT security_id, price_date, quote_currency, close, note "
        "FROM app.security_price_overrides"
    ).fetchone()
    assert row == ("s1", date(2026, 6, 30), "USD", Decimal("42.50"), "409A")


def test_setting_a_mark_twice_replaces_the_value(db: Database) -> None:
    """A correction replaces the number; the mark stays one row for its date."""
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    service.set_mark(
        "s1", date(2026, 6, 30), Decimal("42.50"), quote_currency="USD", note="first"
    )
    service.set_mark(
        "s1", date(2026, 6, 30), Decimal("51.00"), quote_currency="USD", note="revised"
    )

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
        service.set_mark(
            "s1", date(2026, 6, 30), Decimal("0"), quote_currency="USD", note=None
        )

    count = db.execute("SELECT COUNT(*) FROM app.security_price_overrides").fetchone()
    assert count is not None
    assert count[0] == 0


def test_an_oversized_note_is_refused(db: Database) -> None:
    """DuckDB ``VARCHAR`` is unbounded, so the application has to set the limit.

    An unbounded note is stored once and then copied into the audit before/after
    image on every later correction, so the cost of one oversized string is paid
    repeatedly. ``NOTE_MAX_LEN`` is the bound the rest of the codebase already
    applies to user note text; this path simply skipped it.

    The length is derived from the constant rather than written as a literal, so
    the test still names the real boundary if the bound is ever retuned.
    """
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    with pytest.raises(ValueError, match="exceeds"):
        service.set_mark(
            "s1",
            date(2026, 6, 30),
            Decimal("42.50"),
            quote_currency="USD",
            note="x" * (NOTE_MAX_LEN + 1),
        )

    count = db.execute("SELECT COUNT(*) FROM app.security_price_overrides").fetchone()
    assert count is not None
    assert count[0] == 0


def test_a_note_at_the_limit_is_stored(db: Database) -> None:
    """The boundary itself is legal — an off-by-one here rejects a valid note.

    Paired with the oversized case deliberately: a guard written as ``>=`` passes
    that test and fails this one, and neither fixture alone can tell the two
    versions apart.
    """
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    service.set_mark(
        "s1",
        date(2026, 6, 30),
        Decimal("42.50"),
        quote_currency="USD",
        note="x" * NOTE_MAX_LEN,
    )

    row = db.execute(
        "SELECT LENGTH(note) FROM app.security_price_overrides WHERE security_id = 's1'"
    ).fetchone()
    assert row is not None
    assert row[0] == NOTE_MAX_LEN


def test_a_blank_note_is_refused(db: Database) -> None:
    """``--note '   '`` is a slip, not an annotation; ``None`` is how you omit one."""
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    with pytest.raises(ValueError, match="non-empty"):
        service.set_mark(
            "s1",
            date(2026, 6, 30),
            Decimal("42.50"),
            quote_currency="USD",
            note="   ",
        )


def test_a_malformed_quote_currency_is_refused(db: Database) -> None:
    """A typo'd currency is not a rejected input — it is a mark that joins nothing.

    ``dim_holdings`` matches a mark to a position on exact string equality, so
    ``USDX`` writes, reports success, and values nothing, forever and silently.
    Refusing at the write is the only point where the mistake is still visible.
    """
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    with pytest.raises(UserError) as caught:
        service.set_mark(
            "s1", date(2026, 6, 30), Decimal("42.50"), quote_currency="USDX", note=None
        )

    assert caught.value.code == error_codes.INVESTMENT_PRICE_MARK_CURRENCY_INVALID
    count = db.execute("SELECT COUNT(*) FROM app.security_price_overrides").fetchone()
    assert count is not None
    assert count[0] == 0


def test_a_padded_currency_writes_the_canonical_series(db: Database) -> None:
    """Stray whitespace is a typing slip, not a different currency."""
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    service.set_mark(
        "s1", date(2026, 6, 30), Decimal("42.50"), quote_currency=" usd ", note=None
    )

    row = db.execute(
        "SELECT quote_currency FROM app.security_price_overrides"
    ).fetchone()
    assert row == ("USD",)


def test_delete_normalizes_the_currency_the_same_way_set_does(db: Database) -> None:
    """The two writers must agree on the key, or a mark becomes unreachable.

    ``set`` is the only way to create an override and ``delete`` the only way to
    remove one. If they canonicalized differently, a mark written under one
    spelling could not be deleted under the other — the exact unreachability
    ``delete`` exists to prevent.
    """
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())
    service.set_mark(
        "s1", date(2026, 6, 30), Decimal("42.50"), quote_currency="USD", note=None
    )

    assert service.delete_mark("s1", date(2026, 6, 30), quote_currency=" usd ") is True


def test_deleting_with_a_malformed_currency_is_refused(db: Database) -> None:
    """Reporting "nothing was removed" for a typo would read as "no mark existed"."""
    _seed_security(db, security_id="s1", name="Private Co")

    with pytest.raises(UserError) as caught:
        _service(db, _FakeTiingo()).delete_mark(
            "s1", date(2026, 6, 30), quote_currency="US"
        )

    assert caught.value.code == error_codes.INVESTMENT_PRICE_MARK_CURRENCY_INVALID


def test_a_rejected_mark_currency_does_not_reach_the_logged_message(
    db: Database,
) -> None:
    """The twin of the same split in ``currency_service.require_currency``.

    ``quote_currency`` is free text a user types, and text-mode
    ``handle_cli_errors`` sends ``message`` to ``logger.error`` against a file
    handler with no level filter. Two services validating the same kind of
    value must not disagree about where the rejected one is allowed to go.
    """
    _seed_security(db, security_id="s1", name="Private Co")

    with pytest.raises(UserError) as caught:
        _service(db, _FakeTiingo()).set_mark(
            "s1",
            date(2026, 6, 30),
            Decimal("42.50"),
            quote_currency="PASTED-9876543210",
            note=None,
        )

    assert caught.value.code == error_codes.INVESTMENT_PRICE_MARK_CURRENCY_INVALID
    assert "PASTED-9876543210" not in caught.value.message
    assert caught.value.hint is not None
    assert "PASTED-9876543210" in caught.value.hint


def test_deleting_a_mark_returns_the_date_to_provider_valuation(db: Database) -> None:
    """Without delete a mark is unreachable: it outranks every provider row."""
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())
    service.set_mark(
        "s1", date(2026, 6, 30), Decimal("42.50"), quote_currency="USD", note=None
    )

    deleted = service.delete_mark("s1", date(2026, 6, 30), quote_currency="USD")

    assert deleted is True
    count = db.execute("SELECT COUNT(*) FROM app.security_price_overrides").fetchone()
    assert count is not None
    assert count[0] == 0


def test_deleting_an_absent_mark_reports_that_nothing_was_removed(
    db: Database,
) -> None:
    """A silent success would read as "the override is gone" when none existed."""
    _seed_security(db, security_id="s1", name="Private Co")

    removed = _service(db, _FakeTiingo()).delete_mark(
        "s1", date(2026, 6, 30), quote_currency="USD"
    )

    assert removed is False


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

    The two halves used to ship in different files with nothing connecting them:
    ``PriceService`` wrote ``raw.security_prices`` rows tagged with its own
    ``source_type`` constants, and ``prep.stg_security_prices`` restated the
    ``source_type`` -> ``ref_kind`` mapping in a CASE. An unmapped source made
    that CASE return NULL, the comparison UNKNOWN, and the INNER JOIN drop the
    row — with no error, no counter, and no way to recover it by accepting
    bindings later, because the failure was in the mapping rather than the
    binding. That is how C.2 discarded every row its writers produced.

    ``seeds.price_source_map`` now declares both halves in one row, so the
    failure needs a source declaring the security types it prices while
    declaring no ref_kind. ``ref_kind_mapping`` additionally fails if the model
    stops joining the registry, which is the other way the two could part.
    """
    mapping = ref_kind_mapping()
    written = {s.source_type for s in PRICE_SOURCES if s.security_types}

    assert written, (
        "no price source declares the security types it prices, so the subset "
        "check below would hold vacuously"
    )
    assert written <= mapping.keys(), (
        f"PriceService fetches {sorted(written - mapping.keys())} but "
        f"prep.stg_security_prices resolves only {sorted(mapping)} — every row "
        f"written for an unmapped source is discarded permanently by the INNER "
        f"JOIN. Give the source a ref_kind in the registry."
    )


def test_every_ref_kind_the_registry_declares_is_admitted_by_the_schema() -> None:
    """Declaring a ref_kind is only half of it — the CHECK must admit it too.

    ``PriceService`` writes the binding into ``app.security_links.ref_kind``,
    whose CHECK constraint is a hand-maintained list in a migration-frozen
    schema file that no registry edit can reach. A ref_kind the registry
    declares but the constraint rejects fails every insert at run time, so
    adding a provider needs a migration in the same change.

    This replaces a comparison of the registry against itself. Before the
    registry, staging restated the mapping in a CASE and the service held its
    own constants, so asserting the two agreed was a real cross-check; now both
    sides read one row and only the schema is genuinely independent.
    """
    schema = (
        Path(moneybin.__file__).parent / "sql" / "schema" / "app_security_links.sql"
    ).read_text()
    check = re.search(r"CHECK\s*\(\s*ref_kind\s+IN\s*\(([^)]*)\)", schema)
    assert check is not None, (
        "no `CHECK (ref_kind IN (...))` found in app_security_links.sql; the "
        "constraint this test crosses the registry against has moved"
    )
    admitted = set(re.findall(r"'([^']+)'", check.group(1)))
    declared = set(REF_KIND_BY_SOURCE_TYPE.values())

    assert declared <= admitted, (
        f"the registry declares {sorted(declared - admitted)}, which "
        f"app.security_links.ref_kind rejects — every binding the service writes "
        f"for those sources fails its CHECK. Widen the constraint in a migration."
    )


def test_every_ref_kind_the_service_queues_is_routed_as_a_feed_key() -> None:
    """The opposite direction of the same contract, per the C.2 staging lesson.

    `SecurityLinksService.accept` routes on `FEED_KEY_REF_KINDS`: a ref_kind in
    that set BINDS the feed, one outside it MERGES two securities and DELETEs
    one. So a new adapter whose ref_kind never reaches that set would have its
    review decisions routed into the merge path — destroying the very security
    the user was trying to price.

    Asserted against the router itself, and across two INDEPENDENT registry
    columns, so it can actually fail. ``security_types`` says what PriceService
    fetches; ``ref_role`` says what accepting the ref does. Comparing the
    router against the set it reads, or against the column that set derives
    from, would restate one expression twice and could never fail.

    Only the routed direction is universal. A RETIRED provider keeps
    ``ref_role = 'feed_key'`` with no ``security_types``, which is exactly the
    state that must still bind, so the reverse implication is pinned on the
    concrete identity row instead.
    """
    for source in PRICE_SOURCES:
        if not source.security_types:
            continue
        assert SecurityLinksService.binds_a_feed_key(source.feed_ref_kind), (
            f"PriceService fetches {source.source_type!r}, but accepting a "
            f"{source.feed_ref_kind!r} decision routes to the MERGE path, which "
            "re-points every reference and deletes the provisional security — "
            f"destroying the one the review meant to price. Set ref_role to "
            f"{FEED_KEY_ROLE!r} in the registry."
        )

    identity_ref = REF_KIND_BY_SOURCE_TYPE["plaid"]
    assert not SecurityLinksService.binds_a_feed_key(identity_ref), (
        f"{identity_ref!r} names a second catalog row for one instrument, not a "
        "market-data symbol; routing it to the BIND path would create a link and "
        "skip the merge that accepting it exists to perform"
    )


def test_a_routed_source_with_no_derivation_refuses_to_borrow_another_providers() -> (
    None
):
    """Registering a provider must not silently give it Tiingo's key derivation.

    ``_route`` selects any source the registry declares security types for, so
    a third provider becomes reachable the moment its row lands. Both derivation
    sites used to read "CoinGecko, else Tiingo", which meant that provider would
    be handed ``security.ticker`` and Tiingo's metadata contract — binding or
    fetching another provider's identifier while looking correctly registered.
    That is the same silent default ``_adapter_for`` was made loud for, and all
    three had to move together or the fix only relocates the failure.
    """
    service = PriceService(
        MagicMock(), tiingo=_FakeTiingo(), coingecko=_FakeCoinGecko()
    )
    security = HeldSecurity(
        security_id="s1",
        name="Test Security",
        security_type="equity",
        quote_currency="USD",
        ticker="TEST",
        exchange=None,
        coingecko_id="test-coin",
    )

    with pytest.raises(NotImplementedError, match="unregistered_provider"):
        service._catalog_ref(  # pyright: ignore[reportPrivateUsage]  # the dispatch under test
            security, "unregistered_provider"
        )


def test_every_source_the_registry_routes_to_has_an_adapter_wired() -> None:
    """A registry row with no adapter behind it reports every holding as cash.

    ``_route`` returns ``self._adapters.get(...)``, and a ``None`` adapter makes
    the pull record ``no_price_source`` — the same answer a sweep position gets,
    which is the one reply that must not be counterfeitable. ``_adapter_for`` was
    made loud for the UNREGISTERED case; this is the registered-but-unwired one,
    and wiring an adapter is the step the registry deliberately cannot do for you.
    """
    service = PriceService(
        MagicMock(), tiingo=_FakeTiingo(), coingecko=_FakeCoinGecko()
    )

    for source in PRICE_SOURCES:
        for security_type in sorted(source.security_types):
            held = HeldSecurity(
                security_id="s1",
                name="Test Security",
                security_type=security_type,
                quote_currency="USD",
                ticker="TEST",
                exchange=None,
                coingecko_id="test-coin",
            )
            routed, adapter = service._route(held)  # pyright: ignore[reportPrivateUsage]  # the dispatch under test
            assert routed == source.source_type
            assert adapter is not None, (
                f"{source.source_type!r} is routed for security_type "
                f"{security_type!r} but no adapter is wired for it, so every such "
                "holding reports no_price_source and reads as unpriceable"
            )


class TestMarkCurrencyResolution:
    """A mark carries the currency that makes it reach the position, or asks.

    core.dim_holdings joins a price to a position only where
    `lp.quote_currency = UPPER(p.currency_code)`, so a mark quoted in any other
    currency writes, reports success, and values nothing — the failure is
    invisible at the moment it happens and stays invisible afterwards.
    """

    def test_it_reads_the_currency_the_position_is_denominated_in(
        self, db: Database
    ) -> None:
        """One currency across the open positions is the unambiguous case."""
        _seed_security(db, security_id="s_bhp", name="BHP Group", currency_code="AUD")
        _hold(db, "s_bhp", currency_code="EUR")
        service = _service(db, _FakeTiingo())

        assert service.resolve_quote_currency("s_bhp") == "EUR"

    def test_it_prefers_the_position_over_the_catalog_declaration(
        self, db: Database
    ) -> None:
        """The join compares against the position, so the catalog cannot decide it.

        app.securities.currency_code is what the provider fetch quotes in, but a
        mark exists to value a holding — and the holding's own currency_code is
        the value dim_holdings actually compares. Taking the catalog's answer
        here would write a mark that matches the provider series and still joins
        to nothing, which is the original bug wearing a different default.
        """
        _seed_security(db, security_id="s_bhp", name="BHP Group", currency_code="USD")
        _hold(db, "s_bhp", currency_code="AUD")
        service = _service(db, _FakeTiingo())

        assert service.resolve_quote_currency("s_bhp") == "AUD"

    def test_an_explicit_currency_answers_without_consulting_the_holdings(
        self, db: Database
    ) -> None:
        """Naming the currency is the escape hatch for every refusing rung.

        The security below is held in two currencies, which rung 2 refuses. An
        explicit code has to win outright, or the escape hatch would not reach
        the only case that needs it.
        """
        _seed_security(db, security_id="s1", name="Dual Listed")
        _hold(db, "s1", currency_code="USD", account_id="a1")
        _hold(db, "s1", currency_code="EUR", account_id="a2")

        service = _service(db, _FakeTiingo())

        assert service.resolve_quote_currency("s1", " gbp ") == "GBP"

    def test_it_refuses_when_one_security_is_held_in_two_currencies(
        self, db: Database
    ) -> None:
        """Two denominations is a question only the user can answer."""
        _seed_security(db, security_id="s_bhp", name="BHP Group")
        _hold(db, "s_bhp", currency_code="AUD", account_id="acct1")
        _hold(db, "s_bhp", currency_code="GBP", account_id="acct2")
        service = _service(db, _FakeTiingo())

        with pytest.raises(UserError) as caught:
            service.resolve_quote_currency("s_bhp")

        assert caught.value.code == error_codes.INVESTMENT_PRICE_MARK_CURRENCY_AMBIGUOUS
        message = str(caught.value)
        assert "AUD" in message and "GBP" in message, "name the candidates"
        assert "--currency" in message, "name the flag that resolves it"

    def test_an_unheld_security_falls_back_to_its_catalog_currency(
        self, db: Database
    ) -> None:
        """Marking a security before holding it is legitimate, not an error.

        A 409A valuation is routinely recorded ahead of the purchase. The
        catalog's currency_code is a declared fact rather than a default, and it
        is what the provider fetch quotes in — so the mark lands in the series
        the provider will later write to instead of beside it.
        """
        _seed_security(db, security_id="s_bhp", name="BHP Group", currency_code="AUD")
        service = _service(db, _FakeTiingo())

        assert service.resolve_quote_currency("s_bhp") == "AUD"

    def test_it_refuses_for_a_security_no_rung_can_answer_for(
        self, db: Database
    ) -> None:
        """Both rungs silent means nothing to infer — ask rather than assume USD.

        app.securities.currency_code is NOT NULL, so the catalog rung answers for
        anything in the catalog; an id that is not there is what leaves the whole
        ladder without an answer.
        """
        service = _service(db, _FakeTiingo())

        with pytest.raises(UserError) as caught:
            service.resolve_quote_currency("s_not_in_catalog")

        assert caught.value.code == error_codes.INVESTMENT_PRICE_MARK_CURRENCY_AMBIGUOUS
        assert "--currency" in str(caught.value)

    def test_a_position_outranks_the_catalog_when_both_answer(
        self, db: Database
    ) -> None:
        """The ladder is ordered, not first-found: the join compares the position."""
        _seed_security(db, security_id="s_bhp", name="BHP Group", currency_code="USD")
        _hold(db, "s_bhp", currency_code="AUD")
        service = _service(db, _FakeTiingo())

        assert service.resolve_quote_currency("s_bhp") == "AUD"

    def test_a_closed_position_does_not_decide_the_currency(self, db: Database) -> None:
        """Quantity zero is a closed position; it values nothing and votes on nothing.

        Without the quantity filter a security sold out of a EUR account and
        rebought in a USD one reads as ambiguous forever, refusing a mark the
        user can legitimately set.
        """
        _seed_security(db, security_id="s_bhp", name="BHP Group")
        _hold(db, "s_bhp", currency_code="EUR", quantity="0", account_id="acct1")
        _hold(db, "s_bhp", currency_code="USD", quantity="5", account_id="acct2")
        service = _service(db, _FakeTiingo())

        assert service.resolve_quote_currency("s_bhp") == "USD"


# --------------------------------------------------------------------------
# The requested window
# --------------------------------------------------------------------------


def test_a_since_after_the_last_complete_day_is_refused(db: Database) -> None:
    """``start > end`` is a usage error, not a feed failure.

    ``pull`` deliberately ends at yesterday, so a ``--since`` of today or later
    inverts the range. Tiingo is handed ``start > end`` and CoinGecko simply
    matches no observation, so every held security comes back as a feed failure
    or unpriced — the user reads a provider outage where they made a typo. The
    refusal happens before any request so no quota is spent answering it.
    """
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _hold(db, "s1")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})
    service = _service(db, tiingo)

    with pytest.raises(UserError) as caught:
        service.pull(since=_TODAY)

    assert caught.value.code == error_codes.INVESTMENT_DATE_RANGE_INVALID
    assert not tiingo.fetched, "no price request may be spent on a refused window"
    assert not tiingo.metadata_calls, "nor a key-derivation round-trip"


def test_a_since_on_the_last_complete_day_is_accepted(db: Database) -> None:
    """The last complete day is a legal single-day window.

    Paired with the refusal above deliberately: a guard written ``since >= end``
    passes that test and fails this one, and neither fixture alone separates the
    two versions.
    """
    _seed_security(db, security_id="s1", name="Apple Inc", ticker="AAPL")
    _hold(db, "s1")
    tiingo = _FakeTiingo(metadata={"AAPL": TickerMetadata("Apple Inc", None)})
    service = _service(db, tiingo)
    last_complete_day = _TODAY - timedelta(days=1)

    service.pull(since=last_complete_day)

    assert tiingo.windows == [(last_complete_day, last_complete_day)]


# --------------------------------------------------------------------------
# What DECIMAL(28, 10) can actually hold
# --------------------------------------------------------------------------


def test_a_price_finer_than_the_stored_resolution_is_refused(db: Database) -> None:
    """A price the column cannot represent must not reach the column.

    ``close`` is ``DECIMAL(28, 10)``. A positive value below one quantum rounds
    to zero on the way in, which then trips the table's own ``CHECK (close > 0)``
    as an untyped DuckDB error — the CLI prints a traceback where it owes a usage
    message. The bound is derived from ``PRICE_QUANTUM`` rather than written as a
    literal, so retuning the column retunes the test with it.
    """
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    with pytest.raises(UserError) as caught:
        service.set_mark(
            "s1",
            date(2026, 6, 30),
            PRICE_QUANTUM / 10,
            quote_currency="USD",
            note=None,
        )

    assert caught.value.code == error_codes.INVESTMENT_PRICE_MARK_UNREPRESENTABLE
    count = db.execute("SELECT COUNT(*) FROM app.security_price_overrides").fetchone()
    assert count is not None
    assert count[0] == 0


def test_a_price_at_the_stored_resolution_is_kept_exactly(db: Database) -> None:
    """One quantum is legal, and is stored as the value the caller passed."""
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    service.set_mark(
        "s1", date(2026, 6, 30), PRICE_QUANTUM, quote_currency="USD", note=None
    )

    row = db.execute(
        "SELECT close FROM app.security_price_overrides WHERE security_id = 's1'"
    ).fetchone()
    assert row is not None
    assert row[0] == PRICE_QUANTUM


def test_a_price_beyond_the_stored_range_is_refused(db: Database) -> None:
    """18 integer digits is the ceiling; the 19th overflows the conversion.

    This fixture is exactly representable at ten decimal places, so the
    resolution guard has nothing to say about it — only the magnitude guard can
    refuse it, which is what makes the two separable.
    """
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    with pytest.raises(UserError) as caught:
        service.set_mark(
            "s1",
            date(2026, 6, 30),
            MAX_STORED_PRICE + PRICE_QUANTUM,
            quote_currency="USD",
            note=None,
        )

    assert caught.value.code == error_codes.INVESTMENT_PRICE_MARK_UNREPRESENTABLE


def test_the_largest_storable_price_is_kept_exactly(db: Database) -> None:
    """The ceiling itself is legal — an off-by-one here refuses a valid mark."""
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    service.set_mark(
        "s1", date(2026, 6, 30), MAX_STORED_PRICE, quote_currency="USD", note=None
    )

    row = db.execute(
        "SELECT close FROM app.security_price_overrides WHERE security_id = 's1'"
    ).fetchone()
    assert row is not None
    assert row[0] == MAX_STORED_PRICE


def test_an_unstorably_large_negative_price_is_refused_as_a_usage_error(
    db: Database,
) -> None:
    """The magnitude bound has to read the magnitude, sign included.

    Storability is checked before the positivity rule because the finite check
    inside it must run first — ``Decimal("NaN") <= 0`` raises. A bound written
    against the signed value therefore lets every negative through to
    ``close.quantize()``, which needs 29 significant digits for a value this
    large and raises ``InvalidOperation`` — an untyped arithmetic error where the
    CLI owes a usage message, on input a user typed.

    The mirror of ``test_a_price_beyond_the_stored_range_is_refused``: same
    magnitude, opposite sign, and only the sign decides which code path answers.
    """
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    with pytest.raises(UserError) as caught:
        service.set_mark(
            "s1",
            date(2026, 6, 30),
            -(MAX_STORED_PRICE + PRICE_QUANTUM),
            quote_currency="USD",
            note=None,
        )

    assert caught.value.code == error_codes.INVESTMENT_PRICE_MARK_UNREPRESENTABLE
    count = db.execute("SELECT COUNT(*) FROM app.security_price_overrides").fetchone()
    assert count is not None
    assert count[0] == 0


# --------------------------------------------------------------------------
# Retiring a feed key whose catalog value moved
# --------------------------------------------------------------------------


def _seed_stale_binding(db: Database, *, ticker: str, ref_value: str) -> None:
    """A security whose catalog ticker has moved away from its bound feed key."""
    _seed_security(db, security_id="s1", name="Meta Platforms", ticker=ticker)
    _hold(db, "s1")
    SecurityLinksRepo(db).insert(
        security_id="s1",
        ref_kind=TIINGO.feed_ref_kind,
        ref_value=ref_value,
        source_type=TIINGO.source_type,
        decided_by="auto",
        actor="system",
    )


def test_a_stale_binding_is_kept_when_no_replacement_can_be_derived(
    db: Database,
) -> None:
    """Retiring before deriving can strand a holding with no key at all.

    The old binding is the only thing pricing this security. Reversing it first
    and then failing to derive a replacement — no provider coverage for the new
    symbol, a transient metadata error, an ambiguous match queued for review —
    leaves the security with no accepted link, so a holding that was valued
    yesterday is unpriced today and nothing says why.
    """
    _seed_stale_binding(db, ticker="META", ref_value="FB")
    tiingo = _FakeTiingo(metadata={})  # META has no coverage at the provider
    service = _service(db, tiingo)

    service.pull()

    assert _links(db) == [(TIINGO.feed_ref_kind, "FB", "s1")]


def test_a_stale_binding_is_retired_once_its_replacement_is_derived(
    db: Database,
) -> None:
    """The retirement still happens — just after there is something to replace it.

    Paired with the test above: a fix that simply stopped retiring would pass
    that one and fail this one, leaving the security two accepted links for one
    ref_kind.
    """
    _seed_stale_binding(db, ticker="META", ref_value="FB")
    tiingo = _FakeTiingo(metadata={"META": TickerMetadata("Meta Platforms", None)})
    service = _service(db, tiingo)

    service.pull()

    assert _links(db) == [(TIINGO.feed_ref_kind, "META", "s1")]


def test_a_feed_key_carried_across_a_merge_is_still_re_derived(db: Database) -> None:
    """A merge moves the key; it must not also freeze it.

    An identity merge repoints every accepted link on the provisional onto the
    survivor, feed keys included, and it does so as a user decision. The moved
    row was being re-stamped with that decision, which put an auto-derived key
    permanently outside `_binding_is_stale` — so a ticker that diverged from the
    catalog after the merge was never retired, and the security kept pricing
    from a symbol its own row no longer claimed, indefinitely and silently.

    The binding arrives here the way the merge delivers it — through
    `repoint`, not a direct insert — because that is the step that was
    rewriting it. Seeding an `auto` row directly would exercise the retirement
    that already worked and prove nothing about the path that broke it.
    """
    _seed_security(db, security_id="s0", name="Meta Platforms", ticker="FB")
    _seed_security(db, security_id="s1", name="Meta Platforms", ticker="META")
    _hold(db, "s1")
    repo = SecurityLinksRepo(db)
    event = repo.insert(
        security_id="s0",
        ref_kind=TIINGO.feed_ref_kind,
        ref_value="FB",
        source_type=TIINGO.source_type,
        decided_by="auto",
        actor="system",
    )
    assert event.target_id is not None
    repo.repoint(
        link_id=event.target_id,
        new_security_id="s1",
        decided_by="user",
        actor="cli",
    )
    tiingo = _FakeTiingo(metadata={"META": TickerMetadata("Meta Platforms", None)})

    _service(db, tiingo).pull()

    assert _links(db) == [(TIINGO.feed_ref_kind, "META", "s1")]


def test_a_stale_binding_is_retired_even_when_its_replacement_collides(
    db: Database,
) -> None:
    """Zero accepted links is the RIGHT outcome here, and the only tested one.

    Deliberately the exception to the two tests above. Those keep a stale binding
    while the replacement is underivable; this one derives a replacement and then
    loses the race for it, so the retirement has already run and the security ends
    up with no accepted link at all.

    Keeping the old key instead would be worse in both directions: it prices this
    security from a symbol its own catalog row no longer claims, and — because
    `_guard_uniqueness` keys on (source_type, ref_kind, ref_value) — it goes on
    blocking the security that legitimately holds that key. Unpriced is visible
    (`feed_key_bound_elsewhere`, then the held-but-unpriced check); mispriced is
    not. Reordering to insert-then-retire also widens the crash window from "no
    accepted link, rebuilt by the next pull" to "two accepted links, permanent".

    Routed through CoinGecko because Tiingo cannot reach it: `_ticker_is_shared`
    refuses to derive a key for a ticker two catalog rows carry, so the collision
    is settled before the insert. `app.securities.coingecko_id` has no such guard.
    """
    _seed_security(
        db,
        security_id="s_holder",
        name="Bitcoin",
        security_type="crypto",
        coingecko_id="bitcoin",
    )
    _hold(db, "s_holder")
    SecurityLinksRepo(db).insert(
        security_id="s_holder",
        ref_kind=COINGECKO.feed_ref_kind,
        ref_value="bitcoin",
        source_type=COINGECKO.source_type,
        decided_by="auto",
        actor="system",
    )
    _seed_security(
        db,
        security_id="s_renamed",
        name="Bitcoin",
        security_type="crypto",
        coingecko_id="bitcoin",
    )
    _hold(db, "s_renamed")
    SecurityLinksRepo(db).insert(
        security_id="s_renamed",
        ref_kind=COINGECKO.feed_ref_kind,
        ref_value="btc-legacy-slug",
        source_type=COINGECKO.source_type,
        decided_by="auto",
        actor="system",
    )

    result = _service(db, _FakeTiingo(), _FakeCoinGecko()).pull()

    assert _links(db) == [(COINGECKO.feed_ref_kind, "bitcoin", "s_holder")]
    assert [u.reason for u in result.unpriced] == ["feed_key_bound_elsewhere"]


def test_the_staging_model_retires_the_actor_this_service_writes() -> None:
    """One actor name, written in Python and matched in SQL, must not drift.

    ``_retire_stale_binding`` stamps ``reversed_by`` from ``_AUTO_REVERSAL``;
    ``prep.stg_security_prices`` decides whether a reversed link still resolves
    its earlier observations by comparing that column to a SQL literal. If the
    two stop agreeing, a renamed security's whole price history disappears from
    core and nothing fails — the join simply matches nothing, which is
    indistinguishable from having no history.
    """
    assert historical_reversal_actor() == _AUTO_REVERSAL


def test_a_non_finite_price_is_refused_as_a_usage_error(db: Database) -> None:
    """NaN must not reach the positivity test, which cannot survive it.

    ``Decimal("NaN") <= 0`` raises ``InvalidOperation``, so a finite check placed
    after that comparison is unreachable and NaN still escapes as an untyped
    arithmetic error. The CLI parses its own ``PRICE`` and refuses non-finite
    input before calling here; this is the service boundary saying the same thing
    for every other caller, and it only holds because it runs first.
    """
    _seed_security(db, security_id="s1", name="Private Co")
    service = _service(db, _FakeTiingo())

    for value in (Decimal("NaN"), Decimal("Infinity")):
        with pytest.raises(UserError) as caught:
            service.set_mark(
                "s1", date(2026, 6, 30), value, quote_currency="USD", note=None
            )
        assert caught.value.code == error_codes.INVESTMENT_PRICE_MARK_UNREPRESENTABLE
