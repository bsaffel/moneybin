"""core.fct_exchange_rates_effective applies override precedence at read time.

Covers the Tier 1 cases §Testing Strategy names for live override precedence
(no `sqlmesh run` in between), an override carrying forward with the day it
corrects, an override on an otherwise-unpriced day, and parity with
`CurrencyService.resolve_rate`.
"""

from __future__ import annotations

import shutil
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database, sqlmesh_context
from moneybin.services.currency_service import (
    MAX_MARKET_CLOSURE_DAYS,
    CurrencyService,
    RateUnavailableError,
)

pytestmark = pytest.mark.integration


def _insert_provider(
    db: Database,
    *,
    from_currency: str,
    to_currency: str,
    rate_date: str,
    rate: str,
    loaded_at: str = "2026-01-05 09:00:00",
) -> None:
    db.execute(
        """
        INSERT INTO raw.exchange_rates
            (from_currency, to_currency, rate_date, rate, source_type, loaded_at)
        VALUES (?, ?, ?::DATE, ?, 'frankfurter', ?::TIMESTAMP)
        """,  # test fixture, not executing user SQL
        [from_currency, to_currency, rate_date, rate, loaded_at],
    )


def _insert_coverage(
    db: Database, *, from_currency: str, to_currency: str, start: str, end: str
) -> None:
    """Record a complete provider answer over a span, as a refresh would."""
    db.execute(
        """
        INSERT INTO raw.exchange_rate_coverage
            (from_currency, to_currency, start_date, end_date, source_type)
        VALUES (?, ?, ?::DATE, ?::DATE, 'frankfurter')
        """,  # test fixture, not executing user SQL
        [from_currency, to_currency, start, end],
    )


def _insert_override(
    db: Database,
    *,
    from_currency: str,
    to_currency: str,
    rate_date: str,
    rate: str,
) -> None:
    db.execute(
        """
        INSERT INTO app.exchange_rate_overrides
            (from_currency, to_currency, rate_date, rate, created_at, updated_at)
        VALUES (?, ?, ?::DATE, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,  # test fixture, not executing user SQL
        [from_currency, to_currency, rate_date, rate],
    )


@pytest.fixture(scope="module")
def effective_template(
    tmp_path_factory: pytest.TempPathFactory,
    request: pytest.FixtureRequest,
) -> Path:
    """One planned baseline: no overrides yet, so every test writes its own."""
    secret_store = MagicMock()
    secret_store.get_key.return_value = "test-encryption-key-for-unit-tests"
    db = Database(
        tmp_path_factory.mktemp("fct_exchange_rates_effective") / "test.duckdb",
        secret_store=secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    request.addfinalizer(db.close)

    # A continuous week — last observation on a Friday.
    for i, rate in enumerate(["0.900", "0.901", "0.902", "0.903", "0.904"]):
        day = 5 + i
        _insert_provider(
            db,
            from_currency="USD",
            to_currency="EEE",
            rate_date=f"2026-01-{day:02d}",
            rate=rate,
        )

    # Monday, then Thursday — a two-day interior gap.
    _insert_provider(
        db, from_currency="USD", to_currency="FFF", rate_date="2026-01-05", rate="1.000"
    )
    _insert_provider(
        db, from_currency="USD", to_currency="FFF", rate_date="2026-01-08", rate="2.000"
    )
    _insert_coverage(
        db, from_currency="USD", to_currency="FFF", start="2026-01-05", end="2026-01-08"
    )

    # Thursday, then the following Monday — no Friday quote, so the gap
    # Friday carries from Thursday and brackets a real weekend.
    _insert_provider(
        db, from_currency="USD", to_currency="GGG", rate_date="2026-01-08", rate="3.000"
    )
    _insert_provider(
        db, from_currency="USD", to_currency="GGG", rate_date="2026-01-12", rate="4.000"
    )
    _insert_coverage(
        db, from_currency="USD", to_currency="GGG", start="2026-01-08", end="2026-01-12"
    )

    # A real observation dated exactly on a Saturday -- no shipped adapter
    # writes one today, but the weekend-hop arm must not outrank it if one
    # ever does.
    _insert_provider(
        db, from_currency="USD", to_currency="HHH", rate_date="2026-01-10", rate="5.000"
    )

    # Monday, then the Monday a week later — exactly the widest closure.
    _insert_provider(
        db, from_currency="USD", to_currency="LLL", rate_date="2026-01-05", rate="8.000"
    )
    _insert_provider(
        db, from_currency="USD", to_currency="LLL", rate_date="2026-01-12", rate="9.000"
    )
    _insert_coverage(
        db, from_currency="USD", to_currency="LLL", start="2026-01-05", end="2026-01-12"
    )

    # Thursday, then the Friday eight days later — wider than a market closure.
    _insert_provider(
        db, from_currency="USD", to_currency="JJJ", rate_date="2026-01-15", rate="6.000"
    )
    _insert_provider(
        db, from_currency="USD", to_currency="JJJ", rate_date="2026-01-23", rate="7.000"
    )
    _insert_coverage(
        db, from_currency="USD", to_currency="JJJ", start="2026-01-15", end="2026-01-23"
    )

    # Monday and Wednesday from two separate lookups — no answer covers Tuesday.
    _insert_provider(
        db, from_currency="USD", to_currency="MMM", rate_date="2026-01-05", rate="1.500"
    )
    _insert_provider(
        db, from_currency="USD", to_currency="MMM", rate_date="2026-01-07", rate="1.600"
    )

    # GBP->USD: no provider coverage at all, for the uncovered-override case.

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    db.close()
    return db.path


@pytest.fixture()
def db(
    tmp_path: Path, request: pytest.FixtureRequest, effective_template: Path
) -> Database:
    """An isolated, writable copy of the shared planned baseline."""
    path = tmp_path / "test.duckdb"
    shutil.copy(effective_template, path)
    secret_store = MagicMock()
    secret_store.get_key.return_value = "test-encryption-key-for-unit-tests"
    result = Database(
        path,
        secret_store=secret_store,
        no_auto_upgrade=True,
        assume_initialized=True,
        read_only=False,
    )
    request.addfinalizer(result.close)
    return result


@pytest.mark.slow
def test_an_override_on_a_publication_day_is_live_with_no_replan(db: Database) -> None:
    """Writing an override changes the view on the next query — no `sqlmesh run`."""
    before = db.execute(
        "SELECT rate, rate_source, rate_vendor FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'EEE' AND effective_date = '2026-01-09'"
    ).fetchone()
    assert before is not None
    assert before[1] == "provider"
    assert before[2] == "frankfurter"

    _insert_override(
        db, from_currency="USD", to_currency="EEE", rate_date="2026-01-09", rate="0.999"
    )

    after = db.execute(
        "SELECT effective_date, published_date, rate, rate_source, rate_vendor, days_since_published "
        "FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'EEE' AND effective_date = '2026-01-09'"
    ).fetchone()
    assert after is not None
    assert str(after[1]) == "2026-01-09"
    assert float(after[2]) == pytest.approx(0.999)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert after[3] == "override"
    # The override supersedes the provider name too, not only the rate.
    assert after[4] is None
    assert after[5] == 0


@pytest.mark.slow
def test_an_override_on_the_friday_carries_to_the_weekend(db: Database) -> None:
    """Correcting the Friday quote changes Saturday/Sunday, which carry from it."""
    _insert_override(
        db, from_currency="USD", to_currency="EEE", rate_date="2026-01-09", rate="0.999"
    )

    rows = db.execute(
        "SELECT effective_date, published_date, rate, rate_source, days_since_published "
        "FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'EEE' AND effective_date >= '2026-01-09' "
        "ORDER BY effective_date"
    ).fetchall()
    assert [str(r[0]) for r in rows] == ["2026-01-09", "2026-01-10", "2026-01-11"]
    for r in rows:
        assert (
            str(r[1]) == "2026-01-09"
        )  # published_date keeps the hop it already recorded
        assert float(r[2]) == pytest.approx(0.999)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
        assert r[3] == "override"
    assert [r[4] for r in rows] == [0, 1, 2]


@pytest.mark.slow
def test_a_direct_override_wins_over_one_carried_from_the_prior_day(
    db: Database,
) -> None:
    """The two override arms can both match one row; the direct one wins.

    Correcting Friday's quote reaches Saturday only through carry-forward
    (rule 2, the `op` join on published_date). A second, different override
    filed directly on that Saturday (rule 1, the `oe` join on effective_date)
    must win over it — the documented precedence the CASE logic exists for.
    """
    _insert_override(
        db, from_currency="USD", to_currency="EEE", rate_date="2026-01-09", rate="0.850"
    )
    _insert_override(
        db, from_currency="USD", to_currency="EEE", rate_date="2026-01-10", rate="0.950"
    )

    row = db.execute(
        "SELECT rate, published_date, days_since_published "
        "FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'EEE' AND effective_date = '2026-01-10'"
    ).fetchone()
    assert row is not None
    # The direct Saturday override wins, not the Friday override it carries from.
    assert float(row[0]) == pytest.approx(0.950)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert str(row[1]) == "2026-01-10"
    assert row[2] == 0


@pytest.mark.slow
def test_an_override_before_an_interior_gap_changes_every_carried_day(
    db: Database,
) -> None:
    """Correcting Monday's quote changes the Tuesday/Wednesday it carries into."""
    _insert_override(
        db, from_currency="USD", to_currency="FFF", rate_date="2026-01-05", rate="1.500"
    )

    rows = db.execute(
        "SELECT effective_date, published_date, rate, rate_source, rate_vendor, days_since_published "
        "FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'FFF' "
        "ORDER BY effective_date"
    ).fetchall()
    assert [str(r[0]) for r in rows] == [
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
        "2026-01-08",
    ]
    # The corrected Monday and the two days carrying from it.
    for r in rows[:3]:
        assert str(r[1]) == "2026-01-05"
        assert float(r[2]) == pytest.approx(1.500)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
        assert r[3] == "override"
        assert r[4] is None
    assert [r[5] for r in rows[:3]] == [0, 1, 2]
    # Thursday's own (uncorrected) provider observation is untouched.
    assert float(rows[3][2]) == pytest.approx(2.000)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert rows[3][3] == "provider"
    assert rows[3][4] == "frankfurter"


@pytest.mark.slow
def test_an_override_on_an_interior_gap_day_does_not_forward_fill(
    db: Database,
) -> None:
    """Rule 1 is exact-day only — it does not become a new carry-forward anchor.

    Correcting Monday's *publication* (rule 2, tested above) cascades through
    the whole gap it carries into. A direct override on Tuesday — itself an
    interior day carried from Monday, not a publication day — is a fact about
    Tuesday alone: rule 1 gives it `published_date = effective_date` and stops
    there. Wednesday still carries Monday's unmodified provider quote. Treating
    Tuesday's correction as also correcting Wednesday would infer a claim the
    user never made — the same Requirement 5 (never manufacture a rate) that
    keeps rule 3 from interior-filling between two standalone overrides.
    """
    _insert_override(
        db, from_currency="USD", to_currency="FFF", rate_date="2026-01-06", rate="9.999"
    )

    rows = db.execute(
        "SELECT effective_date, published_date, rate, rate_source "
        "FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'FFF' ORDER BY effective_date"
    ).fetchall()
    assert [str(r[0]) for r in rows] == [
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
        "2026-01-08",
    ]
    # Tuesday: the direct override, scoped to itself.
    assert str(rows[1][1]) == "2026-01-06"
    assert float(rows[1][2]) == pytest.approx(9.999)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert rows[1][3] == "override"
    # Wednesday: still carrying Monday's provider quote, untouched by Tuesday's
    # correction.
    assert str(rows[2][1]) == "2026-01-05"
    assert float(rows[2][2]) == pytest.approx(1.000)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert rows[2][3] == "provider"


@pytest.mark.slow
def test_an_override_on_an_unpriced_pair_is_still_visible(db: Database) -> None:
    """A correction for a pair the provider never priced still produces a row."""
    _insert_override(
        db,
        from_currency="GBP",
        to_currency="USD",
        rate_date="2026-01-07",
        rate="1.2500",
    )

    row = db.execute(
        "SELECT effective_date, published_date, rate, rate_source, rate_vendor, days_since_published "
        "FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'GBP' AND to_currency = 'USD' AND effective_date = '2026-01-07'"
    ).fetchone()
    assert row is not None
    assert str(row[1]) == "2026-01-07"
    assert float(row[2]) == pytest.approx(1.2500)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert row[3] == "override"
    assert row[4] is None
    assert row[5] == 0


@pytest.mark.slow
def test_parity_with_resolve_rate_on_a_publication_day(db: Database) -> None:
    """The exact-day lookup `_stored_rate` performs, cross-checked against the view."""
    service = CurrencyService(db, adapter=None)
    resolved = service.resolve_rate("USD", "EEE", date(2026, 1, 7))

    row = db.execute(
        "SELECT rate FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'EEE' AND effective_date = '2026-01-07'"
    ).fetchone()
    assert row is not None
    assert Decimal(str(row[0])) == resolved.rate


@pytest.mark.slow
def test_parity_with_resolve_rate_on_a_weekend_date(db: Database) -> None:
    """The `_last_publication_day` fallback `_stored_rate` performs on a Saturday."""
    service = CurrencyService(db, adapter=None)
    resolved = service.resolve_rate("USD", "EEE", date(2026, 1, 10))

    row = db.execute(
        "SELECT rate FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'EEE' AND effective_date = '2026-01-10'"
    ).fetchone()
    assert row is not None
    assert Decimal(str(row[0])) == resolved.rate


@pytest.mark.slow
def test_parity_with_resolve_rate_across_an_interior_closure(db: Database) -> None:
    """The spine and the cache-only `resolve_rate` price a closure identically.

    Monday and Thursday bracket Tuesday three days apart, inside
    `MAX_MARKET_CLOSURE_DAYS`, so both read it as a closed market and price it
    at Monday's publication. This was a named divergence until the offline
    closure rule closed it; a parity failure here means the two rules drifted.
    """
    row = db.execute(
        "SELECT rate, rate_source, published_date, days_since_published "
        "FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'FFF' AND effective_date = '2026-01-06'"
    ).fetchone()
    assert row is not None
    assert row[1] == "provider"
    assert str(row[2]) == "2026-01-05"
    assert row[3] == 1

    resolved = CurrencyService(db, adapter=None).resolve_rate(
        "USD", "FFF", date(2026, 1, 6)
    )
    assert resolved.rate == Decimal(str(row[0]))
    assert resolved.rate_date == date(2026, 1, 5)


@pytest.mark.slow
def test_an_unfetched_day_between_separate_lookups_is_unpriced_on_both_surfaces(
    db: Database,
) -> None:
    """Monday and Wednesday rows with no answer covering Tuesday prove nothing.

    Tuesday may have been published and never asked for, so neither the spine
    nor the cache-only `resolve_rate` prices it from Monday.
    """
    rows = db.execute(
        "SELECT effective_date FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'MMM' ORDER BY effective_date"
    ).fetchall()
    assert [str(r[0]) for r in rows] == ["2026-01-05", "2026-01-07"]

    with pytest.raises(RateUnavailableError):
        CurrencyService(db, adapter=None).resolve_rate("USD", "MMM", date(2026, 1, 6))


@pytest.mark.slow
def test_the_widest_closure_is_priced_on_both_surfaces(db: Database) -> None:
    """Seven days between publications is the bound on both sides of the seam.

    The SQL spine hard-codes the number `MAX_MARKET_CLOSURE_DAYS` holds; this
    and the eight-day case below pin the two to the same value.
    """
    assert MAX_MARKET_CLOSURE_DAYS == 7
    row = db.execute(
        "SELECT rate, published_date FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'LLL' AND effective_date = '2026-01-08'"
    ).fetchone()
    assert row is not None
    assert str(row[1]) == "2026-01-05"

    resolved = CurrencyService(db, adapter=None).resolve_rate(
        "USD", "LLL", date(2026, 1, 8)
    )
    assert resolved.rate == Decimal(str(row[0]))
    assert resolved.rate_date == date(2026, 1, 5)


@pytest.mark.slow
def test_a_gap_wider_than_a_closure_is_unpriced_on_both_surfaces(db: Database) -> None:
    """Eight days between publications: no spine row, and `resolve_rate` raises.

    Thursday 2026-01-15 to Friday 2026-01-23. Saturday the 17th still carries
    nothing: its calendar Friday (the 16th) was never published, so the weekend
    hop has nothing to reach either.
    """
    rows = db.execute(
        "SELECT effective_date FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'JJJ' ORDER BY effective_date"
    ).fetchall()
    assert [str(r[0]) for r in rows] == [
        "2026-01-15",
        "2026-01-23",
        "2026-01-24",
        "2026-01-25",
    ]

    service = CurrencyService(db, adapter=None)
    for day in (date(2026, 1, 16), date(2026, 1, 17), date(2026, 1, 20)):
        with pytest.raises(RateUnavailableError):
            service.resolve_rate("USD", "JJJ", day)


@pytest.mark.slow
def test_grain_stays_unique_with_overlapping_uncovered_overrides(db: Database) -> None:
    """Two uncovered overrides on one pair can collide on carry-forward.

    The direct hit wins rather than producing two rows for one grain key.
    """
    _insert_override(
        db,
        from_currency="GBP",
        to_currency="USD",
        rate_date="2026-01-09",
        rate="1.2000",
    )
    _insert_override(
        db,
        from_currency="GBP",
        to_currency="USD",
        rate_date="2026-01-10",
        rate="1.3000",
    )

    rows = db.execute(
        "SELECT effective_date, rate, days_since_published FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'GBP' AND to_currency = 'USD' ORDER BY effective_date"
    ).fetchall()
    assert [str(r[0]) for r in rows] == ["2026-01-09", "2026-01-10", "2026-01-11"]
    # The direct Saturday override wins over the Friday override's carry-forward.
    assert float(rows[1][1]) == pytest.approx(1.3000)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert rows[1][2] == 0

    row = db.execute(
        "SELECT COUNT(*), COUNT(DISTINCT (from_currency, to_currency, effective_date)) "
        "FROM core.fct_exchange_rates_effective"
    ).fetchone()
    assert row is not None
    total, distinct = row
    assert total == distinct


@pytest.mark.slow
def test_an_override_on_a_gap_friday_carries_into_the_weekend(db: Database) -> None:
    """A Friday override reaches the weekend even when Friday has no provider quote.

    Bracket: a provider observation Thursday, no Friday quote, a provider
    observation the following Monday — Friday is an ordinary interior gap day,
    carrying Thursday's rate forward, exactly like the Tuesday/Wednesday case
    `test_an_override_on_an_interior_gap_day_does_not_forward_fill` pins. The
    difference is the weekend hop: `CurrencyService.resolve_rate` maps Saturday
    and Sunday back to the calendar Friday and checks `_stored_rate` there
    override-first, regardless of whether Friday itself was ever published.
    A Friday override is therefore live for the weekend `resolve_rate` already
    maps to it, not a manufactured rate for a day the user never priced.

    Measured directly against `resolve_rate`, not asserted from reading the
    SQL alone.
    """
    _insert_override(
        db, from_currency="USD", to_currency="GGG", rate_date="2026-01-09", rate="3.500"
    )

    rows = db.execute(
        "SELECT effective_date, published_date, rate, rate_source, days_since_published "
        "FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'GGG' ORDER BY effective_date"
    ).fetchall()
    assert [str(r[0]) for r in rows] == [
        "2026-01-08",
        "2026-01-09",
        "2026-01-10",
        "2026-01-11",
        "2026-01-12",
    ]
    # Thursday: untouched provider observation.
    assert rows[0][3] == "provider"
    # Friday: the direct override (rule 1).
    assert str(rows[1][1]) == "2026-01-09"
    assert float(rows[1][2]) == pytest.approx(3.500)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert rows[1][3] == "override"
    assert rows[1][4] == 0
    # Saturday and Sunday: the weekend-hop arm (rule 2) reaches the Friday
    # override even though Friday was never a provider publication day.
    for row, expected_gap in zip(rows[2:4], (1, 2), strict=True):
        assert str(row[1]) == "2026-01-09"
        assert float(row[2]) == pytest.approx(3.500)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
        assert row[3] == "override"
        assert row[4] == expected_gap
    # Monday: untouched provider observation.
    assert rows[4][3] == "provider"

    service = CurrencyService(db, adapter=None)
    for day, sql_row in zip(
        (date(2026, 1, 10), date(2026, 1, 11)), rows[2:4], strict=True
    ):
        resolved = service.resolve_rate("USD", "GGG", day)
        assert resolved.source == "override"
        assert resolved.rate == Decimal(str(sql_row[2]))
        assert resolved.rate_date == date(2026, 1, 9)


@pytest.mark.slow
def test_an_exact_weekend_observation_outranks_a_friday_override(
    db: Database,
) -> None:
    """A real same-day weekend publication wins over the calendar-Friday hop.

    USD/HHH carries a genuine provider observation dated exactly on a
    Saturday (2026-01-10) -- no shipped adapter writes one today, but the
    weekend-hop arm (rule 2) must not manufacture a wrong answer if one ever
    does. `_stored_rate(on)` checks the exact requested day BEFORE ever
    falling back to `_last_publication_day`'s Friday lookup, so an override
    on the preceding Friday must not outrank a real Saturday publication.

    Measured directly against `resolve_rate`, not asserted from reading the
    SQL alone.
    """
    _insert_override(
        db, from_currency="USD", to_currency="HHH", rate_date="2026-01-09", rate="9.999"
    )

    row = db.execute(
        "SELECT effective_date, published_date, rate, rate_source, rate_vendor "
        "FROM core.fct_exchange_rates_effective "
        "WHERE from_currency = 'USD' AND to_currency = 'HHH' AND effective_date = '2026-01-10'"
    ).fetchone()
    assert row is not None
    assert str(row[1]) == "2026-01-10"
    assert float(row[2]) == pytest.approx(5.000)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert row[3] == "provider"
    assert row[4] == "frankfurter"

    service = CurrencyService(db, adapter=None)
    resolved = service.resolve_rate("USD", "HHH", date(2026, 1, 10))
    assert resolved.source == "frankfurter"
    assert resolved.rate == Decimal(str(row[2]))
    assert resolved.rate_date == date(2026, 1, 10)
