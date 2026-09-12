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
from moneybin.services.currency_service import CurrencyService

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
