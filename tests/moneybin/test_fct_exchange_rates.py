"""core.fct_exchange_rates resolves override-vs-provider precedence.

Observation grain (from_currency, to_currency, rate_date).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration


def _insert_provider(
    db: Database,
    *,
    from_currency: str,
    to_currency: str,
    rate_date: str,
    rate: str,
    source_type: str = "frankfurter",
    loaded_at: str = "2026-01-05 09:00:00",
) -> None:
    db.execute(
        """
        INSERT INTO raw.exchange_rates
            (from_currency, to_currency, rate_date, rate, source_type, loaded_at)
        VALUES (?, ?, ?::DATE, ?, ?, ?::TIMESTAMP)
        """,  # test fixture, not executing user SQL
        [from_currency, to_currency, rate_date, rate, source_type, loaded_at],
    )


def _insert_override(
    db: Database,
    *,
    from_currency: str,
    to_currency: str,
    rate_date: str,
    rate: str,
    updated_at: str = "2026-01-05 12:00:00",
) -> None:
    db.execute(
        """
        INSERT INTO app.exchange_rate_overrides
            (from_currency, to_currency, rate_date, rate, created_at, updated_at)
        VALUES (?, ?, ?::DATE, ?, ?::TIMESTAMP, ?::TIMESTAMP)
        """,  # test fixture, not executing user SQL
        [from_currency, to_currency, rate_date, rate, updated_at, updated_at],
    )


@pytest.fixture(scope="module")
def fct_exchange_rates_db(
    tmp_path_factory: pytest.TempPathFactory,
    request: pytest.FixtureRequest,
) -> Database:
    """One planned baseline covering every precedence case."""
    secret_store = MagicMock()
    secret_store.get_key.return_value = "test-encryption-key-for-unit-tests"
    db = Database(
        tmp_path_factory.mktemp("fct_exchange_rates") / "test.duckdb",
        secret_store=secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    request.addfinalizer(db.close)

    # Provider-only day: no override in play.
    _insert_provider(
        db,
        from_currency="USD",
        to_currency="AAA",
        rate_date="2026-01-05",
        rate="1.1000",
    )

    # Override outranks a provider row filed for the same pair and date.
    _insert_provider(
        db,
        from_currency="USD",
        to_currency="BBB",
        rate_date="2026-01-05",
        rate="2.0000",
    )
    _insert_override(
        db,
        from_currency="USD",
        to_currency="BBB",
        rate_date="2026-01-05",
        rate="2.5000",
    )

    # Two providers on the same day: freshest loaded_at wins.
    _insert_provider(
        db,
        from_currency="USD",
        to_currency="CCC",
        rate_date="2026-01-05",
        rate="3.0000",
        source_type="frankfurter",
        loaded_at="2026-01-05 09:00:00",
    )
    _insert_provider(
        db,
        from_currency="USD",
        to_currency="CCC",
        rate_date="2026-01-05",
        rate="3.1000",
        source_type="exchangerate_host",
        loaded_at="2026-01-05 10:00:00",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    return db


@pytest.mark.slow
def test_a_provider_row_alone_resolves(fct_exchange_rates_db: Database) -> None:
    row = fct_exchange_rates_db.execute(
        "SELECT rate, rate_source, provider FROM core.fct_exchange_rates "
        "WHERE from_currency = 'USD' AND to_currency = 'AAA'"
    ).fetchone()
    assert row is not None
    assert float(row[0]) == pytest.approx(1.1000)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    # rate_source is the closed provider/identity/override vocabulary shared
    # across all three rate models; provider carries the named feed.
    assert row[1] == "provider"
    assert row[2] == "frankfurter"


@pytest.mark.slow
def test_an_override_outranks_a_provider_row_on_the_same_date(
    fct_exchange_rates_db: Database,
) -> None:
    rows = fct_exchange_rates_db.execute(
        "SELECT rate, rate_source, provider FROM core.fct_exchange_rates "
        "WHERE from_currency = 'USD' AND to_currency = 'BBB'"
    ).fetchall()
    assert len(rows) == 1
    rate, source, provider = rows[0]
    assert float(rate) == pytest.approx(2.5000)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert source == "override"
    # An override is user-authored, not sourced from a named feed.
    assert provider is None


@pytest.mark.slow
def test_two_providers_on_one_day_resolve_by_freshest_write(
    fct_exchange_rates_db: Database,
) -> None:
    rows = fct_exchange_rates_db.execute(
        "SELECT rate, rate_source, provider FROM core.fct_exchange_rates "
        "WHERE from_currency = 'USD' AND to_currency = 'CCC'"
    ).fetchall()
    assert len(rows) == 1
    rate, source, provider = rows[0]
    assert float(rate) == pytest.approx(3.1000)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert source == "provider"
    assert provider == "exchangerate_host"


@pytest.mark.slow
def test_grain_is_unique(fct_exchange_rates_db: Database) -> None:
    row = fct_exchange_rates_db.execute(
        "SELECT COUNT(*), COUNT(DISTINCT (from_currency, to_currency, rate_date)) "
        "FROM core.fct_exchange_rates"
    ).fetchone()
    assert row is not None
    total, distinct = row
    assert total == distinct
