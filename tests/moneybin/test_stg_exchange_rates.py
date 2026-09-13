"""prep.stg_exchange_rates normalizes raw.exchange_rates currency casing."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from moneybin.database import Database, sqlmesh_context

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def stg_exchange_rates_db(
    tmp_path_factory: pytest.TempPathFactory,
    request: pytest.FixtureRequest,
) -> Database:
    """One planned baseline with a mixed-case raw row."""
    secret_store = MagicMock()
    secret_store.get_key.return_value = "test-encryption-key-for-unit-tests"
    db = Database(
        tmp_path_factory.mktemp("stg_exchange_rates") / "test.duckdb",
        secret_store=secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    request.addfinalizer(db.close)

    db.execute(
        """
        INSERT INTO raw.exchange_rates
            (from_currency, to_currency, rate_date, rate, source_type, loaded_at)
        VALUES ('usd', 'eur', '2026-01-05'::DATE, 0.9000, 'frankfurter',
                '2026-01-05 09:00:00'::TIMESTAMP)
        """,  # test fixture, not executing user SQL
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    return db


@pytest.mark.slow
def test_currency_codes_are_upper_cased(stg_exchange_rates_db: Database) -> None:
    """A lower-case raw row reaches the staging view upper-cased."""
    row = stg_exchange_rates_db.execute(
        "SELECT from_currency, to_currency, rate_date, rate, source_type "
        "FROM prep.stg_exchange_rates"
    ).fetchone()
    assert row is not None
    from_currency, to_currency, rate_date, rate, source_type = row
    assert from_currency == "USD"
    assert to_currency == "EUR"
    assert str(rate_date) == "2026-01-05"
    assert source_type == "frankfurter"
    assert float(rate) == pytest.approx(0.9000)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
