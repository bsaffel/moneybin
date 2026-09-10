"""core.fct_exchange_rates_daily: the window-bounded, weekend-hopped spine.

Covers the Tier 1 cases §Testing Strategy names for the rate spine window
bounds, the healthy Friday trailing edge, interior carry-forward provenance,
identity rows, and grain integrity.
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


def _insert_ofx_account(db: Database, *, account_id: str) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_accounts
            (account_id, account_type, source_file, extracted_at,
             source_type, source_origin)
        VALUES (?, 'CHECKING', 'ofx_test', CURRENT_TIMESTAMP, 'ofx', 'test_bank')
        """,  # test fixture, not executing user SQL
        [account_id],
    )


def _insert_ofx_balance(
    db: Database,
    *,
    account_id: str,
    on_date: str,
    balance: str,
    currency_code: str,
) -> None:
    db.execute(
        """
        INSERT INTO raw.ofx_balances
            (account_id, statement_end_date, ledger_balance, ledger_balance_date,
             source_file, extracted_at, source_type, source_origin, currency_code)
        VALUES (?, ?::TIMESTAMP, ?::DECIMAL(18, 2), ?::TIMESTAMP, 'ofx_test',
                CURRENT_TIMESTAMP, 'ofx', 'test_bank', ?)
        """,  # test fixture, not executing user SQL
        [account_id, on_date, balance, on_date, currency_code],
    )


@pytest.fixture(scope="module")
def fct_exchange_rates_daily_db(
    tmp_path_factory: pytest.TempPathFactory,
    request: pytest.FixtureRequest,
) -> Database:
    """One planned baseline over every independent window/identity case."""
    secret_store = MagicMock()
    secret_store.get_key.return_value = "test-encryption-key-for-unit-tests"
    db = Database(
        tmp_path_factory.mktemp("fct_exchange_rates_daily") / "test.duckdb",
        secret_store=secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    request.addfinalizer(db.close)

    # Window bounds: a single Tuesday observation — no row before, no row on
    # the Wednesday after (a 14-day allowance is the regression this pins).
    _insert_provider(
        db,
        from_currency="USD",
        to_currency="AAA",
        rate_date="2026-01-06",
        rate="1.5000",
    )

    # Healthy trailing edge: last observation on a Friday.
    _insert_provider(
        db,
        from_currency="USD",
        to_currency="BBB",
        rate_date="2026-01-09",
        rate="2.5000",
    )

    # Interior carry-forward: Monday, then Thursday — a two-day gap (Tue/Wed).
    _insert_provider(
        db,
        from_currency="USD",
        to_currency="CCC",
        rate_date="2026-01-05",
        rate="1.0000",
    )
    _insert_provider(
        db,
        from_currency="USD",
        to_currency="CCC",
        rate_date="2026-01-08",
        rate="2.0000",
    )

    # Identity rows: an account denominated in a currency the provider never
    # quotes, so the only spine coverage for it is the identity arm. Two
    # observations (first/last) span Jan 1 - Jan 14 via fct_balances_daily's
    # own carry-forward.
    _insert_ofx_account(db, account_id="acct_identity")
    _insert_ofx_balance(
        db,
        account_id="acct_identity",
        on_date="2026-01-01",
        balance="100.00",
        currency_code="DDD",
    )
    _insert_ofx_balance(
        db,
        account_id="acct_identity",
        on_date="2026-01-14",
        balance="100.00",
        currency_code="DDD",
    )

    with sqlmesh_context(db) as ctx:
        ctx.plan(auto_apply=True, no_prompts=True)
    return db


@pytest.mark.slow
def test_no_row_before_the_first_observation(
    fct_exchange_rates_daily_db: Database,
) -> None:
    row = fct_exchange_rates_daily_db.execute(
        "SELECT 1 FROM core.fct_exchange_rates_daily "
        "WHERE from_currency = 'USD' AND to_currency = 'AAA' AND effective_date < '2026-01-06'"
    ).fetchone()
    assert row is None


@pytest.mark.slow
def test_no_row_on_the_weekday_after_a_tuesday_observation(
    fct_exchange_rates_daily_db: Database,
) -> None:
    """The regression this pins: a 14-day carry-forward allowance would fill this."""
    row = fct_exchange_rates_daily_db.execute(
        "SELECT 1 FROM core.fct_exchange_rates_daily "
        "WHERE from_currency = 'USD' AND to_currency = 'AAA' AND effective_date = '2026-01-07'"
    ).fetchone()
    assert row is None


@pytest.mark.slow
def test_a_friday_observation_prices_the_weekend(
    fct_exchange_rates_daily_db: Database,
) -> None:
    rows = fct_exchange_rates_daily_db.execute(
        "SELECT effective_date, published_date, rate, rate_source, days_since_published "
        "FROM core.fct_exchange_rates_daily "
        "WHERE from_currency = 'USD' AND to_currency = 'BBB' "
        "ORDER BY effective_date"
    ).fetchall()
    assert [str(r[0]) for r in rows] == ["2026-01-09", "2026-01-10", "2026-01-11"]
    for r in rows:
        assert str(r[1]) == "2026-01-09"
        assert float(r[2]) == pytest.approx(2.5000)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
        assert r[3] == "provider"
    assert [r[4] for r in rows] == [0, 1, 2]


@pytest.mark.slow
def test_no_row_on_the_monday_after_a_friday_observation(
    fct_exchange_rates_daily_db: Database,
) -> None:
    row = fct_exchange_rates_daily_db.execute(
        "SELECT 1 FROM core.fct_exchange_rates_daily "
        "WHERE from_currency = 'USD' AND to_currency = 'BBB' AND effective_date = '2026-01-12'"
    ).fetchone()
    assert row is None


@pytest.mark.slow
def test_interior_gap_carries_forward_with_provenance(
    fct_exchange_rates_daily_db: Database,
) -> None:
    rows = fct_exchange_rates_daily_db.execute(
        "SELECT effective_date, published_date, rate, days_since_published "
        "FROM core.fct_exchange_rates_daily "
        "WHERE from_currency = 'USD' AND to_currency = 'CCC' "
        "ORDER BY effective_date"
    ).fetchall()
    assert [str(r[0]) for r in rows] == [
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
        "2026-01-08",
    ]
    # Monday's own observation.
    assert str(rows[0][1]) == "2026-01-05"
    assert float(rows[0][2]) == pytest.approx(1.0000)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert rows[0][3] == 0
    # Interior Tuesday/Wednesday carry Monday's rate and publication day.
    for r in rows[1:3]:
        assert str(r[1]) == "2026-01-05"
        assert float(r[2]) == pytest.approx(1.0000)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert [r[3] for r in rows[1:3]] == [1, 2]
    # Thursday's own observation resets the hop.
    assert str(rows[3][1]) == "2026-01-08"
    assert float(rows[3][2]) == pytest.approx(2.0000)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
    assert rows[3][3] == 0


@pytest.mark.slow
def test_identity_rows_span_the_balance_spine_domain(
    fct_exchange_rates_daily_db: Database,
) -> None:
    rows = fct_exchange_rates_daily_db.execute(
        "SELECT effective_date, published_date, rate, rate_source, days_since_published "
        "FROM core.fct_exchange_rates_daily "
        "WHERE from_currency = 'DDD' AND to_currency = 'DDD' "
        "ORDER BY effective_date"
    ).fetchall()
    assert len(rows) == 14
    assert str(rows[0][0]) == "2026-01-01"
    assert str(rows[-1][0]) == "2026-01-14"
    for r in rows:
        assert r[0] == r[1]
        assert float(r[2]) == pytest.approx(1.0)  # type: ignore[reportUnknownArgumentType]  # pytest.approx stubs incomplete
        assert r[3] == "identity"
        assert r[4] == 0


@pytest.mark.slow
def test_grain_is_unique(fct_exchange_rates_daily_db: Database) -> None:
    row = fct_exchange_rates_daily_db.execute(
        "SELECT COUNT(*), COUNT(DISTINCT (from_currency, to_currency, effective_date)) "
        "FROM core.fct_exchange_rates_daily"
    ).fetchone()
    assert row is not None
    total, distinct = row
    assert total == distinct
