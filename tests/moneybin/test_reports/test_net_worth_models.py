"""Live execution tests for the account-grain net-worth SQL view.

``_install_net_worth_sources`` and the ``_balance`` / ``_account`` / ``_rate``
/ ``_home`` helpers below are reused by later tasks in the net-worth SQL rungs
plan (currency and top-level rungs), so keep them general.
"""

from __future__ import annotations

import re
from collections.abc import Generator
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import SQLMESH_ROOT, Database

_REPORT_MODELS = SQLMESH_ROOT / "models" / "reports"

_COLUMNS = (
    "account_id",
    "account_name",
    "currency_code",
    "home_currency_code",
    "account_type",
    "is_observed",
    "observation_source",
    "rate_source",
    "balance_date",
    "rate_published_date",
    "days_since_observed",
    "reconciliation_delta",
    "account_balance",
    "account_balance_home",
)


_CURRENCIES_COLUMNS = (
    "currency_code",
    "home_currency_code",
    "balance_date",
    "rate_published_date",
    "rate_source",
    "account_count",
    "carried_forward_count",
    "total_assets",
    "total_liabilities",
    "net_worth",
    "total_assets_home",
    "total_liabilities_home",
    "net_worth_home",
)


_DAY_COLUMNS = (
    "home_currency_code",
    "balance_date",
    "account_count",
    "carried_forward_count",
    "currency_count",
    "unpriced_currency_count",
    "total_assets",
    "total_liabilities",
    "net_worth",
)


def _model_body(name: str) -> str:
    """Return a report model's executable SQL without its MODEL header."""
    raw = (_REPORT_MODELS / f"{name}.sql").read_text()
    return re.sub(
        r"^.*?MODEL\s*\(.*?\);\s*",
        "",
        raw,
        count=1,
        flags=re.DOTALL,
    ).strip()


def _install_report(db: Database, name: str) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute(  # test-selected shipped model name
        f"CREATE OR REPLACE VIEW reports.{name} AS {_model_body(name)}"
    )


@pytest.fixture()
def model_db(
    tmp_path: Path, mock_secret_store: MagicMock
) -> Generator[Database, None, None]:
    database = Database(
        tmp_path / "report-model.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    yield database
    database.close()


def _install_net_worth_sources(db: Database) -> None:
    """Create the account-grain rung's core.* stub tables.

    ``app.profile_settings`` already exists (empty) via ``init_schemas`` — no
    row means no home currency chosen, which is the default state ``_home``
    below opts into.
    """
    db.execute("""
        CREATE TABLE core.fct_balances_daily (
            account_id VARCHAR,
            balance_date DATE,
            balance DECIMAL(18, 2),
            is_observed BOOLEAN,
            observation_source VARCHAR,
            reconciliation_delta DECIMAL(18, 2),
            currency_code VARCHAR
        )
    """)
    db.execute("""
        CREATE TABLE core.dim_accounts (
            account_id VARCHAR,
            display_name VARCHAR,
            account_type VARCHAR,
            currency_code VARCHAR,
            archived BOOLEAN,
            include_in_net_worth BOOLEAN,
            archived_at DATE
        )
    """)
    db.execute("""
        CREATE TABLE core.fct_exchange_rates_effective (
            from_currency VARCHAR,
            to_currency VARCHAR,
            rate_source VARCHAR,
            rate_vendor VARCHAR,
            rate DECIMAL(18, 8),
            days_since_published INTEGER,
            effective_date DATE,
            published_date DATE
        )
    """)


def _balance(
    db: Database,
    account_id: str,
    day: str,
    balance: str,
    currency: str | None,
    *,
    observed: bool = True,
    source: str = "ofx",
) -> None:
    """Insert one core.fct_balances_daily row."""
    db.execute(
        """
        INSERT INTO core.fct_balances_daily
            (account_id, balance_date, balance, is_observed, observation_source,
             reconciliation_delta, currency_code)
        VALUES (?, ?, ?, ?, ?, NULL, ?)
        """,
        [
            account_id,
            day,
            Decimal(balance),
            observed,
            source if observed else None,
            currency,
        ],
    )


def _account(
    db: Database,
    account_id: str,
    name: str,
    currency: str | None,
    *,
    account_type: str = "depository",
    include: bool = True,
    archived: bool = False,
    archived_at: str | None = None,
) -> None:
    """Insert one core.dim_accounts row."""
    db.execute(
        """
        INSERT INTO core.dim_accounts
            (account_id, display_name, account_type, currency_code, archived,
             include_in_net_worth, archived_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [account_id, name, account_type, currency, archived, include, archived_at],
    )


def _rate(
    db: Database,
    from_ccy: str,
    to_ccy: str,
    day: str,
    rate: str,
    *,
    source: str = "provider",
    published: str | None = None,
) -> None:
    """Insert one core.fct_exchange_rates_effective row."""
    published_date = published or day
    days_since_published = (
        date.fromisoformat(day) - date.fromisoformat(published_date)
    ).days
    db.execute(
        """
        INSERT INTO core.fct_exchange_rates_effective
            (from_currency, to_currency, rate_source, rate_vendor, rate,
             days_since_published, effective_date, published_date)
        VALUES (?, ?, ?, NULL, ?, ?, ?, ?)
        """,
        [
            from_ccy,
            to_ccy,
            source,
            Decimal(rate),
            days_since_published,
            day,
            published_date,
        ],
    )


def _home(db: Database, currency: str) -> None:
    """Insert the singleton app.profile_settings row with a chosen home currency."""
    db.execute(
        "INSERT INTO app.profile_settings (home_currency) VALUES (?)",
        [currency],
    )


def test_accounts_rung_converts_to_home_at_the_effective_rate(
    model_db: Database,
) -> None:
    """A EUR balance converts to USD at the provider rate effective that day.

    ``published`` is a distinct, earlier date from ``balance_date`` so the
    assertion cannot pass by coincidence of the two dates matching.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Euro Checking", "EUR")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "EUR")
    _home(model_db, "USD")
    _rate(model_db, "EUR", "USD", "2026-01-05", "1.10", published="2026-01-02")
    _install_report(model_db, "net_worth_accounts")

    row = model_db.execute(
        """
        SELECT account_balance_home, rate_source, rate_published_date
        FROM reports.net_worth_accounts
        WHERE account_id = 'acct-a' AND balance_date = '2026-01-05'
        """
    ).fetchone()
    assert row == (Decimal("110.00"), "provider", date(2026, 1, 2))


def test_accounts_rung_identity_rate_prices_a_home_currency_account(
    model_db: Database,
) -> None:
    """A USD account priced against an identity USD→USD row equals itself."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _balance(model_db, "acct-a", "2026-01-05", "250.00", "USD")
    _home(model_db, "USD")
    _rate(model_db, "USD", "USD", "2026-01-05", "1.0", source="identity")
    _install_report(model_db, "net_worth_accounts")

    row = model_db.execute(
        """
        SELECT account_balance, account_balance_home, rate_source
        FROM reports.net_worth_accounts
        WHERE account_id = 'acct-a'
        """
    ).fetchone()
    assert row == (Decimal("250.00"), Decimal("250.00"), "identity")


def test_accounts_rung_leaves_an_unpriced_pair_null_not_zero(
    model_db: Database,
) -> None:
    """A currency pair with no rate row is unpriced, not zero, and stays visible."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Euro Checking", "EUR")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "EUR")
    _home(model_db, "USD")
    _install_report(model_db, "net_worth_accounts")

    row = model_db.execute(
        """
        SELECT account_balance_home, rate_source
        FROM reports.net_worth_accounts
        WHERE account_id = 'acct-a'
        """
    ).fetchone()
    assert row is not None
    assert row == (None, None)


def test_accounts_rung_unknown_currency_is_never_priced(model_db: Database) -> None:
    """A NULL currency_code account is never converted, even with a home currency."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Mystery", None)
    _balance(model_db, "acct-a", "2026-01-05", "100.00", None)
    _home(model_db, "USD")
    _install_report(model_db, "net_worth_accounts")

    row = model_db.execute(
        """
        SELECT account_balance_home
        FROM reports.net_worth_accounts
        WHERE account_id = 'acct-a'
        """
    ).fetchone()
    assert row == (None,)


def test_accounts_rung_null_home_currency_prices_nothing(model_db: Database) -> None:
    """No app.profile_settings row means every account is unpriced.

    A USD→USD identity rate exists for the test date, so a mutation that
    defaulted the missing home currency to 'USD' (e.g.
    ``COALESCE(h.home_currency_code, 'USD')``) would find it and price the
    account — the NULL result here can only come from the missing home
    currency itself, not from an absent rate row.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "USD")
    _rate(model_db, "USD", "USD", "2026-01-05", "1.0", source="identity")
    _install_report(model_db, "net_worth_accounts")

    row = model_db.execute(
        """
        SELECT account_balance_home, home_currency_code
        FROM reports.net_worth_accounts
        WHERE account_id = 'acct-a'
        """
    ).fetchone()
    assert row == (None, None)


def test_accounts_rung_counts_days_since_the_last_observation(
    model_db: Database,
) -> None:
    """A balance observed on D and carried forward D+1, D+2 counts age from D."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "USD", observed=True)
    _balance(model_db, "acct-a", "2026-01-06", "100.00", "USD", observed=False)
    _balance(model_db, "acct-a", "2026-01-07", "100.00", "USD", observed=False)
    _install_report(model_db, "net_worth_accounts")

    rows = model_db.execute(
        """
        SELECT balance_date, days_since_observed, is_observed
        FROM reports.net_worth_accounts
        WHERE account_id = 'acct-a'
        ORDER BY balance_date
        """
    ).fetchall()
    assert rows == [
        (date(2026, 1, 5), 0, True),
        (date(2026, 1, 6), 1, False),
        (date(2026, 1, 7), 2, False),
    ]


def test_accounts_rung_excludes_an_archived_account_only_after_its_archive_date(
    model_db: Database,
) -> None:
    """An archived account still contributes rows up to and including archived_at."""
    _install_net_worth_sources(model_db)
    _account(
        model_db,
        "acct-a",
        "Checking",
        "USD",
        archived=True,
        archived_at="2026-01-06",
    )
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "USD")
    _balance(model_db, "acct-a", "2026-01-06", "100.00", "USD")
    _balance(model_db, "acct-a", "2026-01-07", "100.00", "USD")
    _install_report(model_db, "net_worth_accounts")

    dates = model_db.execute(
        """
        SELECT balance_date
        FROM reports.net_worth_accounts
        WHERE account_id = 'acct-a'
        ORDER BY balance_date
        """
    ).fetchall()
    assert dates == [(date(2026, 1, 5),), (date(2026, 1, 6),)]


def test_accounts_rung_excludes_an_account_left_out_of_net_worth(
    model_db: Database,
) -> None:
    """include_in_net_worth = FALSE drops every row for that account."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD", include=False)
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "USD")
    _install_report(model_db, "net_worth_accounts")

    rows = model_db.execute(
        "SELECT * FROM reports.net_worth_accounts WHERE account_id = 'acct-a'"
    ).fetchall()
    assert rows == []


def test_accounts_rung_override_applies_without_a_rebuild(model_db: Database) -> None:
    """The rung reads rate_source/rate straight from fct_exchange_rates_effective.

    The stub effective table here is a plain table, so this only pins the
    read-through: the view applies whatever that table already resolved to,
    override included. The live-override guarantee (a correction taking effect
    with no sqlmesh run in between) is Task 8's end-to-end scenario.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Euro Checking", "EUR")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "EUR")
    _home(model_db, "USD")
    _rate(model_db, "EUR", "USD", "2026-01-05", "1.25", source="override")
    _install_report(model_db, "net_worth_accounts")

    row = model_db.execute(
        """
        SELECT account_balance_home, rate_source
        FROM reports.net_worth_accounts
        WHERE account_id = 'acct-a'
        """
    ).fetchone()
    assert row == (Decimal("125.00"), "override")


def test_accounts_rung_grain_is_unique(model_db: Database) -> None:
    """Two accounts across three days yield exactly six unique grain rows."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking A", "USD")
    _account(model_db, "acct-b", "Checking B", "USD")
    for day in ("2026-01-05", "2026-01-06", "2026-01-07"):
        _balance(model_db, "acct-a", day, "100.00", "USD")
        _balance(model_db, "acct-b", day, "200.00", "USD")
    _install_report(model_db, "net_worth_accounts")

    row = model_db.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT (account_id, balance_date))
        FROM reports.net_worth_accounts
        """
    ).fetchone()
    assert row == (6, 6)


def test_accounts_rung_rate_join_binds_to_the_same_balance_date(
    model_db: Database,
) -> None:
    """The rate join binds on effective_date, not merely the currency pair.

    Two exchange rate rows for the same pair on different days must not
    cross-multiply a spine row into two: each balance_date's own rate wins.
    A join that dropped ``effective_date`` would match both rate rows against
    every spine row for this pair, producing four rows instead of two.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Euro Checking", "EUR")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "EUR")
    _balance(model_db, "acct-a", "2026-01-06", "100.00", "EUR")
    _home(model_db, "USD")
    _rate(model_db, "EUR", "USD", "2026-01-05", "1.10")
    _rate(model_db, "EUR", "USD", "2026-01-06", "1.20")
    _install_report(model_db, "net_worth_accounts")

    rows = model_db.execute(
        """
        SELECT balance_date, account_balance_home
        FROM reports.net_worth_accounts
        WHERE account_id = 'acct-a'
        ORDER BY balance_date
        """
    ).fetchall()
    assert rows == [
        (date(2026, 1, 5), Decimal("110.00")),
        (date(2026, 1, 6), Decimal("120.00")),
    ]


def test_accounts_rung_projects_the_declared_column_order(model_db: Database) -> None:
    """The SELECT projects exactly the Interfaces column order."""
    _install_net_worth_sources(model_db)
    _install_report(model_db, "net_worth_accounts")

    cursor = model_db.execute("SELECT * FROM reports.net_worth_accounts LIMIT 0")
    columns = [column[0] for column in cursor.description]
    assert columns == list(_COLUMNS)


def test_currencies_rung_keeps_original_totals_and_adds_home_totals(
    model_db: Database,
) -> None:
    """Two EUR accounts sum in EUR, then convert to USD at the day's rate."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Euro Checking", "EUR")
    _balance(model_db, "acct-a", "2026-01-05", "200.00", "EUR")
    _account(model_db, "acct-b", "Euro Credit", "EUR")
    _balance(model_db, "acct-b", "2026-01-05", "-50.00", "EUR")
    _home(model_db, "USD")
    _rate(model_db, "EUR", "USD", "2026-01-05", "1.10")
    _install_report(model_db, "net_worth_currencies")

    row = model_db.execute(
        """
        SELECT total_assets, total_liabilities, net_worth,
               total_assets_home, total_liabilities_home, net_worth_home
        FROM reports.net_worth_currencies
        WHERE currency_code = 'EUR' AND balance_date = '2026-01-05'
        """
    ).fetchone()
    assert row == (
        Decimal("200.00"),
        Decimal("-50.00"),
        Decimal("150.00"),
        Decimal("220.00"),
        Decimal("-55.00"),
        Decimal("165.00"),
    )


def test_currencies_rung_home_columns_add_up_under_rounding(
    model_db: Database,
) -> None:
    """net_worth_home is the sum of the two converted components, not a re-rounded conversion of net_worth itself.

    Converting the pooled ``net_worth`` (0.99) at 0.5 rounds to 0.50 — one
    cent more than the correct 0.49 the components sum to — so this test
    fails if ``net_worth_home`` is computed as ``ROUND(net_worth * rate, 2)``
    instead of ``total_assets_home + total_liabilities_home``.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Euro Checking", "EUR")
    _balance(model_db, "acct-a", "2026-01-05", "1.00", "EUR")
    _account(model_db, "acct-b", "Euro Credit", "EUR")
    _balance(model_db, "acct-b", "2026-01-05", "-0.01", "EUR")
    _home(model_db, "USD")
    _rate(model_db, "EUR", "USD", "2026-01-05", "0.5")
    _install_report(model_db, "net_worth_currencies")

    row = model_db.execute(
        """
        SELECT total_assets_home, total_liabilities_home, net_worth_home
        FROM reports.net_worth_currencies
        WHERE currency_code = 'EUR' AND balance_date = '2026-01-05'
        """
    ).fetchone()
    assert row is not None
    total_assets_home, total_liabilities_home, net_worth_home = row
    assert row == (Decimal("0.50"), Decimal("-0.01"), Decimal("0.49"))
    assert net_worth_home == total_assets_home + total_liabilities_home


def test_currencies_rung_unpriced_currency_keeps_its_segment(
    model_db: Database,
) -> None:
    """A currency with no rate row still totals in its own unit; conversion is NULL."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Euro Checking", "EUR")
    _balance(model_db, "acct-a", "2026-01-05", "200.00", "EUR")
    _account(model_db, "acct-b", "Euro Credit", "EUR")
    _balance(model_db, "acct-b", "2026-01-05", "-50.00", "EUR")
    _home(model_db, "USD")
    _install_report(model_db, "net_worth_currencies")

    row = model_db.execute(
        """
        SELECT total_assets, total_liabilities, net_worth,
               total_assets_home, total_liabilities_home, net_worth_home
        FROM reports.net_worth_currencies
        WHERE currency_code = 'EUR' AND balance_date = '2026-01-05'
        """
    ).fetchone()
    assert row == (
        Decimal("200.00"),
        Decimal("-50.00"),
        Decimal("150.00"),
        None,
        None,
        None,
    )


def test_currencies_rung_counts_carried_forward_accounts(
    model_db: Database,
) -> None:
    """One observed and one carried-forward account both count; only one is carried."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking A", "USD")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "USD", observed=True)
    _account(model_db, "acct-b", "Checking B", "USD")
    _balance(model_db, "acct-b", "2026-01-05", "200.00", "USD", observed=False)
    _install_report(model_db, "net_worth_currencies")

    row = model_db.execute(
        """
        SELECT account_count, carried_forward_count
        FROM reports.net_worth_currencies
        WHERE currency_code = 'USD' AND balance_date = '2026-01-05'
        """
    ).fetchone()
    assert row == (2, 1)


def test_currencies_rung_date_scoped_archival(model_db: Database) -> None:
    """An archived account's currency segment still totals up to and including archived_at."""
    _install_net_worth_sources(model_db)
    _account(
        model_db,
        "acct-a",
        "Checking",
        "USD",
        archived=True,
        archived_at="2026-01-06",
    )
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "USD")
    _balance(model_db, "acct-a", "2026-01-06", "100.00", "USD")
    _balance(model_db, "acct-a", "2026-01-07", "100.00", "USD")
    _install_report(model_db, "net_worth_currencies")

    dates = model_db.execute(
        """
        SELECT balance_date
        FROM reports.net_worth_currencies
        WHERE currency_code = 'USD'
        ORDER BY balance_date
        """
    ).fetchall()
    assert dates == [(date(2026, 1, 5),), (date(2026, 1, 6),)]


def test_currencies_rung_pools_unknown_currency_into_one_segment(
    model_db: Database,
) -> None:
    """A NULL currency_code segments on its own, never folded into a known one."""
    _install_net_worth_sources(model_db)
    _account(model_db, "usd", "USD Account", "USD")
    _balance(model_db, "usd", "2026-01-05", "100.00", "USD")
    _account(model_db, "unknown", "Mystery Account", None)
    _balance(model_db, "unknown", "2026-01-05", "100.00", None)
    _install_report(model_db, "net_worth_currencies")

    rows = model_db.execute(
        """
        SELECT currency_code, net_worth
        FROM reports.net_worth_currencies
        WHERE balance_date = '2026-01-05'
        ORDER BY currency_code NULLS LAST
        """
    ).fetchall()
    assert rows == [("USD", Decimal("100.00")), (None, Decimal("100.00"))]


def test_currencies_rung_null_home_currency_prices_nothing(
    model_db: Database,
) -> None:
    """No app.profile_settings row means every currency is unpriced.

    A USD->USD identity rate exists for the test date, so a mutation that
    defaulted the missing home currency to 'USD' (e.g.
    ``COALESCE(h.home_currency_code, 'USD')``) would find it and price the
    segment — the NULL result here can only come from the missing home
    currency itself, not from an absent rate row.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "USD")
    _rate(model_db, "USD", "USD", "2026-01-05", "1.0", source="identity")
    _install_report(model_db, "net_worth_currencies")

    row = model_db.execute(
        """
        SELECT total_assets_home, home_currency_code
        FROM reports.net_worth_currencies
        WHERE currency_code = 'USD' AND balance_date = '2026-01-05'
        """
    ).fetchone()
    assert row == (None, None)


def test_currencies_rung_rate_join_binds_to_the_same_balance_date(
    model_db: Database,
) -> None:
    """The rate join binds on effective_date, not merely the currency pair.

    Two exchange rate rows for the same pair on different days must not
    cross-multiply each day's group into two rows: each balance_date's own
    rate wins. A join that dropped ``effective_date`` would match both rate
    rows against every day's group, doubling the row count for this currency
    and (via the wider GROUP BY) fanning each day's total_assets_home out into
    two conflicting values instead of the one correct rate.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Euro Checking", "EUR")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "EUR")
    _balance(model_db, "acct-a", "2026-01-06", "100.00", "EUR")
    _home(model_db, "USD")
    _rate(model_db, "EUR", "USD", "2026-01-05", "1.10")
    _rate(model_db, "EUR", "USD", "2026-01-06", "1.20")
    _install_report(model_db, "net_worth_currencies")

    rows = model_db.execute(
        """
        SELECT balance_date, total_assets_home
        FROM reports.net_worth_currencies
        WHERE currency_code = 'EUR'
        ORDER BY balance_date
        """
    ).fetchall()
    assert rows == [
        (date(2026, 1, 5), Decimal("110.00")),
        (date(2026, 1, 6), Decimal("120.00")),
    ]


def test_currencies_rung_grain_is_unique(model_db: Database) -> None:
    """Two currencies across three days yield exactly six unique grain rows."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking A", "USD")
    _account(model_db, "acct-b", "Checking B", "EUR")
    for day in ("2026-01-05", "2026-01-06", "2026-01-07"):
        _balance(model_db, "acct-a", day, "100.00", "USD")
        _balance(model_db, "acct-b", day, "200.00", "EUR")
    _install_report(model_db, "net_worth_currencies")

    row = model_db.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT (currency_code, balance_date))
        FROM reports.net_worth_currencies
        """
    ).fetchone()
    assert row == (6, 6)


def test_currencies_rung_projects_the_declared_column_order(
    model_db: Database,
) -> None:
    """The SELECT projects exactly the Interfaces column order."""
    _install_net_worth_sources(model_db)
    _install_report(model_db, "net_worth_currencies")

    cursor = model_db.execute("SELECT * FROM reports.net_worth_currencies LIMIT 0")
    columns = [column[0] for column in cursor.description]
    assert columns == list(_CURRENCIES_COLUMNS)


def test_day_rung_sums_currencies_in_home(model_db: Database) -> None:
    """USD 100.00 plus EUR 100.00 at 1.10 totals 210.00 in the home currency."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-usd", "Checking", "USD")
    _balance(model_db, "acct-usd", "2026-01-05", "100.00", "USD")
    _account(model_db, "acct-eur", "Euro Checking", "EUR")
    _balance(model_db, "acct-eur", "2026-01-05", "100.00", "EUR")
    _home(model_db, "USD")
    _rate(model_db, "USD", "USD", "2026-01-05", "1.0", source="identity")
    _rate(model_db, "EUR", "USD", "2026-01-05", "1.10")
    _install_report(model_db, "net_worth")

    row = model_db.execute(
        """
        SELECT net_worth, currency_count, unpriced_currency_count
        FROM reports.net_worth
        WHERE balance_date = '2026-01-05'
        """
    ).fetchone()
    assert row == (Decimal("210.00"), 2, 0)


def test_day_rung_fails_closed_on_one_unpriced_currency(model_db: Database) -> None:
    """A single unpriced currency NULLs every measure, not just its own segment."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-usd", "Checking", "USD")
    _balance(model_db, "acct-usd", "2026-01-05", "100.00", "USD")
    _account(model_db, "acct-jpy", "Yen Checking", "JPY")
    _balance(model_db, "acct-jpy", "2026-01-05", "1000", "JPY")
    _home(model_db, "USD")
    _rate(model_db, "USD", "USD", "2026-01-05", "1.0", source="identity")
    _install_report(model_db, "net_worth")

    row = model_db.execute(
        """
        SELECT net_worth, total_assets, total_liabilities,
               unpriced_currency_count, currency_count
        FROM reports.net_worth
        WHERE balance_date = '2026-01-05'
        """
    ).fetchone()
    assert row == (None, None, None, 1, 2)
    # The priced USD subset (100.00) must never leak through as if it were
    # the whole total.
    assert row is not None and row[1] != Decimal("100.00")


def test_day_rung_unknown_currency_counts_as_unpriced(model_db: Database) -> None:
    """A NULL currency_code account counts as one unpriced currency."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Mystery", None)
    _balance(model_db, "acct-a", "2026-01-05", "100.00", None)
    _home(model_db, "USD")
    _install_report(model_db, "net_worth")

    row = model_db.execute(
        """
        SELECT net_worth, unpriced_currency_count, currency_count
        FROM reports.net_worth
        WHERE balance_date = '2026-01-05'
        """
    ).fetchone()
    assert row == (None, 1, 1)


def test_day_rung_single_currency_profile_is_never_null(model_db: Database) -> None:
    """A USD-only profile with an identity rate on every date never nulls out.

    Requirement 11: a single-currency profile is priced by construction, since
    every balance's currency is the home currency and an identity rate row
    exists for it on every date.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _home(model_db, "USD")
    for day in ("2026-01-05", "2026-01-06", "2026-01-07"):
        _balance(model_db, "acct-a", day, "100.00", "USD")
        _rate(model_db, "USD", "USD", day, "1.0", source="identity")
    _install_report(model_db, "net_worth")

    rows = model_db.execute(
        "SELECT balance_date, net_worth FROM reports.net_worth ORDER BY balance_date"
    ).fetchall()
    assert len(rows) == 3
    assert all(net_worth is not None for _, net_worth in rows)


def test_day_rung_date_scoped_archival(model_db: Database) -> None:
    """An archived account still contributes rows up to and including archived_at."""
    _install_net_worth_sources(model_db)
    _account(
        model_db,
        "acct-a",
        "Checking",
        "USD",
        archived=True,
        archived_at="2026-01-06",
    )
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "USD")
    _balance(model_db, "acct-a", "2026-01-06", "100.00", "USD")
    _balance(model_db, "acct-a", "2026-01-07", "100.00", "USD")
    _install_report(model_db, "net_worth")

    dates = model_db.execute(
        "SELECT balance_date FROM reports.net_worth ORDER BY balance_date"
    ).fetchall()
    assert dates == [(date(2026, 1, 5),), (date(2026, 1, 6),)]


def test_day_rung_grain_is_unique(model_db: Database) -> None:
    """Two currencies across three days still yield exactly three day rows."""
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking A", "USD")
    _account(model_db, "acct-b", "Checking B", "EUR")
    for day in ("2026-01-05", "2026-01-06", "2026-01-07"):
        _balance(model_db, "acct-a", day, "100.00", "USD")
        _balance(model_db, "acct-b", day, "200.00", "EUR")
    _install_report(model_db, "net_worth")

    row = model_db.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT balance_date)
        FROM reports.net_worth
        """
    ).fetchone()
    assert row == (3, 3)


def test_day_rung_rate_join_binds_to_the_same_balance_date(
    model_db: Database,
) -> None:
    """The rate join binds on effective_date, not merely the currency pair.

    Two exchange rate rows for the same pair on different days must not
    cross-multiply a single day's balance into two per-currency groups: each
    balance_date's own rate wins. A join that dropped ``effective_date`` would
    match both rate rows against every balance_date for this pair, splitting
    each date's single EUR segment into two conflicting rows and doubling
    both currency_count and total_assets for each date.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-eur", "Euro Checking", "EUR")
    _balance(model_db, "acct-eur", "2026-01-05", "100.00", "EUR")
    _balance(model_db, "acct-eur", "2026-01-06", "100.00", "EUR")
    _home(model_db, "USD")
    _rate(model_db, "EUR", "USD", "2026-01-05", "1.10")
    _rate(model_db, "EUR", "USD", "2026-01-06", "1.20")
    _install_report(model_db, "net_worth")

    rows = model_db.execute(
        """
        SELECT balance_date, net_worth, currency_count
        FROM reports.net_worth
        ORDER BY balance_date
        """
    ).fetchall()
    assert rows == [
        (date(2026, 1, 5), Decimal("110.00"), 1),
        (date(2026, 1, 6), Decimal("120.00"), 1),
    ]


def test_day_rung_null_home_currency_prices_nothing(model_db: Database) -> None:
    """No app.profile_settings row means every date is unpriced.

    A USD->USD identity rate exists for the test date, so a mutation that
    defaulted the missing home currency to 'USD' (e.g.
    ``COALESCE(h.home_currency_code, 'USD')``) would find it and price the
    day — the NULL result here can only come from the missing home currency
    itself, not from an absent rate row.
    """
    _install_net_worth_sources(model_db)
    _account(model_db, "acct-a", "Checking", "USD")
    _balance(model_db, "acct-a", "2026-01-05", "100.00", "USD")
    _rate(model_db, "USD", "USD", "2026-01-05", "1.0", source="identity")
    _install_report(model_db, "net_worth")

    row = model_db.execute(
        """
        SELECT net_worth, home_currency_code, unpriced_currency_count
        FROM reports.net_worth
        WHERE balance_date = '2026-01-05'
        """
    ).fetchone()
    assert row == (None, None, 1)


def test_day_rung_projects_the_declared_column_order(model_db: Database) -> None:
    """The SELECT projects exactly the Interfaces column order."""
    _install_net_worth_sources(model_db)
    _install_report(model_db, "net_worth")

    cursor = model_db.execute("SELECT * FROM reports.net_worth LIMIT 0")
    columns = [column[0] for column in cursor.description]
    assert columns == list(_DAY_COLUMNS)
