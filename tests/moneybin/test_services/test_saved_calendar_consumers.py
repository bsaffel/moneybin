"""Reporting defaults follow the saved calendar across UTC day boundaries."""

from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from typer.testing import CliRunner

from moneybin.cli.main import app
from moneybin.database import Database
from moneybin.privacy.sql_query import execute_sql_query
from moneybin.repositories.profile_settings_repo import ProfileSettingsRepo
from moneybin.services.account_service import AccountService
from moneybin.services.categorization.queries import CategorizationQueries
from moneybin.services.schema_catalog import EXAMPLES
from tests.moneybin.db_helpers import create_core_tables


def _skip_restate(*args: object, **kwargs: object) -> None:
    return None


@pytest.fixture(
    params=[
        ("America/Los_Angeles", 6, date(2025, 12, 31)),
        ("Pacific/Kiritimati", 18, date(2026, 1, 2)),
    ]
)
def calendar(
    request: pytest.FixtureRequest, db: Database, monkeypatch: pytest.MonkeyPatch
) -> tuple[datetime, date]:
    zone, hour, today = request.param
    now = datetime(2026, 1, 1, hour, tzinfo=UTC)
    ProfileSettingsRepo(db).set_reporting_timezone(zone, actor="test")

    class Clock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return now.astimezone(tz)

    monkeypatch.setattr("moneybin.services.profile_settings_service.datetime", Clock)
    return now, today


def test_summary_recent_activity_uses_saved_calendar(
    db: Database, calendar: tuple[datetime, date]
) -> None:
    create_core_tables(db)
    cutoff = calendar[1] - timedelta(days=30)
    db.execute(
        """
        INSERT INTO core.fct_transactions (transaction_id, account_id, transaction_date)
        VALUES ('included', 'a', ?), ('excluded', 'b', ?)
    """,
        [cutoff, cutoff - timedelta(days=1)],
    )
    assert AccountService(db).summary().count_with_recent_activity == 1


def test_category_age_and_impact_use_saved_calendar(
    db: Database, calendar: tuple[datetime, date]
) -> None:
    create_core_tables(db)
    db.execute(
        """
        INSERT INTO core.fct_transactions
            (transaction_id, account_id, transaction_date, amount, currency_code)
        VALUES ('recent', 'a', ?, -20, 'USD'), ('older', 'a', ?, -15, 'USD')
    """,
        [calendar[1] - timedelta(days=1), calendar[1] - timedelta(days=2)],
    )
    rows = CategorizationQueries(db).list_uncategorized_transactions(
        limit=None, sort="impact", account_id="a", min_amount=Decimal("10")
    )
    assert rows is not None
    assert [
        (r["transaction_id"], r["age_days"], r["priority_score"]) for r in rows
    ] == [("older", 2, Decimal("30")), ("recent", 1, Decimal("20"))]


def test_archive_uses_saved_calendar(
    db: Database, calendar: tuple[datetime, date], monkeypatch: pytest.MonkeyPatch
) -> None:
    create_core_tables(db)
    db.execute("INSERT INTO core.dim_accounts (account_id) VALUES ('a')")
    monkeypatch.setattr("moneybin.services.restate.restate_after_write", _skip_restate)
    assert AccountService(db).archive("a").archived_at == calendar[1]
    assert db.execute("SELECT archived_at FROM app.account_settings").fetchone() == (
        calendar[1],
    )


@pytest.mark.parametrize("explicit", [False, True])
def test_fx_default_uses_saved_calendar(
    db: Database,
    calendar: tuple[datetime, date],
    monkeypatch: pytest.MonkeyPatch,
    explicit: bool,
) -> None:
    @contextmanager
    def connection(**kwargs: Any):
        yield db

    monkeypatch.setattr("moneybin.cli.commands.fx.get_database", connection)
    requested = date(2024, 2, 29) if explicit else calendar[1]
    # Identity rates exercise real resolution without a network provider.
    result = CliRunner().invoke(
        app,
        [
            "fx",
            "rate",
            "USD",
            "USD",
            *([requested.isoformat()] if explicit else []),
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    import json

    assert json.loads(result.stdout)["data"]["requested_date"] == requested.isoformat()


@pytest.mark.parametrize("explicit", [False, True])
def test_manual_transaction_default_uses_saved_calendar(
    db: Database,
    calendar: tuple[datetime, date],
    monkeypatch: pytest.MonkeyPatch,
    explicit: bool,
) -> None:
    from tests.moneybin.test_cli._curation_helpers import patch_db

    create_core_tables(db)
    db.execute("INSERT INTO core.dim_accounts (account_id) VALUES ('a')")
    patch_db(monkeypatch, db)
    monkeypatch.setattr("moneybin.services.restate.restate_after_write", _skip_restate)
    requested = date(2024, 2, 29) if explicit else calendar[1]
    result = CliRunner().invoke(
        app,
        [
            "transactions",
            "create",
            "--account",
            "a",
            *(["--date", requested.isoformat()] if explicit else []),
            "--",
            "-12.50",
            "Synthetic purchase",
        ],
    )
    assert result.exit_code == 0, result.output
    assert db.execute(
        "SELECT transaction_date FROM raw.manual_transactions"
    ).fetchone() == (requested,)


@pytest.mark.parametrize(
    "table,index",
    [
        ("core.fct_transactions", 0),
        ("core.fct_transactions", 2),
        ("reports.net_worth", 1),
        ("reports.cash_flow", 1),
        ("core.fct_realized_gains", 0),
    ],
)
def test_catalog_examples_use_saved_calendar(
    db: Database,
    calendar: tuple[datetime, date],
    table: str,
    index: int,
) -> None:
    now, today = calendar
    create_core_tables(db)
    db.execute("CREATE SCHEMA IF NOT EXISTS reports")
    db.execute("DROP VIEW IF EXISTS reports.net_worth")
    db.execute("DROP VIEW IF EXISTS reports.cash_flow")
    db.execute(
        "CREATE TABLE reports.net_worth (balance_date DATE, home_currency_code VARCHAR, net_worth DECIMAL(18,2))"
    )
    db.execute(
        "CREATE TABLE reports.cash_flow (year_month VARCHAR, category VARCHAR, currency_code VARCHAR, outflow DECIMAL(18,2))"
    )
    cutoff = today.replace(year=today.year - 1)
    if table == "core.fct_transactions":
        if index == 0:
            cutoff = today.replace(day=1) - timedelta(days=1)
            excluded = today
        else:
            excluded = cutoff - timedelta(days=1)
        db.execute(
            """
            INSERT INTO core.fct_transactions (transaction_id, transaction_date,
                transaction_year_month, currency_code, transaction_direction,
                amount_absolute, category)
            VALUES ('included', ?, ?, 'USD', 'expense', 100, 'Test'),
                   ('excluded', ?, ?, 'USD', 'expense', 200, 'Test')
        """,
            [cutoff, cutoff.strftime("%Y-%m"), excluded, excluded.strftime("%Y-%m")],
        )
    elif table == "reports.net_worth":
        db.execute(
            "INSERT INTO reports.net_worth VALUES (?, 'USD', 100), (?, 'USD', 200)",
            [cutoff, cutoff - timedelta(days=1)],
        )
    elif table == "reports.cash_flow":
        excluded = cutoff.replace(day=1) - timedelta(days=1)
        db.execute(
            "INSERT INTO reports.cash_flow VALUES (?, 'Test', 'USD', 100), (?, 'Test', 'USD', 200)",
            [cutoff.strftime("%Y-%m"), excluded.strftime("%Y-%m")],
        )
    else:
        db.execute(
            "CREATE TABLE IF NOT EXISTS core.fct_realized_gains (disposal_date DATE, term VARCHAR, currency_code VARCHAR, gain_loss DECIMAL(18,2))"
        )
        db.execute(
            "INSERT INTO core.fct_realized_gains (disposal_date, term, currency_code, gain_loss) VALUES (?, 'short', 'USD', 100), (?, 'short', 'USD', 200)",
            [today, today.replace(year=today.year - 1)],
        )
    # Pin only the current instant; the copied SQL must resolve its saved zone.
    sql = EXAMPLES[table][index].sql.replace(
        "CURRENT_TIMESTAMP", f"TIMESTAMPTZ '{now.isoformat()}'"
    )
    rows = execute_sql_query(db, sql, max_rows=10).records
    assert len(rows) == 1
    assert Decimal(str(list(rows[0].values())[-1])) == Decimal("100")
