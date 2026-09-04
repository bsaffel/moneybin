"""An investment event with no currency inherits the account's, never a literal.

multi-currency.md Requirement 3 ("where a grain genuinely cannot know its
currency, it inherits the account's ``currency_code``, never a blind ``'USD'``")
applied to the investment ledger. The cash grain already resolves it this way in
``core.fct_transactions``; these tests hold the same chain for the investment
grain — ``raw.manual_investment_transactions`` →
``prep.stg_manual__investment_transactions`` → ``core.fct_investment_transactions``
— by installing the two shipped models as views over hand-made sources.

The discriminating fixture is a **EUR** account: a USD one passes whether the
model inherits or fabricates. The unknown-account-currency case is the other
half — it is the only one that can tell inheritance from a surviving literal
fallback, because that is where a fallback would still fire.
"""

from __future__ import annotations

import re
import typing as t
from datetime import datetime
from pathlib import Path

import pytest

from moneybin.currency_lots import sqlmesh_loader
from moneybin.database import SQLMESH_ROOT, Database

_MANUAL_STG = (
    SQLMESH_ROOT / "models" / "prep" / "stg_manual__investment_transactions.sql"
)
_LEDGER = SQLMESH_ROOT / "models" / "core" / "fct_investment_transactions.sql"

# The columns core.fct_investment_transactions unions from the two Plaid
# branches. Empty stubs: this file is about the manual branch's currency.
_PLAID_COLUMNS = """
    investment_transaction_id VARCHAR,
    account_id VARCHAR,
    security_id VARCHAR,
    trade_date DATE,
    settlement_date DATE,
    original_acquisition_date DATE,
    type VARCHAR,
    subtype VARCHAR,
    event_group_id VARCHAR,
    quantity DECIMAL(28, 10),
    price DECIMAL(28, 10),
    amount DECIMAL(18, 2),
    fees DECIMAL(18, 2),
    currency_code VARCHAR,
    provider_type VARCHAR,
    provider_subtype VARCHAR,
    source_type VARCHAR,
    source_origin VARCHAR,
    description VARCHAR,
    created_at TIMESTAMP
"""

_ACCOUNT_ID = "acct_brokerage_eur"


class _DatabaseContext:
    """ExecutionContext subset backed by the test database."""

    def __init__(self, db: Database) -> None:
        self.db = db

    def resolve_table(self, name: str) -> str:
        return name

    def fetchdf(self, sql: str) -> t.Any:
        return self.db.execute(sql).fetchdf()


def _model_body(path: Path) -> str:
    """A model's executable SQL, without its MODEL header."""
    return re.sub(
        r"^.*?MODEL\s*\(.*?\);\s*", "", path.read_text(), count=1, flags=re.DOTALL
    ).strip()


def _install_ledger_chain(
    db: Database,
    *,
    account_currency: str | None,
    account_updated_at: str = "2026-05-10 08:00:00",
) -> None:
    """Install the two shipped models over hand-made sources."""
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    db.execute(
        f"CREATE TABLE prep.stg_plaid__investment_transactions ({_PLAID_COLUMNS}, ledger_include BOOLEAN)"
    )  # noqa: S608  # static test DDL
    db.execute(f"CREATE TABLE prep.stg_plaid__opening_lots ({_PLAID_COLUMNS})")  # noqa: S608  # static test DDL
    db.execute("""
        CREATE TABLE core.dim_accounts (
            account_id VARCHAR,
            currency_code VARCHAR,
            updated_at TIMESTAMP
        )
    """)
    db.execute(
        "INSERT INTO core.dim_accounts VALUES (?, ?, ?::TIMESTAMP)",
        [_ACCOUNT_ID, account_currency, account_updated_at],
    )
    db.execute(  # noqa: S608  # shipped model body, not user SQL
        "CREATE OR REPLACE VIEW prep.stg_manual__investment_transactions AS "
        + _model_body(_MANUAL_STG)
    )
    db.execute(  # noqa: S608  # shipped model body, not user SQL
        "CREATE OR REPLACE VIEW core.fct_investment_transactions AS "
        + _model_body(_LEDGER)
    )


def _record_event(
    db: Database,
    *,
    currency_code: str | None,
    created_at: str = "2026-05-11 09:00:00",
    event_type: str = "buy",
    amount: str = "-100.00",
) -> None:
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions
            (source_transaction_id, import_id, account_id, security_id, type,
             trade_date, quantity, amount, currency_code, created_by,
             investment_transaction_id, created_at)
        VALUES ('manual_evt_1', 'import_1', ?, 'sec_1', ?, '2026-05-11'::DATE,
                10::DECIMAL(28,10), ?::DECIMAL(18,2), ?, 'cli', 'inv_evt_1',
                ?::TIMESTAMP)
        """,  # noqa: S608  # static test fixture
        [_ACCOUNT_ID, event_type, amount, currency_code, created_at],
    )


def _ledger_currency(db: Database) -> str | None:
    row = db.execute(
        "SELECT currency_code FROM core.fct_investment_transactions"
    ).fetchone()
    assert row is not None, "expected one ledger row"
    return row[0]


def _ledger_currency_and_updated_at(db: Database) -> tuple[str | None, datetime]:
    row = db.execute(
        "SELECT currency_code, updated_at FROM core.fct_investment_transactions"
    ).fetchone()
    assert row is not None, "expected one ledger row"
    return row[0], row[1]


class TestInvestmentEventCurrencyInheritance:
    """core.fct_investment_transactions resolves currency like the cash grain."""

    def test_an_omitted_currency_inherits_the_accounts(self, db: Database) -> None:
        """A EUR account's event recorded without a currency is EUR, not USD."""
        _install_ledger_chain(db, account_currency="EUR")
        _record_event(db, currency_code=None)

        assert _ledger_currency(db) == "EUR"

    def test_a_supplied_currency_wins_over_the_accounts(self, db: Database) -> None:
        """The caller's own currency is never overwritten by the account's."""
        _install_ledger_chain(db, account_currency="EUR")
        _record_event(db, currency_code="GBP")

        assert _ledger_currency(db) == "GBP"

    def test_an_unknown_currency_stays_unknown(self, db: Database) -> None:
        """No currency and no account currency stays NULL — never a fabricated USD."""
        _install_ledger_chain(db, account_currency=None)
        _record_event(db, currency_code=None)

        assert _ledger_currency(db) is None

    def test_inherited_currency_uses_account_freshness(self, db: Database) -> None:
        """A newer Account Currency timestamp advances the inherited event."""
        _install_ledger_chain(
            db,
            account_currency="EUR",
            account_updated_at="2026-05-13 10:00:00",
        )
        _record_event(
            db,
            currency_code=None,
            created_at="2026-05-12 09:00:00",
        )

        assert _ledger_currency_and_updated_at(db) == (
            "EUR",
            datetime(2026, 5, 13, 10),
        )

    def test_cleared_inherited_currency_uses_account_freshness(
        self, db: Database
    ) -> None:
        """Clearing inherited Currency is a fresh NULL, not stale source data."""
        _install_ledger_chain(
            db,
            account_currency=None,
            account_updated_at="2026-05-13 10:00:00",
        )
        _record_event(
            db,
            currency_code=None,
            created_at="2026-05-12 09:00:00",
        )

        assert _ledger_currency_and_updated_at(db) == (
            None,
            datetime(2026, 5, 13, 10),
        )

    def test_own_currency_ignores_newer_account_freshness(self, db: Database) -> None:
        """An Account edit cannot refresh a row whose own Currency wins."""
        _install_ledger_chain(
            db,
            account_currency="EUR",
            account_updated_at="2026-05-13 10:00:00",
        )
        _record_event(
            db,
            currency_code="GBP",
            created_at="2026-05-12 09:00:00",
        )

        assert _ledger_currency_and_updated_at(db) == (
            "GBP",
            datetime(2026, 5, 12, 9),
        )

    def test_inherited_currency_freshness_reaches_security_sale_lot(
        self, db: Database
    ) -> None:
        """The sale loader preserves Account freshness from the actual fact view."""
        account_updated_at = datetime(2026, 5, 13, 10)
        _install_ledger_chain(
            db,
            account_currency="EUR",
            account_updated_at=str(account_updated_at),
        )
        _record_event(
            db,
            currency_code=None,
            created_at="2026-05-12 09:00:00",
            event_type="sell",
            amount="100.00",
        )
        db.execute(
            """
            INSERT INTO raw.exchange_rates (
                from_currency, to_currency, rate_date, rate, source_type, loaded_at
            ) VALUES (
                'EUR', 'USD', '2026-05-11'::DATE, 1.20000000,
                'frankfurter', '2026-05-12 08:00:00'::TIMESTAMP
            )
            """
        )
        context = t.cast(t.Any, _DatabaseContext(db))
        stored_rate = sqlmesh_loader._load_stored_rates(context)  # pyright: ignore[reportPrivateUsage]
        sales = sqlmesh_loader._load_security_sales(  # pyright: ignore[reportPrivateUsage]
            context,
            home_currency="USD",
            home_updated_at=datetime(2026, 5, 1, 10),
            stored_rate=stored_rate,
        )

        result = sqlmesh_loader.derive_currency_accounting((), sales, {})

        assert len(result.lots) == 1
        assert result.lots[0].currency_code == "EUR"
        assert result.lots[0].updated_at == account_updated_at


class TestManualInvestmentCurrencyIsNeverFabricated:
    """The write path stores what the caller gave, including nothing."""

    @pytest.mark.fresh_db
    def test_the_raw_column_carries_no_default(self, db: Database) -> None:
        """A DDL DEFAULT would re-fabricate USD under every layer above it."""
        row = db.execute(
            """
            SELECT column_default FROM duckdb_columns()
            WHERE schema_name = 'raw'
              AND table_name = 'manual_investment_transactions'
              AND column_name = 'currency_code'
            """
        ).fetchone()
        assert row is not None, (
            "raw.manual_investment_transactions.currency_code missing"
        )
        assert row[0] is None, f"currency_code still defaults to {row[0]}"
