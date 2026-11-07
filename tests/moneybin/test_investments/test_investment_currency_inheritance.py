"""An investment event with no currency inherits the account's, never a literal.

multi-currency.md Requirement 3 ("where a grain genuinely cannot know its
currency, it inherits the account's ``currency_code``, never a blind ``'USD'``")
applied to the investment ledger. The cash grain already resolves it this way in
``core.fct_transactions``; these tests hold the same chain for the investment
grain through the shipped identity, comparison, membership, and Golden
projection models over synthetic Raw observations. Currency becomes immutable
published evidence until an audited dependency transition replaces its binding.

The discriminating fixture is a **EUR** account: a USD one passes whether the
model inherits or fabricates. The unknown-account-currency case is the other
half — it is the only one that can tell inheritance from a surviving literal
fallback, because that is where a fallback would still fire.
"""

from __future__ import annotations

import re
import typing as t
from datetime import UTC, datetime
from pathlib import Path

import pytest

from moneybin.currency_lots import sqlmesh_loader
from moneybin.database import SQLMESH_ROOT, Database
from moneybin.repositories.account_settings_repo import AccountSettingsRepo
from moneybin.repositories.investment_event_members_repo import (
    InvestmentEventMembersRepo,
)
from moneybin.repositories.securities_repo import SecuritiesRepo
from moneybin.services.investment_membership_service import InvestmentMembershipService
from tests.moneybin.test_investments.comparison_helpers import install_comparison_models
from tests.moneybin.test_investments.test_investment_golden_projection import (
    install_model,
)

_MANUAL_STG = (
    SQLMESH_ROOT / "models" / "prep" / "stg_manual__investment_transactions.sql"
)
_MANUAL_IDENTITY = (
    SQLMESH_ROOT / "models" / "prep" / "int_manual__investment_identity.sql"
)
_LEDGER = SQLMESH_ROOT / "models" / "core" / "fct_investment_transactions.sql"

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
    account_updated_at: str = "2026-05-10 08:00:00+00:00",
) -> None:
    """Install the shipped identity and ledger models over hand-made sources."""
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    # Clearing an override rereads the shipped Account query and its sources.
    for seed in ("account_type_map", "institutions", "iso_currencies"):
        db.execute(
            f"CREATE TABLE seeds.{seed} AS SELECT * FROM read_csv(?, all_varchar = true)",  # noqa: S608  # fixed shipped seed names, bound paths
            [str(SQLMESH_ROOT / "models" / "seeds" / f"{seed}.csv")],
        )  # fixed shipped seed names, bound local paths
    for name in (
        "prep.stg_ofx__accounts",
        "prep.stg_ofx__balances",
        "prep.stg_plaid__accounts",
        "prep.stg_plaid__balances",
        "prep.stg_simplefin__accounts",
        "prep.stg_tabular__accounts",
    ):
        install_model(db, name)
    # The ledger reads the account base, not the dim view over it.
    db.execute("""
        CREATE TABLE prep.int_accounts__merged (
            account_id VARCHAR,
            currency_code VARCHAR,
            updated_at TIMESTAMPTZ,
            investment_source_type VARCHAR,
            investment_source_type_changed_at TIMESTAMPTZ
        )
    """)
    db.execute(
        "INSERT INTO prep.int_accounts__merged (account_id, currency_code, updated_at) "
        "VALUES (?, ?, ?::TIMESTAMPTZ)",
        [_ACCOUNT_ID, account_currency, account_updated_at],
    )
    db.execute(
        "CREATE VIEW core.dim_accounts AS "
        "SELECT account_id, currency_code FROM prep.int_accounts__merged"
    )
    SecuritiesRepo(db).upsert(
        security_id="sec_1",
        name="Synthetic Security",
        security_type="equity",
        actor="cli",
    )
    install_comparison_models(db)
    _publish_currency(db, account_currency)
    for name in (
        "prep.int_investment_events__source_projection",
        "prep.int_investment_events__opening_projection",
    ):
        install_model(db, name)
    db.execute(  # shipped model body, not user SQL
        "CREATE OR REPLACE VIEW prep.int_manual__investment_identity AS "
        + _model_body(_MANUAL_IDENTITY)
    )
    db.execute(  # shipped model body, not user SQL
        "CREATE OR REPLACE VIEW prep.stg_manual__investment_transactions AS "
        + _model_body(_MANUAL_STG)
    )
    db.execute(  # shipped model body, not user SQL
        "CREATE OR REPLACE VIEW core.fct_investment_transactions AS "
        + _model_body(_LEDGER)
    )


def _record_event(
    db: Database,
    *,
    currency_code: str | None,
    created_at: str = "2026-05-11 09:00:00+00:00",
    event_type: str = "buy",
    amount: str = "-100.00",
) -> None:
    db.execute(
        """
        INSERT INTO raw.manual_investment_transactions
            (source_transaction_id, import_id, account_id, security_id, type,
             trade_date, quantity, price, amount, fees, currency_code, created_by,
             investment_transaction_id, created_at)
        VALUES ('manual_evt_1', 'import_1', ?, 'sec_1', ?, '2026-05-11'::DATE,
                ?::DECIMAL(28,10), 10, ?::DECIMAL(18,2), 0, ?, 'cli', 'inv_evt_1',
                ?::TIMESTAMPTZ)
        """,  # static test fixture
        [
            _ACCOUNT_ID,
            event_type,
            "-10" if event_type == "sell" else "10",
            amount,
            currency_code,
            created_at,
        ],
    )
    InvestmentMembershipService(db).reconcile(actor="cli")


def _publish_currency(db: Database, currency: str | None) -> None:
    AccountSettingsRepo(db).set(
        account_id=_ACCOUNT_ID,
        display_name=None,
        official_name=None,
        last_four=None,
        account_subtype=None,
        holder_category=None,
        currency_code=currency,
        credit_limit=None,
        archived=False,
        archived_at=None,
        include_in_net_worth=True,
        default_cost_basis_method=None,
        investment_source_type=None,
        investment_source_type_changed_at=None,
        archived_changed_at=None,
        currency_code_changed_at=datetime.now(UTC),
        default_cost_basis_method_changed_at=None,
        actor="cli",
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

        assert db.execute(
            "SELECT currency_code FROM raw.manual_investment_transactions"
        ).fetchone() == (None,)
        assert InvestmentEventMembersRepo(db).active_members() == []
        assert (
            db.execute(
                "SELECT investment_transaction_id FROM core.fct_investment_transactions"
            ).fetchall()
            == []
        )

    def test_inherited_currency_uses_account_freshness(self, db: Database) -> None:
        """Published Account Currency changes advance inherited membership freshness."""
        _install_ledger_chain(
            db,
            account_currency="GBP",
            account_updated_at="2026-05-10 08:00:00+00:00",
        )
        _record_event(
            db,
            currency_code=None,
            created_at="2026-05-12 09:00:00+00:00",
        )

        raw = db.execute("SELECT * FROM raw.manual_investment_transactions").fetchall()
        before = InvestmentEventMembersRepo(db).active_members()[0]
        assert _ledger_currency_and_updated_at(db) == (
            "GBP",
            before["projection_changed_at"],
        )
        db.execute(
            """UPDATE prep.int_accounts__merged
            SET currency_code = 'EUR', updated_at = TIMESTAMPTZ '2026-05-13 10:00:00'
            WHERE account_id = ?""",
            [_ACCOUNT_ID],
        )
        assert _ledger_currency_and_updated_at(db) == (
            "GBP",
            before["projection_changed_at"],
        )
        _publish_currency(db, "EUR")
        after = InvestmentEventMembersRepo(db).active_members()[0]
        assert _ledger_currency_and_updated_at(db) == (
            "EUR",
            after["projection_changed_at"],
        )
        assert after["projection_changed_at"] > before["projection_changed_at"]
        assert (
            db.execute("SELECT * FROM raw.manual_investment_transactions").fetchall()
            == raw
        )

    def test_cleared_inherited_currency_uses_account_freshness(
        self, db: Database
    ) -> None:
        """Unresolved inherited Currency retires publication without fabricating USD."""
        _install_ledger_chain(
            db,
            account_currency="GBP",
            account_updated_at="2026-05-13 10:00:00+00:00",
        )
        _record_event(
            db,
            currency_code=None,
            created_at="2026-05-12 09:00:00+00:00",
        )

        raw = db.execute("SELECT * FROM raw.manual_investment_transactions").fetchall()
        assert _ledger_currency(db) == "GBP"
        db.execute(
            "UPDATE prep.int_accounts__merged SET currency_code = NULL WHERE account_id = ?",
            [_ACCOUNT_ID],
        )
        _publish_currency(db, None)

        assert InvestmentEventMembersRepo(db).active_members() == []
        assert (
            db.execute(
                "SELECT investment_transaction_id FROM core.fct_investment_transactions"
            ).fetchall()
            == []
        )
        assert db.execute(
            "SELECT currency_code FROM raw.manual_investment_transactions"
        ).fetchone() == (None,)
        assert (
            db.execute("SELECT * FROM raw.manual_investment_transactions").fetchall()
            == raw
        )

    def test_own_currency_ignores_newer_account_freshness(self, db: Database) -> None:
        """An Account edit cannot refresh a row whose own Currency wins."""
        _install_ledger_chain(
            db,
            account_currency="EUR",
            account_updated_at="2026-05-13 10:00:00+00:00",
        )
        _record_event(
            db,
            currency_code="GBP",
            created_at="2026-05-12 09:00:00+00:00",
        )

        before = _ledger_currency_and_updated_at(db)
        history = InvestmentEventMembersRepo(db).history()
        raw = db.execute("SELECT * FROM raw.manual_investment_transactions").fetchall()
        assert before[0] == "GBP"
        db.execute(
            "UPDATE prep.int_accounts__merged SET currency_code = 'CAD' WHERE account_id = ?",
            [_ACCOUNT_ID],
        )
        _publish_currency(db, "CAD")

        assert _ledger_currency_and_updated_at(db) == before
        assert InvestmentEventMembersRepo(db).history() == history
        assert (
            db.execute("SELECT * FROM raw.manual_investment_transactions").fetchall()
            == raw
        )

    def test_inherited_currency_freshness_reaches_security_sale_lot(
        self, db: Database
    ) -> None:
        """The sale loader preserves membership freshness from the actual fact view."""
        account_updated_at = datetime(2026, 5, 13, 10, tzinfo=UTC)
        _install_ledger_chain(
            db,
            account_currency="EUR",
            account_updated_at=str(account_updated_at),
        )
        _record_event(
            db,
            currency_code=None,
            created_at="2026-05-12 09:00:00+00:00",
            event_type="sell",
            amount="100.00",
        )
        db.execute(
            """
            INSERT INTO raw.exchange_rates (
                from_currency, to_currency, rate_date, rate, source_type, loaded_at
            ) VALUES (
                'EUR', 'USD', '2026-05-11'::DATE, 1.20000000,
                'frankfurter', '2026-05-12 08:00:00'::TIMESTAMPTZ
            )
            """
        )
        context = t.cast(t.Any, _DatabaseContext(db))
        stored_rate = sqlmesh_loader._load_stored_rates(context)  # pyright: ignore[reportPrivateUsage]
        sales = sqlmesh_loader._load_security_sales(  # pyright: ignore[reportPrivateUsage]
            context,
            home_currency="USD",
            home_updated_at=datetime(2026, 5, 1, 10, tzinfo=UTC),
            stored_rate=stored_rate,
        )

        result = sqlmesh_loader.derive_currency_accounting((), sales, {})

        assert len(result.lots) == 1
        assert result.lots[0].currency_code == "EUR"
        published = InvestmentEventMembersRepo(db).active_members()[0]
        assert _ledger_currency_and_updated_at(db) == (
            "EUR",
            published["projection_changed_at"],
        )
        assert result.lots[0].updated_at == published["projection_changed_at"]


def test_live_currency_edit_requires_published_successor_membership(
    db: Database,
) -> None:
    _install_ledger_chain(db, account_currency="GBP")
    _record_event(db, currency_code=None)
    raw = db.execute("SELECT * FROM raw.manual_investment_transactions").fetchall()
    before = InvestmentEventMembersRepo(db).active_members()[0]
    projected = _ledger_currency_and_updated_at(db)

    db.execute(
        "UPDATE prep.int_accounts__merged SET currency_code = 'EUR' WHERE account_id = ?",
        [_ACCOUNT_ID],
    )
    assert _ledger_currency_and_updated_at(db) == projected
    _publish_currency(db, "EUR")

    after = InvestmentEventMembersRepo(db).active_members()[0]
    assert after["membership_id"] != before["membership_id"]
    assert after["investment_transaction_id"] == before["investment_transaction_id"]
    assert after["bound_effective_currency"] == "EUR"
    assert _ledger_currency_and_updated_at(db) == (
        "EUR",
        after["projection_changed_at"],
    )
    assert after["projection_changed_at"] > before["projection_changed_at"]
    assert (
        db.execute("SELECT * FROM raw.manual_investment_transactions").fetchall() == raw
    )


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
