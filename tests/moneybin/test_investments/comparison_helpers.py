"""Synthetic Source observations and real comparison model installation."""

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from sqlglot import exp
from sqlmesh.core.dialect import parse

from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from moneybin.repositories.account_links_repo import AccountLinksRepo


def seed_account_metadata(
    db: Database,
    account_id: str,
    *,
    native: str | None = None,
    origin: str = "fixture_metadata",
    currency: str | None = None,
) -> None:
    """Keep a synthetic canonical account available through real Core rebuilds."""
    native = native or account_id
    db.execute(
        """INSERT INTO raw.plaid_accounts (
            account_id, source_origin, account_type, account_subtype,
            name, institution_name, source_file, extracted_at
        ) VALUES (?, ?, 'investment', 'brokerage',
                  'Synthetic Brokerage', 'Synthetic Broker', 'fixture',
                  TIMESTAMPTZ '2026-01-01T00:00:00Z')
        ON CONFLICT DO NOTHING""",
        [native, origin],
    )
    if currency is not None:
        db.execute(
            """INSERT INTO raw.plaid_balances (
                account_id, balance_date, iso_currency_code, source_origin,
                source_file, extracted_at
            ) VALUES (?, DATE '2026-01-01', ?, ?, 'fixture',
                      TIMESTAMPTZ '2026-01-01T00:00:00Z')
            ON CONFLICT DO NOTHING""",
            [native, currency, origin],
        )
    if (
        db.execute(
            """SELECT 1 FROM app.account_links WHERE status = 'accepted'
        AND ref_kind = 'source_native' AND source_type = 'plaid'
        AND source_origin = ? AND ref_value = ?""",
            [origin, native],
        ).fetchone()
        is None
    ):
        AccountLinksRepo(db).insert(
            link_id=f"fixture_{origin}_{native}",
            account_id=account_id,
            ref_kind="source_native",
            ref_value=native,
            source_type="plaid",
            source_origin=origin,
            decided_by="user",
            actor="test",
        )


def install_comparison_models(db: Database) -> None:
    db.execute("CREATE SCHEMA IF NOT EXISTS prep")
    for name in [
        "int_manual__investment_identity",
        "stg_manual__investment_transactions",
        "stg_plaid__investment_revisions",
        "stg_plaid__investment_transactions",
        "int_investment_events__observations",
        "int_investment_events__legs",
        "int_investment_events__headers",
        "int_investment_events__evidence",
    ]:
        path = Path("src/moneybin/sqlmesh/models/prep") / f"{name}.sql"
        assert path.exists(), f"Missing comparison model: {name}"
        query = next(
            node
            for node in parse(path.read_text(), default_dialect="duckdb")
            if isinstance(node, exp.Query)
        )
        db.execute(
            f"CREATE OR REPLACE VIEW prep.{name} AS {query.sql(dialect='duckdb')}"
        )  # fixed local test model names


def seed_manual_event(
    db: Database,
    native: str,
    *,
    type_: str = "buy",
    subtype: str | None = None,
    group: str | None = None,
    quantity: str | None = "1",
    amount: str = "-100",
    price: str | None = "100",
    day: int = 10,
) -> None:
    db.execute(
        """INSERT INTO raw.manual_investment_transactions (
        source_transaction_id, import_id, account_id, security_id, type, subtype,
        event_group_id, trade_date, original_acquisition_date, quantity, price,
        amount, fees, currency_code, description, created_by, investment_transaction_id
    ) VALUES (?, 'import', 'account', 'security', ?, ?, ?, ?, NULL, ?, ?, ?, 0, NULL,
              'Manual context', 'cli', ?)""",
        [
            native,
            type_,
            subtype,
            group,
            date(2026, 1, day),
            quantity,
            price,
            amount,
            native,
        ],
    )


def seed_plaid_event(
    db: Database,
    native: str,
    *,
    type_: str = "buy",
    subtype: str = "buy",
    quantity: str | None = "1",
    amount: str = "100",
    day: int = 10,
    security_id: str | None = "native_security",
    explicit_date: bool = False,
    source_origin: str = "origin",
    currency: str | None = None,
) -> None:
    txn = SyncInvestmentTransaction(
        investment_transaction_id=native,
        account_id="native_account",
        provider_item_id=source_origin,
        security_id=security_id,
        iso_currency_code=currency,
        date=date(2026, 1, day),
        transaction_datetime=datetime(2026, 1, day, tzinfo=UTC)
        if explicit_date
        else None,
        name="Aggregator context",
        quantity=Decimal(quantity) if quantity else None,
        amount=Decimal(amount),
        price=Decimal("100"),
        fees=Decimal("0"),
        type=type_,
        subtype=subtype,
    )
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]
        [txn],
        f"sync_{native}",
        datetime(2026, 2, 1, tzinfo=UTC),
        datetime(2026, 2, 1, tzinfo=UTC),
    )
