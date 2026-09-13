"""Legacy investment evidence survives the revision/receipt migration."""

import importlib
from datetime import UTC, datetime
from pathlib import Path

from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from tests.moneybin.migration_helpers import run_migration


def test_legacy_transactions_and_receipts_preserve_all_values(db: Database) -> None:
    path = Path("src/moneybin/sql/migrations/V060__investment_observation_receipts.py")
    assert path.exists(), "Legacy investment revision migration is required"
    migrate = importlib.import_module(
        "moneybin.sql.migrations.V060__investment_observation_receipts"
    ).migrate
    db.execute("DROP TABLE raw.plaid_investment_transactions")
    db.execute("""CREATE TABLE raw.plaid_investment_transactions (
        investment_transaction_id VARCHAR NOT NULL, account_id VARCHAR NOT NULL,
        security_id VARCHAR, transaction_date DATE NOT NULL,
        transaction_datetime TIMESTAMP, transaction_name VARCHAR,
        quantity DECIMAL(28,10), amount DECIMAL(18,2) NOT NULL,
        price DECIMAL(28,10), fees DECIMAL(18,2), iso_currency_code VARCHAR,
        unofficial_currency_code VARCHAR, investment_transaction_type VARCHAR,
        investment_transaction_subtype VARCHAR, source_file VARCHAR NOT NULL,
        source_type VARCHAR NOT NULL DEFAULT 'plaid', source_origin VARCHAR NOT NULL,
        extracted_at TIMESTAMP, loaded_at TIMESTAMP,
        PRIMARY KEY(investment_transaction_id, source_origin))""")
    db.execute("""INSERT INTO raw.plaid_investment_transactions VALUES
        ('one', 'account', 'security', DATE '2026-01-02', TIMESTAMP '2026-01-01 18:00:00',
         'First observation', 2.123456789, 21.25, 10.001, 0.01, 'USD', NULL,
         'buy', 'buy', 'sync_z', 'plaid', 'origin',
         TIMESTAMP '2026-01-03 12:00:00', TIMESTAMP '2026-01-03 13:00:00'),
        ('two', 'account2', NULL, DATE '2026-02-02', NULL,
         'Cash observation', NULL, -15.12, NULL, 0.02, 'CAD', NULL,
         'cash', 'deposit', 'sync_a', 'plaid', 'origin',
         TIMESTAMP '2026-02-03 12:00:00', TIMESTAMP '2026-02-03 13:00:00'),
        ('three', 'account3', 'security3', DATE '2026-03-02', TIMESTAMP '2026-03-01 17:00:00',
         'Third observation', -3.25, -30.50, 9.3876, 0.03, NULL, 'BTC',
         'sell', 'sell', 'sync_b', 'plaid', 'origin2',
         TIMESTAMP '2026-03-03 12:00:00', TIMESTAMP '2026-03-03 13:00:00')""")
    before = db.execute(
        "SELECT * FROM raw.plaid_investment_transactions ORDER BY investment_transaction_id"
    ).fetchall()
    run_migration(db, migrate)
    after = db.execute("""SELECT t.investment_transaction_id, t.account_id,
        t.security_id, t.transaction_date, t.transaction_datetime, t.transaction_name,
        t.quantity, t.amount, t.price, t.fees, t.iso_currency_code,
        t.unofficial_currency_code, t.investment_transaction_type,
        t.investment_transaction_subtype, r.source_file, t.source_type,
        t.source_origin, r.extracted_at, r.loaded_at
        FROM raw.plaid_investment_transactions t
        JOIN raw.plaid_investment_transaction_receipts r
        USING (investment_transaction_id, source_origin, observation_version)
        ORDER BY investment_transaction_id""").fetchall()
    assert after == before
    receipts = db.execute(
        "SELECT * FROM raw.plaid_investment_transaction_receipts"
    ).fetchall()
    assert len(receipts) == 3
    run_migration(db, migrate)
    assert (
        db.execute("SELECT * FROM raw.plaid_investment_transaction_receipts").fetchall()
        == receipts
    )
    revisions = db.execute("SELECT * FROM raw.plaid_investment_transactions").fetchall()
    fields = [
        column[0]
        for column in db.execute(
            "SELECT * FROM raw.plaid_investment_transactions"
        ).description
    ]
    transactions: list[SyncInvestmentTransaction] = []
    for values in revisions:
        row = dict(zip(fields, values, strict=True))
        row["provider_item_id"] = row.pop("source_origin")
        row.pop("observation_version")
        row.pop("source_type")
        transactions.append(SyncInvestmentTransaction.model_validate(row))
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]
        transactions, "sync_redelivery", datetime.now(UTC), datetime.now(UTC)
    )
    assert (
        db.execute("SELECT * FROM raw.plaid_investment_transactions").fetchall()
        == revisions
    )


def test_legacy_holdings_receipts_backfill_prior_deterministic_order(
    db: Database,
) -> None:
    migrate = importlib.import_module(
        "moneybin.sql.migrations.V060__investment_observation_receipts"
    ).migrate
    db.execute(
        "ALTER TABLE raw.plaid_investment_holdings_snapshots DROP COLUMN ingestion_sequence"
    )
    db.execute("""INSERT INTO raw.plaid_investment_holdings_snapshots (
        source_origin, source_file, holdings_date, holdings_count,
        transactions_window_start, extracted_at, loaded_at
    ) VALUES
        ('origin', 'z', DATE '2026-01-01', 3, DATE '2024-01-01',
         TIMESTAMP '2026-01-01 12:00:00', TIMESTAMP '2026-01-01 13:00:00'),
        ('origin', 'a', DATE '2026-01-01', 0, DATE '2024-01-01',
         TIMESTAMP '2026-01-01 12:00:00', TIMESTAMP '2026-01-01 14:00:00'),
        ('other', 'b', DATE '2026-02-01', 2, DATE '2024-02-01',
         TIMESTAMP '2026-02-01 12:00:00', TIMESTAMP '2026-02-01 13:00:00')""")
    before = db.execute(
        "SELECT * FROM raw.plaid_investment_holdings_snapshots ORDER BY source_origin, source_file"
    ).fetchall()
    run_migration(db, migrate)
    assert (
        db.execute(
            "SELECT * EXCLUDE (ingestion_sequence) FROM raw.plaid_investment_holdings_snapshots ORDER BY source_origin, source_file"
        ).fetchall()
        == before
    )
    sequenced = db.execute(
        "SELECT source_origin, source_file, ingestion_sequence FROM raw.plaid_investment_holdings_snapshots ORDER BY ingestion_sequence"
    ).fetchall()
    assert [(row[0], row[1]) for row in sequenced] == [
        ("origin", "a"),
        ("origin", "z"),
        ("other", "b"),
    ]
    run_migration(db, migrate)
    assert (
        db.execute(
            "SELECT source_origin, source_file, ingestion_sequence FROM raw.plaid_investment_holdings_snapshots ORDER BY ingestion_sequence"
        ).fetchall()
        == sequenced
    )
