"""Verify raw.plaid_* tables are created by init_schemas."""

from moneybin.database import Database


def test_raw_plaid_tables_exist(db: Database) -> None:
    rows = db.execute(
        """
        SELECT table_name FROM duckdb_tables()
        WHERE schema_name = 'raw' AND table_name LIKE 'plaid_%'
        ORDER BY table_name
        """
    ).fetchall()
    assert [r[0] for r in rows] == [
        "plaid_accounts",
        "plaid_balances",
        "plaid_transactions",
    ]


def test_raw_plaid_transactions_primary_key(db: Database) -> None:
    """PK is (transaction_id, source_origin), not (transaction_id, source_file)."""
    db.execute(
        """
        INSERT INTO raw.plaid_transactions
            (transaction_id, account_id, transaction_date, amount,
             source_file, source_type, source_origin)
        VALUES
            ('txn_x', 'acc_a', '2026-04-07', 10.00, 'sync_1', 'plaid', 'item_a'),
            ('txn_x', 'acc_b', '2026-04-07', 20.00, 'sync_1', 'plaid', 'item_b')
        """  # noqa: S608  # test input, not executing dynamic SQL
    )
    row = db.execute(
        "SELECT COUNT(*) FROM raw.plaid_transactions WHERE transaction_id = 'txn_x'"
    ).fetchone()
    assert row is not None
    assert row[0] == 2
