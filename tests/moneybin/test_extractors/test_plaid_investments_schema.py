"""Raw investment DDL: auto-discovered, snapshot-keyed, upsert-deduped."""

from moneybin.database import Database

_TABLES = [
    "raw.plaid_securities",
    "raw.plaid_investment_transactions",
    "raw.plaid_investment_holdings",
    "raw.plaid_investment_holding_lots",
]


def test_investment_raw_tables_exist(db: Database) -> None:
    for table in _TABLES:
        row = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()  # noqa: S608  # fixed table list
        assert row is not None and row[0] == 0


def test_snapshot_pk_scopes_by_origin_and_file(db: Database) -> None:
    ins = (
        "INSERT OR REPLACE INTO raw.plaid_investment_holdings "
        "(account_id, security_id, transactions_window_start, source_file, source_origin) "
        "VALUES (?, ?, ?, ?, ?)"
    )
    db.execute(ins, ["acc", "sec", "2024-07-08", "sync_j1", "item_1"])
    db.execute(
        ins, ["acc", "sec", "2025-01-15", "sync_j1", "item_2"]
    )  # same provider-local pair, other item
    db.execute(
        ins, ["acc", "sec", "2024-07-08", "sync_j2", "item_1"]
    )  # later snapshot is RETAINED
    db.execute(
        ins, ["acc", "sec", "2024-07-08", "sync_j1", "item_1"]
    )  # replay of same job replaces
    row = db.execute("SELECT COUNT(*) FROM raw.plaid_investment_holdings").fetchone()
    assert row is not None and row[0] == 3


def test_transactional_pk_replaces_across_jobs(db: Database) -> None:
    ins = (
        "INSERT OR REPLACE INTO raw.plaid_investment_transactions "
        "(investment_transaction_id, account_id, transaction_date, amount, source_file, source_origin) "
        "VALUES (?, ?, ?, ?, ?, ?)"
    )
    db.execute(ins, ["itx", "acc", "2026-07-06", "10.00", "sync_j1", "item_1"])
    db.execute(
        ins, ["itx", "acc", "2026-07-06", "10.00", "sync_j2", "item_1"]
    )  # re-delivery replaces
    rows = db.execute(
        "SELECT source_file FROM raw.plaid_investment_transactions"
    ).fetchall()
    assert rows == [("sync_j2",)]
