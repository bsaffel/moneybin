"""Identity cutover preserves populated storage and refuses unsafe activation."""

from importlib import import_module

import pytest

from moneybin.database import Database
from tests.moneybin.db_helpers import create_core_dim_stub_views
from tests.moneybin.migration_helpers import run_migration

_OLD_SECURITY_LINKS_SQL = """
CREATE TABLE app.security_links (
    link_id VARCHAR NOT NULL PRIMARY KEY,
    security_id VARCHAR NOT NULL,
    ref_kind VARCHAR NOT NULL CHECK (ref_kind IN (
        'plaid_security_id', 'institution_security_id', 'tiingo_ticker',
        'coingecko_slug')),
    ref_value VARCHAR NOT NULL,
    source_type VARCHAR NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('accepted', 'reversed')),
    decided_by VARCHAR NOT NULL CHECK (decided_by IN ('auto', 'user', 'system')),
    decided_at TIMESTAMP NOT NULL,
    reversed_at TIMESTAMP,
    reversed_by VARCHAR CHECK (
        reversed_by IS NULL OR reversed_by IN ('auto', 'user', 'system'))
)
"""


def _old_schema(db: Database) -> None:
    db.execute("DROP TABLE app.security_links")
    db.execute(_OLD_SECURITY_LINKS_SQL)
    db.execute("""INSERT INTO app.security_links VALUES
        ('link_one', 'security_one', 'plaid_security_id', 'native_one', 'plaid',
         'accepted', 'auto', TIMESTAMP '2026-01-01 12:34:56', NULL, NULL),
        ('link_two', 'security_two', 'tiingo_ticker', 'TEST', 'tiingo',
         'reversed', 'user', TIMESTAMP '2026-01-02 12:34:56', TIMESTAMP '2026-01-03 12:34:56', 'user'),
        ('link_three', 'security_three', 'coingecko_slug', 'example', 'coingecko',
         'accepted', 'system', TIMESTAMP '2026-01-04 12:34:56', NULL, NULL)""")
    db.execute("""INSERT INTO raw.manual_investment_transactions
        (source_transaction_id, import_id, account_id, security_id, type,
         trade_date, quantity, amount, created_by, investment_transaction_id)
        VALUES ('one', 'import_one', 'a', 'security_one', 'buy', DATE '2026-01-01', 3.25, -12.34, 'cli', 'itx_one'),
               ('two', 'import_one', 'a', 'security_one', 'sell', DATE '2026-02-01', -1.25, 5.67, 'cli', 'itx_two'),
               ('three', 'import_two', 'b', NULL, 'deposit', DATE '2026-01-01', NULL, 15.50, 'cli', 'itx_three')""")


def _migrate(db: Database) -> None:
    migration = import_module("moneybin.sql.migrations.V061__immutable_manual_identity")
    run_migration(db, migration.migrate)


def test_upgrade_widens_links_without_changing_raw_or_existing_links(
    db: Database,
) -> None:
    _old_schema(db)
    raw = db.execute(
        "SELECT * FROM raw.manual_investment_transactions ORDER BY source_transaction_id"
    ).fetchall()
    links = db.execute("SELECT * FROM app.security_links ORDER BY link_id").fetchall()
    _migrate(db)
    _migrate(db)
    assert (
        db.execute(
            "SELECT * FROM raw.manual_investment_transactions ORDER BY source_transaction_id"
        ).fetchall()
        == raw
    )
    assert (
        db.execute("SELECT * FROM app.security_links ORDER BY link_id").fetchall()
        == links
    )
    db.execute("""INSERT INTO app.security_links
        (link_id, security_id, ref_kind, ref_value, source_type, status, decided_by, decided_at)
        VALUES ('manual_link', 'security_two', 'manual_investment_transaction_id', 'one', 'manual', 'accepted', 'user', CURRENT_TIMESTAMP)""")


def test_historical_account_activation_with_unprovable_selections_refuses_upgrade(
    db: Database,
) -> None:
    _old_schema(db)
    db.execute("""INSERT INTO app.account_link_decisions
        (decision_id, provisional_account_id, candidate_account_id, status, decided_by, decided_at)
        VALUES ('historical_merge', 'a', 'b', 'accepted', 'user', CURRENT_TIMESTAMP)""")
    db.execute("""INSERT INTO app.lot_selections
        (investment_transaction_id, lot_id, quantity)
        VALUES ('itx_two', 'selected_old_lot', 1.25)""")
    raw = db.execute(
        "SELECT * FROM raw.manual_investment_transactions ORDER BY source_transaction_id"
    ).fetchall()
    links = db.execute("SELECT * FROM app.security_links ORDER BY link_id").fetchall()
    with pytest.raises(Exception, match="lot selection"):
        _migrate(db)
    assert (
        db.execute(
            "SELECT * FROM raw.manual_investment_transactions ORDER BY source_transaction_id"
        ).fetchall()
        == raw
    )
    assert (
        db.execute("SELECT * FROM app.security_links ORDER BY link_id").fetchall()
        == links
    )
    constraints = db.execute(
        "SELECT constraint_text FROM duckdb_constraints() WHERE schema_name = 'app' AND table_name = 'security_links'"
    ).fetchall()
    assert not any("manual_investment_transaction_id" in row[0] for row in constraints)


def test_historical_activation_cannot_ignore_survivor_selections(db: Database) -> None:
    _old_schema(db)
    create_core_dim_stub_views(db)
    db.execute("""INSERT INTO app.account_link_decisions
        (decision_id, provisional_account_id, candidate_account_id, status, decided_by, decided_at)
        VALUES ('historical_merge', 'a', 'b', 'accepted', 'user', CURRENT_TIMESTAMP)""")
    db.execute("""INSERT INTO core.fct_investment_transactions
        (investment_transaction_id, account_id, security_id, trade_date, type, quantity)
        VALUES ('survivor_sell', 'b', 'security_one', DATE '2026-02-01', 'sell', -1)""")
    db.execute("""INSERT INTO core.fct_investment_lots
        (lot_id, account_id, security_id, source_transaction_id)
        VALUES ('survivor_lot', 'b', 'security_one', 'survivor_buy')""")
    db.execute(
        "INSERT INTO app.lot_selections (investment_transaction_id, lot_id, quantity) VALUES ('survivor_sell', 'survivor_lot', 1)"
    )
    with pytest.raises(RuntimeError, match="lot selection"):
        _migrate(db)
