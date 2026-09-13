"""The real upgrade runner composes currency shape and immutable identity cutover."""

from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from moneybin.database import Database
from moneybin.migrations import MigrationRunner
from tests.moneybin.db_helpers import create_core_dim_stub_views
from tests.moneybin.test_migration_v059 import pre_v059_db as pre_v059_db
from tests.moneybin.test_migration_v061 import seed_legacy_manual_identity_schema
from tests.moneybin.test_services.test_manual_identity_projection import (
    add_account_edge,
    add_account_terminal,
)

_FILES = (
    "V059__add_currency_conversion_shape.py",
    "V060__investment_observation_receipts.py",
    "V061__immutable_manual_identity.py",
)
_IDENTITY_TABLES = (
    "raw.manual_investment_transactions",
    "app.security_links",
    "app.account_links",
    "app.account_link_decisions",
    "app.lot_selections",
    "app.audit_log",
)
_INVESTMENT_TABLES = (
    "raw.plaid_investment_transactions",
    "raw.plaid_investment_transaction_receipts",
    "raw.plaid_investment_holdings_snapshots",
)


def _snapshot(
    db: Database, tables: tuple[str, ...]
) -> dict[str, list[tuple[Any, ...]]]:
    return {
        table: db.execute(
            f"SELECT * FROM {table} ORDER BY 1, 2"  # noqa: S608  # closed test table lists
        ).fetchall()
        for table in tables
    }


@pytest.fixture
def upgrade_runner(pre_v059_db: Database, tmp_path: Path) -> MigrationRunner:
    """Populate the three legacy shapes and discover only the shipped upgrade tail."""
    db = pre_v059_db
    seed_legacy_manual_identity_schema(db)
    add_account_terminal(db, "a")
    add_account_terminal(db, "b")
    db.execute("""CREATE TABLE raw.legacy_investment_transactions AS
        SELECT * EXCLUDE (observation_version),
               'sync_old'::VARCHAR AS source_file,
               TIMESTAMP '2026-01-03 12:00:00' AS extracted_at,
               TIMESTAMP '2026-01-03 13:00:00' AS loaded_at
        FROM raw.plaid_investment_transactions WHERE FALSE""")
    db.execute("DROP TABLE raw.plaid_investment_transactions")
    db.execute(
        "ALTER TABLE raw.legacy_investment_transactions RENAME TO plaid_investment_transactions"
    )
    db.execute("""INSERT INTO raw.plaid_investment_transactions
        (investment_transaction_id, account_id, security_id, transaction_date,
         quantity, amount, price, fees, iso_currency_code,
         investment_transaction_type, investment_transaction_subtype,
         source_type, source_origin, source_file, extracted_at, loaded_at)
        VALUES ('legacy_buy', 'native_a', 'native_security', DATE '2026-01-02',
                3.25, 32.50, 10, 0, 'USD', 'buy', 'buy', 'plaid', 'item',
                'sync_z', TIMESTAMP '2026-01-03 12:00:00',
                TIMESTAMP '2026-01-03 13:00:00')""")
    db.execute(
        "ALTER TABLE raw.plaid_investment_holdings_snapshots DROP COLUMN ingestion_sequence"
    )
    db.execute("""INSERT INTO raw.plaid_investment_holdings_snapshots
        (source_origin, source_file, holdings_date, holdings_count,
         transactions_window_start, extracted_at, loaded_at)
        VALUES ('item', 'z', DATE '2026-01-03', 1, DATE '2025-01-01',
                TIMESTAMP '2026-01-03 12:00:00', TIMESTAMP '2026-01-03 13:00:00'),
               ('item', 'a', DATE '2026-01-03', 0, DATE '2025-01-01',
                TIMESTAMP '2026-01-03 12:00:00', TIMESTAMP '2026-01-03 14:00:00')""")
    for filename in _FILES:
        source = Path("src/moneybin/sql/migrations") / filename
        (tmp_path / filename).write_bytes(source.read_bytes())
    return MigrationRunner(db, migrations_dir=tmp_path)


@pytest.mark.parametrize("history", ["all_pending", "missing_059_after_060_061"])
def test_runner_preserves_populated_upgrade_and_replay(
    pre_v059_db: Database, upgrade_runner: MigrationRunner, history: str
) -> None:
    db = pre_v059_db
    identity = _snapshot(db, _IDENTITY_TABLES)
    legacy = db.execute("""SELECT investment_transaction_id, account_id, security_id,
        transaction_date, quantity, amount, price, fees, iso_currency_code,
        source_origin, source_file, extracted_at, loaded_at
        FROM raw.plaid_investment_transactions""").fetchall()
    holdings = db.execute(
        "SELECT * FROM raw.plaid_investment_holdings_snapshots ORDER BY 1, 2"
    ).fetchall()
    later_state = None
    if history == "missing_059_after_060_061":
        for migration in upgrade_runner.pending():
            if migration.version in (60, 61):
                upgrade_runner.apply_one(migration)
        later_state = _snapshot(db, _INVESTMENT_TABLES)
        assert [migration.version for migration in upgrade_runner.pending()] == [59]
    else:
        assert [migration.version for migration in upgrade_runner.pending()] == [
            59,
            60,
            61,
        ]

    result = upgrade_runner.apply_all()
    assert not result.failed, result.error_message
    assert result.applied_count == (3 if history == "all_pending" else 1)
    assert db.execute(
        "SELECT version, success FROM app.schema_migrations ORDER BY version"
    ).fetchall() == [(59, True), (60, True), (61, True)]
    assert _snapshot(db, _IDENTITY_TABLES) == identity
    for source in ("ofx", "tabular", "plaid", "manual"):
        assert db.execute(
            f"SELECT source_transaction_id, amount, to_amount, to_currency FROM raw.{source}_transactions"  # noqa: S608  # closed test source names
        ).fetchall() == [("txn-before-v059", Decimal("125.50"), None, None)]
    assert (
        db.execute("""SELECT t.investment_transaction_id, t.account_id,
        t.security_id, t.transaction_date, t.quantity, t.amount, t.price, t.fees,
        t.iso_currency_code, t.source_origin, r.source_file, r.extracted_at, r.loaded_at
        FROM raw.plaid_investment_transactions AS t
        JOIN raw.plaid_investment_transaction_receipts AS r
        USING (investment_transaction_id, source_origin, observation_version)
    """).fetchall()
        == legacy
    )
    assert (
        db.execute(
            "SELECT * EXCLUDE (ingestion_sequence) FROM raw.plaid_investment_holdings_snapshots ORDER BY 1, 2"
        ).fetchall()
        == holdings
    )
    assert db.execute(
        "SELECT source_file FROM raw.plaid_investment_holdings_snapshots ORDER BY ingestion_sequence"
    ).fetchall() == [("a",), ("z",)]
    assert any(
        "manual_investment_transaction_id" in row[0]
        for row in db.execute(
            "SELECT constraint_text FROM duckdb_constraints() WHERE schema_name = 'app' AND table_name = 'security_links'"
        ).fetchall()
    )
    if later_state is not None:
        assert _snapshot(db, _INVESTMENT_TABLES) == later_state
    applied = _snapshot(db, (*_INVESTMENT_TABLES, "app.schema_migrations"))
    replay = upgrade_runner.apply_all()
    assert not replay.failed, replay.error_message
    assert replay.applied_count == 0
    assert _snapshot(db, (*_INVESTMENT_TABLES, "app.schema_migrations")) == applied
    assert _snapshot(db, _IDENTITY_TABLES) == identity


@pytest.mark.parametrize("selected_position", ["donor", "survivor"])
def test_runner_refusal_preserves_prior_steps_and_blocks_same_code_retry(
    pre_v059_db: Database,
    upgrade_runner: MigrationRunner,
    selected_position: str,
) -> None:
    db = pre_v059_db
    create_core_dim_stub_views(db)
    add_account_edge(db, "a", "b")
    account = "a" if selected_position == "donor" else "b"
    db.execute(
        """INSERT INTO core.fct_investment_transactions
        (investment_transaction_id, account_id, security_id, trade_date, type, quantity)
        VALUES ('selected_sale', ?, 'security_one', DATE '2026-02-01', 'sell', -1)""",
        [account],
    )
    db.execute(
        """INSERT INTO core.fct_investment_lots
        (lot_id, account_id, security_id, source_transaction_id)
        VALUES ('selected_lot', ?, 'security_one', 'selected_buy')""",
        [account],
    )
    db.execute("""INSERT INTO app.lot_selections
        (investment_transaction_id, lot_id, quantity)
        VALUES ('selected_sale', 'selected_lot', 1)""")
    before = _snapshot(db, _IDENTITY_TABLES)
    constraints = db.execute(
        "SELECT constraint_text FROM duckdb_constraints() WHERE schema_name = 'app' AND table_name = 'security_links' ORDER BY constraint_text"
    ).fetchall()
    result = upgrade_runner.apply_all()
    assert result.failed
    assert result.applied_count == 2
    assert result.failed_migration == _FILES[2]
    assert "lot selections" in (result.error_message or "")
    assert db.execute(
        "SELECT version, success FROM app.schema_migrations ORDER BY version"
    ).fetchall() == [(59, True), (60, True), (61, False)]
    assert _snapshot(db, _IDENTITY_TABLES) == before
    assert (
        db.execute(
            "SELECT constraint_text FROM duckdb_constraints() WHERE schema_name = 'app' AND table_name = 'security_links' ORDER BY constraint_text"
        ).fetchall()
        == constraints
    )
    assert db.execute(
        "SELECT COUNT(*) FROM raw.plaid_investment_transaction_receipts"
    ).fetchone() == (1,)
    failed_state = _snapshot(db, (*_INVESTMENT_TABLES, "app.schema_migrations"))
    retry = upgrade_runner.apply_all()
    assert retry.failed
    assert retry.applied_count == 0
    assert "same code" in (retry.error_message or "")
    assert _snapshot(db, (*_INVESTMENT_TABLES, "app.schema_migrations")) == failed_state
    assert _snapshot(db, _IDENTITY_TABLES) == before
