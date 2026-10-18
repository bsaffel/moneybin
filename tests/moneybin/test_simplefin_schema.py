"""Fresh schema and frozen migration agree on immutable SimpleFIN tables."""

import importlib

from moneybin.database import Database
from tests.moneybin.migration_helpers import run_migration


def test_simplefin_schema_and_migration_equivalence(db: Database) -> None:
    names = db.execute(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name='raw' AND table_name LIKE 'simplefin_%' ORDER BY table_name"
    ).fetchall()
    assert names == [
        (f"simplefin_{name}",)
        for name in [
            "accounts",
            "balances",
            "responses",
            "sync_receipts",
            "transactions",
        ]
    ]
    query = "SELECT table_name, column_name, data_type, is_nullable, column_default FROM duckdb_columns() WHERE schema_name='raw' AND table_name LIKE 'simplefin_%' ORDER BY table_name, column_index"
    before = db.execute(query).fetchall()
    constraints_query = """SELECT table_name, constraint_type, constraint_column_names
        FROM duckdb_constraints() WHERE schema_name = 'raw'
        AND table_name LIKE 'simplefin_%'
        ORDER BY table_name, constraint_type, constraint_column_names"""
    before_constraints = db.execute(constraints_query).fetchall()
    for (name,) in names:
        db.execute(f"DROP TABLE raw.{name}")  # catalog-owned test identifiers
    migration = importlib.import_module(
        "moneybin.sql.migrations.V075__add_simplefin_raw"
    )
    run_migration(db, migration.migrate)
    run_migration(db, migration.migrate)
    label_migration = importlib.import_module(
        "moneybin.sql.migrations.V077__add_simplefin_account_label"
    )
    run_migration(db, label_migration.migrate)
    run_migration(db, label_migration.migrate)
    institution_migration = importlib.import_module(
        "moneybin.sql.migrations.V078__add_simplefin_institution_label"
    )
    run_migration(db, institution_migration.migrate)
    run_migration(db, institution_migration.migrate)
    assert db.execute(query).fetchall() == before
    assert db.execute(constraints_query).fetchall() == before_constraints
