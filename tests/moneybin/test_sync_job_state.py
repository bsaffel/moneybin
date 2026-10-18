"""Sync operational recovery is durable, additive and privacy protected."""

import importlib

from moneybin.database import Database
from moneybin.privacy.sql_query import execute_sql_query
from tests.moneybin.migration_helpers import run_migration


def test_sync_job_state_schema_matches_additive_migration(db: Database) -> None:
    """Fresh and upgraded profiles must support the identical recovery phases."""
    assert db.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE schema_name='app' AND table_name='sync_job_state'"
    ).fetchone() == (1,), "durable sync recovery table is missing"
    query = "SELECT column_name, data_type, is_nullable, column_default FROM duckdb_columns() WHERE schema_name='app' AND table_name='sync_job_state' ORDER BY column_index"
    fresh = db.execute(query).fetchall()
    db.execute("DROP TABLE app.sync_job_state")
    migration = importlib.import_module(
        "moneybin.sql.migrations.V079__add_sync_job_state"
    )
    run_migration(db, migration.migrate)
    run_migration(db, migration.migrate)
    assert db.execute(query).fetchall() == fresh


def test_nested_job_evidence_is_whole_value_protected(db: Database) -> None:
    """A raw native identifier in recovery evidence must not leak through SQL."""
    assert db.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE schema_name='app' AND table_name='sync_job_state'"
    ).fetchone() == (1,), "durable sync recovery table is missing"
    db.execute("""INSERT INTO app.sync_job_state (job_id, consumer_id, job_details)
                  VALUES ('00000000-0000-0000-0000-000000000001', 'synthetic-profile',
                  '{"native":{"account_id":"12-34-56"}}')""")
    result = execute_sql_query(
        db, "SELECT job_details FROM app.sync_job_state", max_rows=10
    )
    assert result.records == [{"job_details": "*****"}]
