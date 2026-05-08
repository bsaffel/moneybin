"""Integration test: app.merchants table → view migration preserves data."""

from __future__ import annotations

import duckdb
import pytest

pytestmark = pytest.mark.integration

_USER_MERCHANTS_DDL = """
    CREATE TABLE IF NOT EXISTS app.user_merchants (
        merchant_id   VARCHAR PRIMARY KEY,
        raw_pattern   VARCHAR NOT NULL,
        match_type    VARCHAR NOT NULL,
        canonical_name VARCHAR NOT NULL,
        category      VARCHAR,
        subcategory   VARCHAR,
        created_by    VARCHAR NOT NULL,
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""

_MERCHANTS_TABLE_DDL = """
    CREATE TABLE app.merchants (
        merchant_id   VARCHAR PRIMARY KEY,
        raw_pattern   VARCHAR NOT NULL,
        match_type    VARCHAR NOT NULL,
        canonical_name VARCHAR NOT NULL,
        category      VARCHAR,
        subcategory   VARCHAR,
        created_by    VARCHAR NOT NULL,
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""


def _table_type(conn: duckdb.DuckDBPyConnection, name: str) -> str | None:
    row = conn.execute(
        "SELECT table_type FROM information_schema.tables "
        "WHERE table_schema='app' AND table_name=?",
        [name],
    ).fetchone()
    return row[0] if row else None


def test_migration_preserves_existing_merchants(tmp_path: object) -> None:
    """Pre-migration app.merchants rows survive the split into app.user_merchants."""
    from moneybin.sql.migrations.V006__migrate_app_merchants_to_user_merchants import (
        migrate,
    )

    db_path = str(tmp_path / "premigration.db")  # type: ignore[operator]
    with duckdb.connect(db_path) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS app")
        conn.execute(_MERCHANTS_TABLE_DDL)
        conn.execute(_USER_MERCHANTS_DDL)
        conn.executemany(
            "INSERT INTO app.merchants VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
            [
                [
                    "m_test_1",
                    "STARBUCKS",
                    "contains",
                    "Starbucks",
                    "Food & Dining",
                    "Coffee Shops",
                    "ai",
                ],
                [
                    "m_test_2",
                    "AMZN MKTP",
                    "contains",
                    "Amazon",
                    "Shopping",
                    "Online",
                    "user",
                ],
            ],
        )

        migrate(conn)

        rows = conn.execute(
            "SELECT merchant_id, raw_pattern, canonical_name, category, created_by "
            "FROM app.user_merchants ORDER BY merchant_id"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][0] == "m_test_1"
        assert rows[0][1] == "STARBUCKS"
        assert rows[0][4] == "ai"
        assert rows[1][0] == "m_test_2"
        assert rows[1][4] == "user"

        # Old table is dropped; seeds.refresh_views() creates the VIEW later.
        assert _table_type(conn, "merchants") is None


def test_migration_idempotent_when_already_a_view(tmp_path: object) -> None:
    """Re-running migrate() when app.merchants is already a VIEW is a no-op."""
    from moneybin.sql.migrations.V006__migrate_app_merchants_to_user_merchants import (
        migrate,
    )

    db_path = str(tmp_path / "postmigration.db")  # type: ignore[operator]
    with duckdb.connect(db_path) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS app")
        conn.execute(_USER_MERCHANTS_DDL)
        conn.execute(
            "INSERT INTO app.user_merchants VALUES "
            "('m_existing', 'NETFLIX', 'contains', 'Netflix', 'Entertainment', 'Streaming', 'user', CURRENT_TIMESTAMP)"
        )
        # Simulate post-migration: app.merchants is a VIEW over user_merchants.
        conn.execute("CREATE VIEW app.merchants AS SELECT * FROM app.user_merchants")

        migrate(conn)  # must not raise or alter data

        assert _table_type(conn, "merchants") == "VIEW"
        count_row = conn.execute("SELECT COUNT(*) FROM app.user_merchants").fetchone()
        assert count_row is not None
        assert count_row[0] == 1


def test_migration_no_op_on_fresh_install(tmp_path: object) -> None:
    """migrate() on a DB with no app.merchants is a clean no-op."""
    from moneybin.sql.migrations.V006__migrate_app_merchants_to_user_merchants import (
        migrate,
    )

    db_path = str(tmp_path / "fresh.db")  # type: ignore[operator]
    with duckdb.connect(db_path) as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS app")
        # Neither table nor view exists — fresh install state.
        migrate(conn)

        assert _table_type(conn, "merchants") is None
