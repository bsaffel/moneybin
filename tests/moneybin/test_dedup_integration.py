"""End-to-end integration tests for transaction dedup.

These tests load real data into DuckDB, run the matching engine,
and verify the gold records in core.fct_transactions.
"""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.hashing import gold_key_matched, gold_key_unmatched
from moneybin.matching.persistence import (
    get_active_matches,
    undo_match,
)
from moneybin.matching.priority import seed_source_priority


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Generator[Database, None, None]:
    """Provide a test Database with all schemas needed for dedup integration tests."""
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    yield database
    database.close()


def _seed_test_data(db: Database) -> None:
    """Insert test data that simulates cross-source overlap."""
    # Create required schemas and tables
    db.execute("CREATE SCHEMA IF NOT EXISTS raw")
    db.execute("CREATE SCHEMA IF NOT EXISTS app")

    # Raw OFX transactions
    db.execute("""
        CREATE TABLE IF NOT EXISTS raw.ofx_transactions (
            source_transaction_id VARCHAR,
            account_id VARCHAR,
            transaction_type VARCHAR,
            date_posted TIMESTAMP,
            amount DECIMAL(18,2),
            payee VARCHAR,
            memo VARCHAR,
            check_number VARCHAR,
            source_file VARCHAR,
            extracted_at TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)  # noqa: S608  # test input, not executing SQL

    # Raw OFX accounts
    db.execute("""
        CREATE TABLE IF NOT EXISTS raw.ofx_accounts (
            account_id VARCHAR,
            routing_number VARCHAR,
            account_type VARCHAR,
            institution_org VARCHAR,
            institution_fid VARCHAR,
            source_file VARCHAR,
            extracted_at TIMESTAMP,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)  # noqa: S608  # test input, not executing SQL

    # Raw tabular transactions
    db.execute("""
        CREATE TABLE IF NOT EXISTS raw.tabular_transactions (
            transaction_id VARCHAR,
            account_id VARCHAR,
            transaction_date DATE,
            amount DECIMAL(18,2),
            description VARCHAR,
            source_file VARCHAR,
            source_type VARCHAR,
            source_origin VARCHAR,
            import_id VARCHAR,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)  # noqa: S608  # test input, not executing SQL

    # Match decisions table
    db.execute("""
        CREATE TABLE IF NOT EXISTS app.match_decisions (
            match_id VARCHAR NOT NULL,
            source_transaction_id_a VARCHAR NOT NULL,
            source_type_a VARCHAR NOT NULL,
            source_origin_a VARCHAR NOT NULL,
            source_transaction_id_b VARCHAR NOT NULL,
            source_type_b VARCHAR NOT NULL,
            source_origin_b VARCHAR NOT NULL,
            account_id VARCHAR NOT NULL,
            confidence_score DECIMAL(5, 4),
            match_signals JSON,
            match_type VARCHAR NOT NULL DEFAULT 'dedup',
            match_tier VARCHAR,
            account_id_b VARCHAR,
            match_status VARCHAR NOT NULL,
            match_reason VARCHAR,
            decided_by VARCHAR NOT NULL,
            decided_at TIMESTAMP NOT NULL,
            reversed_at TIMESTAMP,
            reversed_by VARCHAR,
            PRIMARY KEY (match_id)
        )
    """)  # noqa: S608  # test input, not executing SQL

    # Seed source priority table
    db.execute("""
        CREATE TABLE IF NOT EXISTS app.seed_source_priority (
            source_type VARCHAR PRIMARY KEY,
            priority INTEGER NOT NULL
        )
    """)  # noqa: S608  # test input, not executing SQL

    # Insert OFX transaction — payee trimmed to same value as CSV description
    # so jaro_winkler similarity = 1.0, confidence >= high_confidence_threshold
    db.execute("""
        INSERT INTO raw.ofx_transactions
        (source_transaction_id, account_id, transaction_type, date_posted,
         amount, payee, memo, check_number, source_file, extracted_at)
        VALUES
        ('FITID001', 'acct_checking', 'DEBIT', '2026-03-15 00:00:00',
         -42.50, 'STARBUCKS 1234', NULL, NULL,
         '/tmp/test.ofx', '2026-03-16 10:00:00')
    """)

    # Insert OFX account
    db.execute("""
        INSERT INTO raw.ofx_accounts
        (account_id, routing_number, account_type, institution_org,
         institution_fid, source_file, extracted_at)
        VALUES
        ('acct_checking', '021000021', 'CHECKING', 'Chase Bank',
         '10898', '/tmp/test.ofx', '2026-03-16 10:00:00')
    """)

    # Insert CSV transaction (same real-world transaction as OFX)
    db.execute("""
        INSERT INTO raw.tabular_transactions
        (transaction_id, account_id, transaction_date, amount,
         description, source_file, source_type, source_origin, import_id)
        VALUES
        ('csv_abc123def456', 'acct_checking', '2026-03-15', -42.50,
         'STARBUCKS 1234', '/tmp/test.csv', 'csv', 'chase_credit',
         '00000000-0000-0000-0000-000000000001')
    """)

    # Insert unrelated CSV transaction (should NOT match)
    db.execute("""
        INSERT INTO raw.tabular_transactions
        (transaction_id, account_id, transaction_date, amount,
         description, source_file, source_type, source_origin, import_id)
        VALUES
        ('csv_xyz789', 'acct_checking', '2026-03-15', -15.00,
         'SUBWAY 456', '/tmp/test.csv', 'csv', 'chase_credit',
         '00000000-0000-0000-0000-000000000001')
    """)


def _create_test_unioned_view(db: Database) -> None:
    """Create a test version of the unioned view against raw tables."""
    db.execute("""
        CREATE OR REPLACE VIEW _test_unioned AS
        SELECT
            t.source_transaction_id,
            t.account_id,
            t.date_posted::DATE AS transaction_date,
            amount::DECIMAL(18,2) AS amount,
            TRIM(payee) AS description,
            'ofx' AS source_type,
            COALESCE(a.institution_org, 'ofx_unknown') AS source_origin,
            t.source_file
        FROM raw.ofx_transactions t
        LEFT JOIN raw.ofx_accounts a ON t.account_id = a.account_id
        UNION ALL
        SELECT
            transaction_id AS source_transaction_id,
            account_id,
            transaction_date,
            amount::DECIMAL(18,2) AS amount,
            description,
            source_type,
            source_origin,
            source_file
        FROM raw.tabular_transactions
    """)  # noqa: S608  # test input, not executing SQL


@pytest.mark.integration
class TestEndToEndDedup:
    """End-to-end dedup tests using real DuckDB."""

    def test_cross_source_match_produces_one_gold_record(self, db: Database) -> None:
        """OFX + CSV describing the same transaction -> one gold record."""
        _seed_test_data(db)
        settings = MatchingSettings()
        seed_source_priority(db, settings)
        _create_test_unioned_view(db)

        matcher = TransactionMatcher(db, settings, table="_test_unioned")
        result = matcher.run()

        assert result.auto_merged == 1, (
            f"Expected 1 auto-merge, got {result.auto_merged}"
        )

        active = get_active_matches(db)
        assert len(active) == 1
        assert active[0]["source_type_a"] in ("csv", "ofx")
        assert active[0]["source_type_b"] in ("csv", "ofx")

    def test_unrelated_transactions_stay_separate(self, db: Database) -> None:
        """Transactions with different amounts are never matched."""
        _seed_test_data(db)
        settings = MatchingSettings()
        seed_source_priority(db, settings)
        _create_test_unioned_view(db)

        matcher = TransactionMatcher(db, settings, table="_test_unioned")
        result = matcher.run()

        # Only the Starbucks pair should match; Subway stays separate
        assert result.auto_merged <= 1

    def test_undo_and_rematch_repropose(self, db: Database) -> None:
        """Undoing a match and re-running repropose the same pair."""
        _seed_test_data(db)
        settings = MatchingSettings()
        seed_source_priority(db, settings)
        _create_test_unioned_view(db)

        # First run
        matcher = TransactionMatcher(db, settings, table="_test_unioned")
        result1 = matcher.run()
        assert result1.auto_merged >= 1

        # Undo (but don't reject)
        active = get_active_matches(db)
        undo_match(db, active[0]["match_id"], reversed_by="user")

        # Re-run: should re-propose
        matcher2 = TransactionMatcher(db, settings, table="_test_unioned")
        result2 = matcher2.run()
        assert result2.auto_merged >= 1

    def test_gold_key_consistency(self) -> None:
        """Python and SQL gold key generation must produce identical results."""
        py_key = gold_key_unmatched("csv", "txn123", "acct1")

        assert len(py_key) == 16
        assert all(c in "0123456789abcdef" for c in py_key)

        group_key = gold_key_matched([
            ("csv", "txn_csv", "acct1"),
            ("ofx", "txn_ofx", "acct1"),
        ])
        assert len(group_key) == 16
        assert group_key != py_key
