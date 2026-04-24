"""Tests for TransactionMatcher orchestrator."""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.engine import MatchResult, TransactionMatcher


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Generator[Database, None, None]:
    """Provide a test Database instance scoped to this module."""
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    yield database
    database.close()


def _create_test_table(db: Database) -> None:
    """Create a minimal unioned-style table for engine tests."""
    db.execute("""
        CREATE SCHEMA IF NOT EXISTS app;
    """)
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
    """)
    db.execute("""
        CREATE OR REPLACE TABLE _test_unioned (
            source_transaction_id VARCHAR,
            account_id VARCHAR,
            transaction_date DATE,
            amount DECIMAL(18, 2),
            description VARCHAR,
            source_type VARCHAR,
            source_origin VARCHAR,
            source_file VARCHAR
        )
    """)


def _insert(
    db: Database,
    stid: str,
    acct: str,
    txn_date: str,
    amount: str,
    desc: str,
    stype: str,
    sorigin: str,
    sfile: str = "test.csv",
) -> None:
    db.execute(
        """
        INSERT INTO _test_unioned VALUES (?, ?, ?::DATE, ?::DECIMAL(18,2), ?, ?, ?, ?)
        """,
        [stid, acct, txn_date, amount, desc, stype, sorigin, sfile],
    )


class TestTransactionMatcher:
    """Tests for the TransactionMatcher orchestrator."""

    def test_no_data_no_matches(self, db: Database) -> None:
        _create_test_table(db)
        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()
        assert isinstance(result, MatchResult)
        assert result.auto_merged == 0
        assert result.pending_review == 0

    def test_cross_source_auto_merge(self, db: Database) -> None:
        _create_test_table(db)
        _insert(
            db,
            "csv_a",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS #1234",
            "csv",
            "chase",
        )
        _insert(
            db,
            "ofx_b",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS 1234",
            "ofx",
            "chase_ofx",
        )
        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()
        assert result.auto_merged == 1
        assert result.pending_review == 0

    def test_low_confidence_goes_to_review(self, db: Database) -> None:
        _create_test_table(db)
        _insert(
            db,
            "csv_a",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS COFFEE",
            "csv",
            "chase",
        )
        _insert(
            db,
            "ofx_b",
            "acct1",
            "2026-03-17",
            "-42.50",
            "SB CAFE NYC",
            "ofx",
            "chase_ofx",
        )
        settings = MatchingSettings(
            high_confidence_threshold=0.95, review_threshold=0.10
        )
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()
        # Same amount, date within window, low description similarity + date offset
        # → confidence below auto-merge (0.95) but above review threshold (0.10)
        assert result.pending_review >= 1
        assert result.auto_merged == 0

    def test_rejected_pairs_not_reproposed(self, db: Database) -> None:
        _create_test_table(db)
        _insert(
            db, "csv_a", "acct1", "2026-03-15", "-42.50", "STARBUCKS", "csv", "chase"
        )
        _insert(
            db,
            "ofx_b",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS",
            "ofx",
            "chase_ofx",
        )
        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")

        # First run: auto-merge
        result1 = matcher.run()
        assert result1.auto_merged == 1

        # Undo and reject
        from moneybin.matching.persistence import (
            get_active_matches,
            undo_match,
            update_match_status,
        )

        matches = get_active_matches(db)
        undo_match(db, matches[0]["match_id"], reversed_by="user")
        update_match_status(
            db, matches[0]["match_id"], status="rejected", decided_by="user"
        )

        # Second run: should not re-propose
        result2 = matcher.run()
        assert result2.auto_merged == 0
        assert result2.pending_review == 0

    def test_match_result_summary(self) -> None:
        result = MatchResult(auto_merged=5, pending_review=2)
        assert "5 auto-merged" in result.summary()
        assert "2 pending review" in result.summary()
