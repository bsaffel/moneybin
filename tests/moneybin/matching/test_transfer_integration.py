"""Integration tests for the transfer detection pipeline."""

import json
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.persistence import (
    get_active_matches,
    get_pending_matches,
    undo_match,
    update_match_status,
)


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Generator[Database, None, None]:
    """Provide a test Database with match_decisions table."""
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    yield database
    database.close()


def _setup_tables(db: Database) -> None:
    """Create the unioned table and match_decisions for integration tests."""
    db.execute("CREATE SCHEMA IF NOT EXISTS app")
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
    stype: str = "csv",
    sorigin: str = "bank",
    sfile: str = "test.csv",
) -> None:
    db.execute(
        """
        INSERT INTO _test_unioned VALUES (?, ?, ?::DATE, ?::DECIMAL(18,2), ?, ?, ?, ?)
        """,
        [stid, acct, txn_date, amount, desc, stype, sorigin, sfile],
    )


@pytest.mark.integration
class TestTransferPipeline:
    """End-to-end transfer detection tests."""

    def test_same_day_same_institution_transfer(self, db: Database) -> None:
        """Happy path: checking->savings, same day, transfer keywords."""
        _setup_tables(db)
        _insert(
            db,
            "csv_chk1",
            "checking",
            "2026-03-15",
            "-500.00",
            "ONLINE TRANSFER TO SAV",
        )
        _insert(
            db,
            "csv_sav1",
            "savings",
            "2026-03-15",
            "500.00",
            "TRANSFER FROM CHK",
        )

        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()

        assert result.pending_transfers == 1
        pending = get_pending_matches(db, match_type="transfer")
        assert len(pending) == 1
        assert pending[0]["account_id"] == "checking"
        assert pending[0]["account_id_b"] == "savings"
        assert pending[0]["match_type"] == "transfer"

        signals = json.loads(pending[0]["match_signals"])
        assert "date_distance" in signals
        assert "keyword" in signals
        assert "roundness" in signals
        assert "pair_frequency" in signals

    def test_cross_institution_ach_with_date_offset(self, db: Database) -> None:
        """Cross-institution ACH with 2-day offset, different descriptions."""
        _setup_tables(db)
        _insert(
            db,
            "csv_chk1",
            "chase_checking",
            "2026-03-15",
            "-1000.00",
            "ACH TRANSFER TO ALLY",
        )
        _insert(
            db,
            "csv_sav1",
            "ally_savings",
            "2026-03-17",
            "1000.00",
            "ACH TRANSFER FROM EXTERNAL",
        )

        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()

        assert result.pending_transfers == 1
        pending = get_pending_matches(db, match_type="transfer")
        assert len(pending) == 1
        assert pending[0]["confidence_score"] > 0

    def test_review_accept_flow(self, db: Database) -> None:
        """Accept a transfer pair, verify it appears in active matches."""
        _setup_tables(db)
        _insert(db, "csv_chk1", "checking", "2026-03-15", "-500.00", "TRANSFER")
        _insert(db, "csv_sav1", "savings", "2026-03-15", "500.00", "TRANSFER")

        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        matcher.run()

        pending = get_pending_matches(db, match_type="transfer")
        assert len(pending) == 1

        update_match_status(
            db, pending[0]["match_id"], status="accepted", decided_by="user"
        )

        active = get_active_matches(db, match_type="transfer")
        assert len(active) == 1

    def test_undo_flow(self, db: Database) -> None:
        """Accept a transfer, undo it, verify restored to independent status."""
        _setup_tables(db)
        _insert(db, "csv_chk1", "checking", "2026-03-15", "-500.00", "TRANSFER")
        _insert(db, "csv_sav1", "savings", "2026-03-15", "500.00", "TRANSFER")

        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        matcher.run()

        pending = get_pending_matches(db, match_type="transfer")
        update_match_status(
            db,
            pending[0]["match_id"],
            status="accepted",
            decided_by="user",
        )

        active = get_active_matches(db, match_type="transfer")
        assert len(active) == 1

        undo_match(db, active[0]["match_id"], reversed_by="user")
        active_after = get_active_matches(db, match_type="transfer")
        assert len(active_after) == 0

        # Re-running the matcher should re-propose
        result2 = matcher.run()
        assert result2.pending_transfers == 1

    def test_recurring_monthly_transfers(self, db: Database) -> None:
        """3 months of $500 checking->savings; greedy pairs same-day, not cross-month."""
        _setup_tables(db)
        for month in ["01", "02", "03"]:
            _insert(
                db,
                f"csv_chk_{month}",
                "checking",
                f"2026-{month}-15",
                "-500.00",
                "MONTHLY TRANSFER",
            )
            _insert(
                db,
                f"csv_sav_{month}",
                "savings",
                f"2026-{month}-15",
                "500.00",
                "MONTHLY TRANSFER",
            )

        settings = MatchingSettings(date_window_days=3)
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()

        # Each month should pair with its own counterpart
        assert result.pending_transfers == 3
        pending = get_pending_matches(db, match_type="transfer")
        assert len(pending) == 3

    def test_false_positive_coincidental_amount(self, db: Database) -> None:
        """$100 electric bill and $100 refund -- same amount, not a transfer."""
        _setup_tables(db)
        _insert(
            db,
            "csv_chk1",
            "checking",
            "2026-03-15",
            "-100.00",
            "ELECTRIC COMPANY PAYMENT",
        )
        _insert(
            db,
            "csv_sav1",
            "savings",
            "2026-03-15",
            "100.00",
            "INTEREST PAYMENT",
        )

        settings = MatchingSettings(transfer_review_threshold=0.85)
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        matcher.run()

        # Low keyword score + high threshold should filter this out
        pending = get_pending_matches(db, match_type="transfer")
        for p in pending:
            signals = json.loads(p["match_signals"])
            assert signals["keyword"] == 0.0

    def test_multiple_candidates_best_match_wins(self, db: Database) -> None:
        """$200 debit with two $200 credits; best match (same-day) wins."""
        _setup_tables(db)
        _insert(
            db,
            "csv_chk1",
            "checking",
            "2026-03-15",
            "-200.00",
            "TRANSFER TO SAVINGS",
        )
        _insert(
            db,
            "csv_sav1",
            "savings",
            "2026-03-15",
            "200.00",
            "TRANSFER FROM CHECKING",
        )
        _insert(
            db,
            "csv_brk1",
            "brokerage",
            "2026-03-16",
            "200.00",
            "DEPOSIT",
        )

        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()

        pending = get_pending_matches(db, match_type="transfer")
        # The checking debit should pair with savings (same-day, keywords)
        # not brokerage (next-day, no keywords)
        assert result.pending_transfers >= 1
        best = max(pending, key=lambda p: float(p["confidence_score"]))
        assert best["account_id_b"] == "savings"
