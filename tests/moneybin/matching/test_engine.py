"""Tests for TransactionMatcher orchestrator."""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.engine import MatchResult, TransactionMatcher
from moneybin.matching.scoring import CandidatePair


def _make_pair(confidence: float) -> CandidatePair:
    return CandidatePair(
        source_transaction_id_a="a",
        source_type_a="csv",
        source_origin_a="chase",
        source_transaction_id_b="b",
        source_type_b="ofx",
        source_origin_b="chase",
        account_id="acct1",
        date_distance_days=0,
        description_similarity=confidence,
        confidence_score=confidence,
        description_a="",
        description_b="",
    )


def _matcher_with_settings(**kwargs: object) -> TransactionMatcher:
    settings = MatchingSettings(**kwargs)  # type: ignore[arg-type]
    return TransactionMatcher(MagicMock(), settings)


class TestClassifyPair:
    """Unit tests for _classify_pair — no DB required."""

    def test_high_confidence_returns_accepted_for_2b(self) -> None:
        matcher = _matcher_with_settings(
            high_confidence_threshold=0.90, review_threshold=0.70
        )
        assert matcher._classify_pair(_make_pair(0.95), "2b") == ("accepted", "auto")

    def test_high_confidence_returns_accepted_for_3(self) -> None:
        matcher = _matcher_with_settings(
            high_confidence_threshold=0.90, review_threshold=0.70
        )
        assert matcher._classify_pair(_make_pair(0.95), "3") == ("accepted", "auto")

    def test_tier3_above_review_threshold_returns_pending(self) -> None:
        matcher = _matcher_with_settings(
            high_confidence_threshold=0.90, review_threshold=0.70
        )
        assert matcher._classify_pair(_make_pair(0.80), "3") == ("pending", "auto")

    def test_tier2b_above_review_threshold_returns_none(self) -> None:
        # Same confidence range as pending case, but tier 2b has no review bucket.
        matcher = _matcher_with_settings(
            high_confidence_threshold=0.90, review_threshold=0.70
        )
        assert matcher._classify_pair(_make_pair(0.80), "2b") is None

    def test_below_all_thresholds_returns_none(self) -> None:
        matcher = _matcher_with_settings(
            high_confidence_threshold=0.90, review_threshold=0.70
        )
        for tier in ("2b", "3"):
            result = matcher._classify_pair(_make_pair(0.50), tier)  # type: ignore[arg-type]
            assert result is None, f"Expected None for tier {tier!r}"


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


class TestFetchActiveDedupDecisions:
    """Equivalence check: _fetch_active_dedup_decisions covers pre-seeded and newly-created matches."""

    def test_returns_pre_seeded_and_new_matches(self, db: Database) -> None:
        """Verify _fetch_active_dedup_decisions covers pre-seeded and newly-created matches.

        Pre-seeds two dedup decisions, runs the matcher on a fresh pair, then
        asserts both matched_ids and secondary_ids include all three pairs.
        """
        _create_test_table(db)

        # Pre-seed two accepted dedup decisions before any run.
        from datetime import UTC, datetime

        now = datetime.now(tz=UTC).isoformat()
        db.execute(
            """
            INSERT INTO app.match_decisions
            (match_id, source_transaction_id_a, source_type_a, source_origin_a,
             source_transaction_id_b, source_type_b, source_origin_b,
             account_id, confidence_score, match_signals, match_type, match_tier,
             match_status, decided_by, decided_at)
            VALUES
            ('seed000001', 'csv_pre1', 'csv', 'bank',
             'ofx_pre1', 'ofx', 'bank',
             'acct1', 0.99, '{}', 'dedup', '3', 'accepted', 'auto', ?),
            ('seed000002', 'csv_pre2', 'csv', 'bank',
             'ofx_pre2', 'ofx', 'bank',
             'acct1', 0.99, '{}', 'dedup', '3', 'accepted', 'auto', ?)
            """,
            [now, now],
        )  # noqa: S608  # test fixture data, not user input

        # Insert a fresh pair that the matcher will create a new decision for.
        _insert(
            db, "csv_new", "acct1", "2026-03-15", "-42.50", "STARBUCKS", "csv", "chase"
        )
        _insert(
            db,
            "ofx_new",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS",
            "ofx",
            "chase_ofx",
        )

        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()
        assert result.auto_merged >= 1  # the fresh pair was matched

        # Now call _fetch_active_dedup_decisions directly and verify both sets.
        decisions = matcher._fetch_active_dedup_decisions()

        # matched_ids must include both sides of all three pairs.
        assert ("csv_pre1", "acct1") in decisions.matched_ids
        assert ("ofx_pre1", "acct1") in decisions.matched_ids
        assert ("csv_pre2", "acct1") in decisions.matched_ids
        assert ("ofx_pre2", "acct1") in decisions.matched_ids
        assert ("csv_new", "acct1") in decisions.matched_ids
        assert ("ofx_new", "acct1") in decisions.matched_ids

        # secondary_ids: ofx is lower-priority than csv per default source_priority,
        # so the ofx side of each pair is the secondary (excluded from transfers).
        assert ("ofx_pre1", "ofx", "acct1") in decisions.secondary_ids
        assert ("ofx_pre2", "ofx", "acct1") in decisions.secondary_ids
        assert ("ofx_new", "ofx", "acct1") in decisions.secondary_ids


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
            source_file VARCHAR,
            currency_code VARCHAR DEFAULT 'USD'
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
        INSERT INTO _test_unioned (
            source_transaction_id, account_id, transaction_date, amount,
            description, source_type, source_origin, source_file
        ) VALUES (?, ?, ?::DATE, ?::DECIMAL(18,2), ?, ?, ?, ?)
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

        matches = get_active_matches(db, match_type="dedup")
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


class TestTransferDetection:
    """Tests for Tier 4 transfer detection."""

    def test_transfer_pair_goes_to_review(self, db: Database) -> None:
        _create_test_table(db)
        _insert(
            db,
            "csv_chk1",
            "checking",
            "2026-03-15",
            "-500.00",
            "ONLINE TRANSFER TO SAV",
            "csv",
            "chase",
        )
        _insert(
            db,
            "csv_sav1",
            "savings",
            "2026-03-15",
            "500.00",
            "TRANSFER FROM CHK",
            "csv",
            "chase",
        )
        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()
        assert result.pending_transfers >= 1
        assert result.auto_merged == 0

    def test_no_auto_merge_for_transfers(self, db: Database) -> None:
        """Transfers are always-review in v1, even with perfect scores."""
        _create_test_table(db)
        _insert(
            db,
            "csv_chk1",
            "checking",
            "2026-03-15",
            "-500.00",
            "TRANSFER TO SAV",
            "csv",
            "chase",
        )
        _insert(
            db,
            "csv_sav1",
            "savings",
            "2026-03-15",
            "500.00",
            "TRANSFER FROM CHK",
            "csv",
            "chase",
        )
        settings = MatchingSettings(transfer_review_threshold=0.0)
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()
        assert result.auto_merged == 0
        assert result.pending_transfers >= 1

    def test_dedup_then_transfer_sequencing(self, db: Database) -> None:
        """Dedup runs first; deduped transactions then match as transfers."""
        _create_test_table(db)
        _insert(
            db,
            "csv_chk1",
            "checking",
            "2026-03-15",
            "-500.00",
            "ONLINE TRANSFER TO SAV",
            "csv",
            "chase",
        )
        _insert(
            db,
            "ofx_chk1",
            "checking",
            "2026-03-15",
            "-500.00",
            "ONLINE TRANSFER TO SAV",
            "ofx",
            "chase_ofx",
        )
        _insert(
            db,
            "csv_sav1",
            "savings",
            "2026-03-15",
            "500.00",
            "TRANSFER FROM CHK",
            "csv",
            "chase",
        )
        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()
        assert result.auto_merged == 1
        assert result.pending_transfers >= 1

    def test_rejected_transfer_not_reproposed(self, db: Database) -> None:
        _create_test_table(db)
        _insert(
            db,
            "csv_chk1",
            "checking",
            "2026-03-15",
            "-500.00",
            "TRANSFER",
            "csv",
            "chase",
        )
        _insert(
            db,
            "csv_sav1",
            "savings",
            "2026-03-15",
            "500.00",
            "TRANSFER",
            "csv",
            "chase",
        )
        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")

        result1 = matcher.run()
        assert result1.pending_transfers >= 1

        from moneybin.matching.persistence import (
            get_pending_matches,
            update_match_status,
        )

        pending = get_pending_matches(db, match_type="transfer")
        for m in pending:
            update_match_status(db, m["match_id"], status="rejected", decided_by="user")

        result2 = matcher.run()
        assert result2.pending_transfers == 0

    def test_match_result_includes_transfers(self) -> None:
        result = MatchResult(auto_merged=3, pending_review=1, pending_transfers=2)
        summary = result.summary()
        assert "3 auto-merged" in summary
        assert "1 pending review" in summary
        assert "2 potential transfers" in summary

    def test_one_sided_transfer_no_match(self, db: Database) -> None:
        """Only one side imported — no transfer pair proposed."""
        _create_test_table(db)
        _insert(
            db,
            "csv_chk1",
            "checking",
            "2026-03-15",
            "-500.00",
            "TRANSFER",
            "csv",
            "chase",
        )
        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()
        assert result.pending_transfers == 0
