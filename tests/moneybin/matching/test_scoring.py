"""Tests for candidate blocking and scoring."""

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.matching.scoring import (
    CandidatePair,
    compute_confidence,
    get_candidates_cross_source,
    get_candidates_within_source,
)


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Generator[Database, None, None]:
    """Provide a fresh test database for scoring tests."""
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    yield database
    database.close()


def _insert_unioned_row(
    db: Database,
    *,
    source_transaction_id: str,
    account_id: str,
    transaction_date: str,
    amount: str,
    description: str,
    source_type: str,
    source_origin: str,
    source_file: str = "test.csv",
) -> None:
    db.execute(
        """
        INSERT INTO _test_unioned (
            source_transaction_id, account_id, transaction_date, amount,
            description, source_type, source_origin, source_file
        ) VALUES (?, ?, ?::DATE, ?::DECIMAL(18,2), ?, ?, ?, ?)
        """,
        [
            source_transaction_id,
            account_id,
            transaction_date,
            amount,
            description,
            source_type,
            source_origin,
            source_file,
        ],
    )


@pytest.fixture()
def unioned_table(db: Database) -> Database:
    """Create a minimal unioned-style table for testing blocking queries."""
    db.execute("""
        CREATE TABLE _test_unioned (
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
    return db


class TestComputeConfidence:
    """Tests for compute_confidence scoring function."""

    def test_exact_date_high_similarity(self) -> None:
        score = compute_confidence(date_distance_days=0, description_similarity=0.95)
        assert score >= 0.95

    def test_exact_date_low_similarity(self) -> None:
        score = compute_confidence(date_distance_days=0, description_similarity=0.3)
        assert 0.5 < score < 0.95

    def test_far_date_high_similarity(self) -> None:
        score = compute_confidence(date_distance_days=3, description_similarity=0.95)
        assert score < compute_confidence(
            date_distance_days=0, description_similarity=0.95
        )

    def test_score_between_zero_and_one(self) -> None:
        for days in range(4):
            for sim in [0.0, 0.3, 0.5, 0.7, 0.9, 1.0]:
                score = compute_confidence(
                    date_distance_days=days, description_similarity=sim
                )
                assert 0.0 <= score <= 1.0


class TestGetCandidatesCrossSource:
    """Tests for get_candidates_cross_source blocking query."""

    def test_finds_cross_source_pair(self, unioned_table: Database) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS #1234",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS 1234 NEW YORK",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        candidates = get_candidates_cross_source(
            unioned_table, table="main._test_unioned", date_window_days=3
        )
        assert len(candidates) == 1
        assert candidates[0].source_transaction_id_a == "csv_abc"
        assert candidates[0].source_transaction_id_b == "ofx_xyz"

    def test_finds_cross_source_pair_with_equal_source_ids(
        self, unioned_table: Database
    ) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="shared-id",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS #1234",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="shared-id",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS 1234 NEW YORK",
            source_type="ofx",
            source_origin="chase_ofx",
        )

        candidates = get_candidates_cross_source(
            unioned_table, table="main._test_unioned", date_window_days=3
        )

        assert len(candidates) == 1
        assert candidates[0].source_transaction_id_a == "shared-id"
        assert candidates[0].source_transaction_id_b == "shared-id"

    def test_excludes_same_source_type_and_origin(
        self, unioned_table: Database
    ) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="a",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="b",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
        )
        candidates = get_candidates_cross_source(
            unioned_table, table="main._test_unioned", date_window_days=3
        )
        assert len(candidates) == 0

    def test_excludes_different_accounts(self, unioned_table: Database) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="a",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="b",
            account_id="acct2",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="ofx",
            source_origin="chase",
        )
        candidates = get_candidates_cross_source(
            unioned_table, table="main._test_unioned", date_window_days=3
        )
        assert len(candidates) == 0

    def test_excludes_different_amounts(self, unioned_table: Database) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="a",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="b",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-43.00",
            description="STARBUCKS",
            source_type="ofx",
            source_origin="chase",
        )
        candidates = get_candidates_cross_source(
            unioned_table, table="main._test_unioned", date_window_days=3
        )
        assert len(candidates) == 0

    def test_excludes_outside_date_window(self, unioned_table: Database) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="a",
            account_id="acct1",
            transaction_date="2026-03-10",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="b",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        candidates = get_candidates_cross_source(
            unioned_table, table="main._test_unioned", date_window_days=3
        )
        assert len(candidates) == 0

    def test_respects_excluded_ids(self, unioned_table: Database) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        candidates = get_candidates_cross_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=3,
            excluded_ids={("csv_abc", "acct1")},
        )
        assert len(candidates) == 0

    def test_respects_rejected_pairs(self, unioned_table: Database) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        rejected = [
            {
                "source_type_a": "csv",
                "source_transaction_id_a": "csv_abc",
                "source_type_b": "ofx",
                "source_transaction_id_b": "ofx_xyz",
                "account_id": "acct1",
            }
        ]
        candidates = get_candidates_cross_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=3,
            rejected_pairs=rejected,
        )
        assert len(candidates) == 0

    def test_returns_candidate_pair_dataclass(self, unioned_table: Database) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        candidates = get_candidates_cross_source(
            unioned_table, table="main._test_unioned", date_window_days=3
        )
        assert len(candidates) == 1
        pair = candidates[0]
        assert isinstance(pair, CandidatePair)
        assert 0.0 <= pair.confidence_score <= 1.0
        assert pair.date_distance_days == 0
        assert pair.account_id == "acct1"

    def test_cross_source_same_type_different_origin(
        self, unioned_table: Database
    ) -> None:
        """Two csv rows from different origins should be cross-source candidates."""
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_a",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-50.00",
            description="AMAZON",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_b",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-50.00",
            description="AMAZON",
            source_type="csv",
            source_origin="tiller",
        )
        candidates = get_candidates_cross_source(
            unioned_table, table="main._test_unioned", date_window_days=3
        )
        assert len(candidates) == 1


class TestGetCandidatesWithinSource:
    """Tests for get_candidates_within_source blocking query."""

    def test_finds_within_source_duplicate(self, unioned_table: Database) -> None:
        """Same source_type + origin, different source_file — within-source dup."""
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_a",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
            source_file="jan.csv",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_b",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
            source_file="feb.csv",
        )
        candidates = get_candidates_within_source(
            unioned_table, table="main._test_unioned", date_window_days=3
        )
        assert len(candidates) == 1

    def test_excludes_cross_source_rows(self, unioned_table: Database) -> None:
        """Cross-source pairs should not appear in within-source results."""
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        candidates = get_candidates_within_source(
            unioned_table, table="main._test_unioned", date_window_days=3
        )
        assert len(candidates) == 0

    def test_excludes_same_source_file(self, unioned_table: Database) -> None:
        """Rows from the same file should not pair with each other."""
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_a",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
            source_file="same.csv",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_b",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
            source_file="same.csv",
        )
        candidates = get_candidates_within_source(
            unioned_table, table="main._test_unioned", date_window_days=3
        )
        assert len(candidates) == 0

    def test_respects_rejected_pairs(self, unioned_table: Database) -> None:
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_a",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
            source_file="jan.csv",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_b",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
            source_file="feb.csv",
        )
        rejected = [
            {
                "source_type_a": "csv",
                "source_transaction_id_a": "csv_a",
                "source_type_b": "csv",
                "source_transaction_id_b": "csv_b",
                "account_id": "acct1",
            }
        ]
        candidates = get_candidates_within_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=3,
            rejected_pairs=rejected,
        )
        assert len(candidates) == 0
