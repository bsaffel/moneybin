"""Tests for transfer detection scoring and blocking."""

from decimal import Decimal

import pytest

from moneybin.database import Database
from moneybin.matching.transfer import (
    TransferCandidatePair,
    compute_date_score,
    compute_keyword_score,
    compute_transfer_confidence,
    get_candidates_transfers,
)

_DEFAULT_WEIGHTS: dict[str, float] = {
    "date_distance": 0.6,
    "keyword": 0.4,
}


class TestComputeKeywordScore:
    """Tests for transfer keyword detection."""

    def test_no_keywords(self) -> None:
        assert compute_keyword_score("STARBUCKS COFFEE", "GROCERY STORE") == 0.0

    def test_one_keyword(self) -> None:
        assert compute_keyword_score("ONLINE TRANSFER TO SAV", "GROCERY") == 0.5

    def test_two_keywords(self) -> None:
        assert compute_keyword_score("ACH TRANSFER", "PAYMENT") == 0.8

    def test_three_or_more_keywords(self) -> None:
        score = compute_keyword_score("ACH TRANSFER TO SAV", "WIRE FROM CHK")
        assert score == 1.0

    def test_case_insensitive(self) -> None:
        assert compute_keyword_score("transfer from checking", "deposit") == 0.5

    def test_both_descriptions_contribute(self) -> None:
        score = compute_keyword_score("TRANSFER", "ACH DEPOSIT")
        assert score >= 0.8

    def test_no_substring_matches(self) -> None:
        """Keywords must match whole words, not substrings."""
        assert compute_keyword_score("MARCH WIRELESS", "PURCHASE") == 0.0


class TestComputeTransferConfidence:
    """Tests for combined transfer confidence scoring."""

    def test_perfect_signals(self) -> None:
        score = compute_transfer_confidence(
            date_score=compute_date_score(0, 3),
            keyword_score=1.0,
            weights=_DEFAULT_WEIGHTS,
        )
        assert score == pytest.approx(1.0)  # type: ignore[reportUnknownMemberType] — pytest.approx stub incomplete

    def test_zero_signals(self) -> None:
        score = compute_transfer_confidence(
            date_score=compute_date_score(3, 3),
            keyword_score=0.0,
            weights=_DEFAULT_WEIGHTS,
        )
        assert score == pytest.approx(0.0)  # type: ignore[reportUnknownMemberType] — pytest.approx stub incomplete

    def test_date_distance_impact(self) -> None:
        same_day = compute_transfer_confidence(
            date_score=compute_date_score(0, 3),
            keyword_score=0.5,
            weights=_DEFAULT_WEIGHTS,
        )
        one_day = compute_transfer_confidence(
            date_score=compute_date_score(1, 3),
            keyword_score=0.5,
            weights=_DEFAULT_WEIGHTS,
        )
        assert same_day > one_day

    def test_custom_weights(self) -> None:
        weights = {
            "date_distance": 1.0,
            "keyword": 0.0,
        }
        score = compute_transfer_confidence(
            date_score=compute_date_score(0, 3),
            keyword_score=1.0,
            weights=weights,
        )
        assert score == pytest.approx(1.0)  # type: ignore[reportUnknownMemberType] — pytest.approx stub incomplete

    def test_score_between_zero_and_one(self) -> None:
        for days in range(4):
            for kw in [0.0, 0.5, 1.0]:
                score = compute_transfer_confidence(
                    date_score=compute_date_score(days, 3),
                    keyword_score=kw,
                    weights=_DEFAULT_WEIGHTS,
                )
                assert 0.0 <= score <= 1.0


def _insert_transfer_row(
    db: Database,
    *,
    source_transaction_id: str,
    account_id: str,
    transaction_date: str,
    amount: str,
    description: str,
    source_type: str = "csv",
    source_origin: str = "bank",
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
def transfer_table(db: Database) -> Database:
    """Create a minimal unioned-style table for transfer blocking tests."""
    db.execute("""
        CREATE TABLE _test_unioned (
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
    return db


class TestGetCandidatesTransfers:
    """Tests for transfer candidate blocking query."""

    def test_finds_opposite_sign_pair(self, transfer_table: Database) -> None:
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="csv_chk1",
            account_id="checking",
            transaction_date="2026-03-15",
            amount="-500.00",
            description="ONLINE TRANSFER TO SAV",
        )
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="csv_sav1",
            account_id="savings",
            transaction_date="2026-03-15",
            amount="500.00",
            description="TRANSFER FROM CHK",
        )
        candidates = get_candidates_transfers(
            transfer_table,
            table="main._test_unioned",
            date_window_days=3,
            signal_weights=_DEFAULT_WEIGHTS,
        )
        assert len(candidates) == 1
        pair = candidates[0]
        assert isinstance(pair, TransferCandidatePair)
        assert pair.account_id_a == "checking"
        assert pair.account_id_b == "savings"
        assert pair.amount == Decimal("500.00")
        assert pair.date_distance_days == 0

    def test_excludes_same_account(self, transfer_table: Database) -> None:
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="a",
            account_id="checking",
            transaction_date="2026-03-15",
            amount="-500.00",
            description="REFUND",
        )
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="b",
            account_id="checking",
            transaction_date="2026-03-15",
            amount="500.00",
            description="DEPOSIT",
        )
        candidates = get_candidates_transfers(
            transfer_table,
            table="main._test_unioned",
            date_window_days=3,
            signal_weights=_DEFAULT_WEIGHTS,
        )
        assert len(candidates) == 0

    def test_excludes_same_sign(self, transfer_table: Database) -> None:
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="a",
            account_id="checking",
            transaction_date="2026-03-15",
            amount="-500.00",
            description="PAYMENT",
        )
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="b",
            account_id="savings",
            transaction_date="2026-03-15",
            amount="-500.00",
            description="PAYMENT",
        )
        candidates = get_candidates_transfers(
            transfer_table,
            table="main._test_unioned",
            date_window_days=3,
            signal_weights=_DEFAULT_WEIGHTS,
        )
        assert len(candidates) == 0

    def test_excludes_different_amount(self, transfer_table: Database) -> None:
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="a",
            account_id="checking",
            transaction_date="2026-03-15",
            amount="-500.00",
            description="TRANSFER",
        )
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="b",
            account_id="savings",
            transaction_date="2026-03-15",
            amount="501.00",
            description="TRANSFER",
        )
        candidates = get_candidates_transfers(
            transfer_table,
            table="main._test_unioned",
            date_window_days=3,
            signal_weights=_DEFAULT_WEIGHTS,
        )
        assert len(candidates) == 0

    def test_excludes_outside_date_window(self, transfer_table: Database) -> None:
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="a",
            account_id="checking",
            transaction_date="2026-03-10",
            amount="-500.00",
            description="TRANSFER",
        )
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="b",
            account_id="savings",
            transaction_date="2026-03-15",
            amount="500.00",
            description="TRANSFER",
        )
        candidates = get_candidates_transfers(
            transfer_table,
            table="main._test_unioned",
            date_window_days=3,
            signal_weights=_DEFAULT_WEIGHTS,
        )
        assert len(candidates) == 0

    def test_excludes_currency_mismatch(self, transfer_table: Database) -> None:
        """Cross-currency pairs must not be proposed as transfers."""
        transfer_table.execute(
            """
            INSERT INTO _test_unioned (
                source_transaction_id, account_id, transaction_date, amount,
                description, source_type, source_origin, source_file, currency_code
            ) VALUES
                ('a', 'checking', '2026-03-15'::DATE, -100.00, 'TRANSFER', 'csv', 'bank', 'f.csv', 'EUR'),
                ('b', 'savings',  '2026-03-15'::DATE,  100.00, 'TRANSFER', 'csv', 'bank', 'f.csv', 'USD')
            """
        )
        candidates = get_candidates_transfers(
            transfer_table,
            table="main._test_unioned",
            date_window_days=3,
            signal_weights=_DEFAULT_WEIGHTS,
        )
        assert len(candidates) == 0

    def test_includes_unknown_currency_pair(self, transfer_table: Database) -> None:
        """Two transactions with NULL currency_code must still be found.

        Regression test: `currency_code IS NULL` on both sides must not be
        excluded by the blocking predicate. SQL's `NULL = NULL` evaluates to
        NULL (not TRUE), so a naive `a.currency_code = b.currency_code`
        silently drops every pair where currency wasn't captured upstream —
        which, post multi-currency Task 4, is the common case (uncaptured
        currency is now honestly NULL instead of defaulted to 'USD').
        """
        transfer_table.execute(
            """
            INSERT INTO _test_unioned (
                source_transaction_id, account_id, transaction_date, amount,
                description, source_type, source_origin, source_file, currency_code
            ) VALUES
                ('a', 'checking', '2026-03-15'::DATE, -100.00, 'TRANSFER', 'csv', 'bank', 'f.csv', NULL),
                ('b', 'savings',  '2026-03-15'::DATE,  100.00, 'TRANSFER', 'csv', 'bank', 'f.csv', NULL)
            """
        )
        candidates = get_candidates_transfers(
            transfer_table,
            table="main._test_unioned",
            date_window_days=3,
            signal_weights=_DEFAULT_WEIGHTS,
        )
        assert len(candidates) == 1

    def test_includes_one_side_unknown_currency_pair(
        self, transfer_table: Database
    ) -> None:
        """One side NULL, other side a known currency must still be found.

        This is a deliberate, narrower gap than full strictness would allow:
        a known-vs-unknown pair is not blocked, only known-vs-different-known
        is (see test_excludes_currency_mismatch). Tightening this further is
        out of scope for this fix.
        """
        transfer_table.execute(
            """
            INSERT INTO _test_unioned (
                source_transaction_id, account_id, transaction_date, amount,
                description, source_type, source_origin, source_file, currency_code
            ) VALUES
                ('a', 'checking', '2026-03-15'::DATE, -100.00, 'TRANSFER', 'csv', 'bank', 'f.csv', NULL),
                ('b', 'savings',  '2026-03-15'::DATE,  100.00, 'TRANSFER', 'csv', 'bank', 'f.csv', 'USD')
            """
        )
        candidates = get_candidates_transfers(
            transfer_table,
            table="main._test_unioned",
            date_window_days=3,
            signal_weights=_DEFAULT_WEIGHTS,
        )
        assert len(candidates) == 1

    def test_respects_excluded_ids(self, transfer_table: Database) -> None:
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="csv_chk1",
            account_id="checking",
            transaction_date="2026-03-15",
            amount="-500.00",
            description="TRANSFER",
        )
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="csv_sav1",
            account_id="savings",
            transaction_date="2026-03-15",
            amount="500.00",
            description="TRANSFER",
        )
        candidates = get_candidates_transfers(
            transfer_table,
            table="main._test_unioned",
            date_window_days=3,
            signal_weights=_DEFAULT_WEIGHTS,
            excluded_ids={("csv_chk1", "csv", "checking")},
        )
        assert len(candidates) == 0

    def test_respects_rejected_pairs(self, transfer_table: Database) -> None:
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="csv_chk1",
            account_id="checking",
            transaction_date="2026-03-15",
            amount="-500.00",
            description="TRANSFER",
        )
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="csv_sav1",
            account_id="savings",
            transaction_date="2026-03-15",
            amount="500.00",
            description="TRANSFER",
        )
        rejected = [
            {
                "source_type_a": "csv",
                "source_transaction_id_a": "csv_chk1",
                "source_origin_a": "bank",
                "source_type_b": "csv",
                "source_transaction_id_b": "csv_sav1",
                "source_origin_b": "bank",
                "account_id": "checking",
                "account_id_b": "savings",
            }
        ]
        candidates = get_candidates_transfers(
            transfer_table,
            table="main._test_unioned",
            date_window_days=3,
            signal_weights=_DEFAULT_WEIGHTS,
            rejected_pairs=rejected,
        )
        assert len(candidates) == 0

    def test_scores_both_signals(self, transfer_table: Database) -> None:
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="csv_chk1",
            account_id="checking",
            transaction_date="2026-03-15",
            amount="-500.00",
            description="ONLINE TRANSFER TO SAV",
        )
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="csv_sav1",
            account_id="savings",
            transaction_date="2026-03-15",
            amount="500.00",
            description="TRANSFER FROM CHK",
        )
        candidates = get_candidates_transfers(
            transfer_table,
            table="main._test_unioned",
            date_window_days=3,
            signal_weights=_DEFAULT_WEIGHTS,
        )
        assert len(candidates) == 1
        pair = candidates[0]
        assert pair.date_distance_score == 1.0
        assert pair.keyword_score > 0.0
        assert 0.0 < pair.confidence_score <= 1.0

    def test_debit_side_is_a_credit_side_is_b(self, transfer_table: Database) -> None:
        """Verify the debit (negative) transaction is always side A."""
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="csv_sav1",
            account_id="savings",
            transaction_date="2026-03-15",
            amount="500.00",
            description="TRANSFER FROM CHK",
        )
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="csv_chk1",
            account_id="checking",
            transaction_date="2026-03-15",
            amount="-500.00",
            description="ONLINE TRANSFER TO SAV",
        )
        candidates = get_candidates_transfers(
            transfer_table,
            table="main._test_unioned",
            date_window_days=3,
            signal_weights=_DEFAULT_WEIGHTS,
        )
        assert len(candidates) == 1
        pair = candidates[0]
        assert pair.source_transaction_id_a == "csv_chk1"
        assert pair.source_transaction_id_b == "csv_sav1"

    def test_near_boundary_date(self, transfer_table: Database) -> None:
        """Pair exactly at date_window_days boundary is included."""
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="a",
            account_id="checking",
            transaction_date="2026-03-12",
            amount="-500.00",
            description="TRANSFER",
        )
        _insert_transfer_row(
            transfer_table,
            source_transaction_id="b",
            account_id="savings",
            transaction_date="2026-03-15",
            amount="500.00",
            description="TRANSFER",
        )
        candidates = get_candidates_transfers(
            transfer_table,
            table="main._test_unioned",
            date_window_days=3,
            signal_weights=_DEFAULT_WEIGHTS,
        )
        assert len(candidates) == 1
        assert candidates[0].date_distance_days == 3
