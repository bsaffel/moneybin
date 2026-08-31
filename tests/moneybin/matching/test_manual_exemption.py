"""Manual-source matcher exemption (transaction-curation spec Req 6).

Manual rows must never be matched as candidates — neither against imported
rows nor against other manual rows — across Tier 2b (within-source), Tier 3
(cross-source), or Tier 4 (transfer detection).
"""

import pytest

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.persistence import get_active_matches, get_pending_matches
from tests.moneybin.matching.conftest import create_matcher_tables as _create_test_table
from tests.moneybin.matching.conftest import insert_matcher_row as _insert

pytestmark = pytest.mark.unit


class TestMatcherManualExemption:
    """Manual rows are blocked from candidate-pair construction in either direction."""

    def test_manual_row_not_matched_against_imported_row_same_date_amount(
        self, db: Database
    ) -> None:
        """Manual + imported with identical (date, amount, desc) — no pair.

        Tier 3 cross-source: must not produce a candidate pair.
        """
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
            "manual_a",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS #1234",
            "manual",
            "user",
        )
        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()
        assert result.auto_merged == 0
        assert result.pending_review == 0
        assert get_active_matches(db, match_type="dedup") == []

    def test_manual_row_not_matched_against_other_manual_row(
        self, db: Database
    ) -> None:
        """Two manual rows with identical (date, amount) must not pair."""
        _create_test_table(db)
        _insert(
            db,
            "manual_a",
            "acct1",
            "2026-03-15",
            "-42.50",
            "GROCERY",
            "manual",
            "user",
        )
        _insert(
            db,
            "manual_b",
            "acct1",
            "2026-03-15",
            "-42.50",
            "GROCERY",
            "manual",
            "user",
            sfile="other.csv",
        )
        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()
        assert result.auto_merged == 0
        assert result.pending_review == 0
        assert get_active_matches(db, match_type="dedup") == []

    def test_manual_row_not_matched_as_transfer(self, db: Database) -> None:
        """Manual debit + imported credit across accounts — no transfer pair.

        Tier 4: cross-account opposite-sign equal-amount must not pair.
        """
        _create_test_table(db)
        _insert(
            db,
            "manual_chk",
            "checking",
            "2026-03-15",
            "-500.00",
            "TRANSFER TO SAV",
            "manual",
            "user",
        )
        _insert(
            db,
            "csv_sav",
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
        assert result.pending_transfers == 0
        assert get_pending_matches(db, match_type="transfer") == []

    def test_imported_pair_still_matches_when_manual_row_present(
        self, db: Database
    ) -> None:
        """Negative-control: imported pair still merges with manual present.

        Two imported rows that should match are not blocked just because a
        manual row exists in the same batch.
        """
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
        _insert(
            db,
            "manual_a",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS #1234",
            "manual",
            "user",
        )
        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        result = matcher.run()
        # csv ↔ ofx still merges; manual is excluded from any pair.
        assert result.auto_merged == 1
        assert result.pending_review == 0
