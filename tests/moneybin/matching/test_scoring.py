"""Tests for candidate blocking and scoring."""

import pytest

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.scoring import (
    CandidatePair,
    compute_confidence,
    get_candidates_cross_source,
    get_candidates_within_source,
)


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

    def test_identical_descriptions_inside_window_are_always_review_eligible(
        self,
    ) -> None:
        """The blocking window must never admit a pair the scoring cannot surface.

        Blocking already requires the same account, an exact amount match, and a
        date inside ``date_window_days``. If two such rows also carry *identical*
        descriptions, there is no signal left that could justify hiding them —
        yet the date term decays to 0 at the window edge, so confidence there is
        ``_WEIGHT_DESCRIPTION`` alone. Whenever that weight sits below the review
        threshold the edge of the window is a dead zone: admitted as a candidate,
        mathematically unable to be reviewed.

        Found on real data — 77 of 345 genuine Chase duplicate pairs scored below
        the review threshold, every one of them at the window edge.

        Thresholds come from the live defaults, not literals, so retuning the
        bands cannot silently reopen the dead zone.
        """
        bands = MatchingSettings()
        for window in (1, 3, 5, 7):
            for days in range(window + 1):
                score = compute_confidence(
                    date_distance_days=days,
                    description_similarity=1.0,
                    date_window_days=window,
                )
                assert score >= bands.review_threshold, (
                    f"identical descriptions at {days}d inside a {window}d window "
                    f"scored {score:.3f}, below review_threshold "
                    f"{bands.review_threshold}"
                )

    def test_only_same_day_pairs_can_auto_merge(self) -> None:
        """Any date gap means review, never a silent merge.

        A wrong silent merge is the hardest inference to notice and undo
        (design-principles.md), so the auto-merge band is reserved for the one
        case that is near-certain on its own: same account, exact amount, same
        day. A pair separated by even one day must land in review no matter how
        perfectly its descriptions agree.
        """
        bands = MatchingSettings()
        # Up to the shipped default window. The margin narrows as the window
        # widens, so this holds for supported configurations rather than for
        # every conceivable one — see test_shipped_defaults_keep_every_date_gap
        # _in_review, which pins the same property against the live defaults.
        for window in (1, 2, 3, 4, 5):
            for days in range(1, window + 1):
                score = compute_confidence(
                    date_distance_days=days,
                    description_similarity=1.0,
                    date_window_days=window,
                )
                assert score < bands.high_confidence_threshold, (
                    f"a {days}d-apart pair scored {score:.3f}, at or above the "
                    f"{bands.high_confidence_threshold} auto-merge threshold"
                )

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

    def test_exact_key_floor_lifts_low_similarity_above_threshold(self) -> None:
        """An exact-key (date_distance=0) pair auto-merges regardless of low desc.

        Without the floor, date_distance=0 + desc 0.2 scores 0.30 + 0.70*0.2 = 0.44,
        well below the 0.95 auto-merge threshold (the OFX-vs-CSV truncation bug).
        With exact_key_floor=0.95 the score is lifted to >= 0.95 so it auto-merges.
        """
        score = compute_confidence(
            date_distance_days=0, description_similarity=0.2, exact_key_floor=0.95
        )
        assert score >= 0.95

    def test_exact_key_floor_preserves_description_ordering(self) -> None:
        """Description stays a *tiebreaker*: higher similarity ranks higher, both >= floor."""
        high = compute_confidence(
            date_distance_days=0, description_similarity=0.9, exact_key_floor=0.95
        )
        low = compute_confidence(
            date_distance_days=0, description_similarity=0.1, exact_key_floor=0.95
        )
        assert high > low
        assert low >= 0.95
        assert high <= 1.0

    def test_exact_key_floor_ignored_when_date_distance_nonzero(self) -> None:
        """The floor applies only to exact-key (date_distance=0) pairs.

        A date_distance>0 pair keeps the weighted formula even when a floor is
        supplied, so description still gates acceptance when dates differ.
        """
        with_floor = compute_confidence(
            date_distance_days=1, description_similarity=0.2, exact_key_floor=0.95
        )
        weighted = compute_confidence(date_distance_days=1, description_similarity=0.2)
        assert with_floor == weighted
        assert with_floor < 0.95

    def test_no_floor_uses_weighted_formula(self) -> None:
        """Default (no floor) is unchanged: exact-key + low desc stays sub-threshold."""
        score = compute_confidence(date_distance_days=0, description_similarity=0.2)
        assert score < 0.95


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
        assert candidates[0].source_type_a == "csv"
        assert candidates[0].source_type_b == "ofx"
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

    def test_exact_key_low_similarity_pair_scores_above_threshold(
        self, unioned_table: Database
    ) -> None:
        """Cross-source exact-key twins auto-merge despite divergent descriptions.

        OFX truncates descriptions differently from CSV, so jaro_winkler is low,
        but same account + exact amount + same day is a near-certain duplicate.
        With high_confidence_threshold passed, the candidate scores >= 0.95.
        """
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS STORE 1234 NEW YORK NY",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCK",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        candidates = get_candidates_cross_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=3,
            high_confidence_threshold=0.95,
        )
        assert len(candidates) == 1
        assert candidates[0].description_similarity < 0.95  # divergent descriptions
        assert candidates[0].confidence_score >= 0.95  # but exact-key lifts it

    def test_candidate_carries_source_file(self, unioned_table: Database) -> None:
        """Candidates expose source_file on both sides for the cardinality guard."""
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS",
            source_type="csv",
            source_origin="chase",
            source_file="march.csv",
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
            source_file="march.ofx",
        )
        candidates = get_candidates_cross_source(
            unioned_table, table="main._test_unioned", date_window_days=3
        )
        assert len(candidates) == 1
        assert candidates[0].source_file_a == "march.csv"
        assert candidates[0].source_file_b == "march.ofx"

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
