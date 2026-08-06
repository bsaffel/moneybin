"""Tests for candidate blocking and scoring."""

import pytest

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.scoring import (
    _BOILERPLATE_TOKENS,  # pyright: ignore[reportPrivateUsage]  # pinned by set equality below
    CandidatePair,
    compute_confidence,
    get_candidates_cross_source,
    get_candidates_within_source,
)


def test_boilerplate_vocabulary_is_pinned_by_set_equality() -> None:
    """Every token here removes evidence from the auto-merge gate — pin the set.

    A membership check would pass while the set silently grew, and each addition
    widens what merges without review: a word listed here can no longer be the
    sole thing two sources agree on. Equality forces a deliberate edit here when
    the vocabulary changes, and the change lands in one reviewable diff.
    """
    assert _BOILERPLATE_TOKENS == {
        "ACH",
        "ATM",
        "AUTHORIZED",
        "CARD",
        "CHECK",
        "CREDIT",
        "DEBIT",
        "DEPOSIT",
        "EFT",
        "ELECTRONIC",
        "FEE",
        "PAYMENT",
        "POS",
        "PURCHASE",
        "RECURRING",
        "TRANSACTION",
        "TRANSFER",
        "WITHDRAWAL",
    }
    assert all(token.isupper() and token.isalpha() for token in _BOILERPLATE_TOKENS), (
        "tokens are compared against an upper-cased, whitespace-collapsed "
        "description and inlined into SQL as literals"
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

    def test_the_weighted_path_peaks_at_zero_gap_and_clears_the_threshold(
        self,
    ) -> None:
        """The formula is not the auto-merge guard, and must not be mistaken for one.

        ``date_score`` is ``1 - days/window``, so it is largest at *zero* days
        apart, not one. At that point the weighted path scores
        ``_WEIGHT_DATE + _WEIGHT_DESCRIPTION * similarity``, which clears the
        auto-merge threshold for any similarity above roughly 0.93 — with no
        description agreement involved at all.

        This test pins that hazard rather than denying it. An earlier version
        asserted the opposite, and held only because it started its loop at one
        day apart and never evaluated the peak; two genuinely distinct
        transactions differing by a trailing digit would have merged silently.
        The gate that actually holds the line is
        ``TransactionMatcher._classify_pair``, which requires
        ``descriptions_agree``. Anyone tempted to move it back into the formula
        should fail here first.
        """
        bands = MatchingSettings()
        for window in (1, 2, 3, 4, 5):
            scores = [
                compute_confidence(
                    date_distance_days=days,
                    description_similarity=1.0,
                    date_window_days=window,
                )
                for days in range(0, window + 1)
            ]
            assert scores == sorted(scores, reverse=True), (
                f"scores over a {window}d window are not monotonically "
                f"decreasing in date distance: {scores}"
            )
            assert scores[0] >= bands.high_confidence_threshold, (
                f"the weighted path's peak over a {window}d window scored "
                f"{scores[0]:.3f}, below the {bands.high_confidence_threshold} "
                "auto-merge threshold — if this is now true, the classification "
                "gate is no longer the only thing preventing a silent merge and "
                "this test should be rewritten deliberately, not deleted"
            )

    def test_the_within_source_boundary_narrowed_with_the_reweight(self) -> None:
        """Tier 2b's silent-merge boundary moved, and the narrowing is deliberate.

        Tier 2b always takes the weighted path — no agreement floor, and no review
        queue behind it, so a pair below ``high_confidence_threshold`` is dropped
        with no trace at all. The 0.40/0.60 → 0.30/0.70 reweight therefore moved a
        real acceptance boundary: a same-day pair at 0.92 similarity scored
        ``0.40 + 0.60 * 0.92 = 0.952`` and merged silently, and now scores
        ``0.30 + 0.70 * 0.92 = 0.944`` and is dropped.

        The narrowing is accepted rather than incidental. It errs toward leaving
        two rows a human can still see instead of deleting one nobody will, and
        restoring the old boundary would need ``_WEIGHT_DESCRIPTION`` back at
        0.60 — below ``review_threshold``, which reopens the window-edge dead zone
        the weights are chosen to close. Pinned as a literal so a future re-tune
        has to make that trade again in the open rather than drift back into it.
        """
        bands = MatchingSettings()
        same_day = compute_confidence(
            date_distance_days=0, description_similarity=0.92, date_window_days=3
        )
        assert same_day == pytest.approx(0.944)  # type: ignore[reportUnknownMemberType]  # pytest.approx stub incomplete
        assert same_day < bands.high_confidence_threshold

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

    def test_agreeing_descriptions_auto_merge_at_every_date_gap(self) -> None:
        """Description agreement, not the date gap, is what earns a silent merge.

        Across sources each side lists the same transaction once, so two rows
        whose descriptions agree are one transaction however differently the
        two sources dated it. Posting lag is not evidence of distinctness, so
        every gap the window admits must clear the auto-merge bar.
        """
        settings = MatchingSettings()
        floor = settings.high_confidence_threshold
        for gap in range(settings.date_window_days + 1):
            score = compute_confidence(
                date_distance_days=gap,
                description_similarity=0.85,
                date_window_days=settings.date_window_days,
                agreement_floor=floor,
                descriptions_agree=True,
            )
            assert score >= floor, f"a {gap}-day gap fell below the auto-merge bar"

    def test_disagreeing_descriptions_are_reviewed_even_on_the_same_day(self) -> None:
        """Landing on the same day is not enough to merge two rows silently.

        Same account and exact amount are already required by blocking, so on a
        same-day pair the description is the only remaining evidence — and when
        it disagrees, that is precisely the case a human should see. A wrong
        silent merge is the hardest inference to notice and undo, so this lands
        in review rather than either merging or being dropped.
        """
        settings = MatchingSettings()
        score = compute_confidence(
            date_distance_days=0,
            description_similarity=0.71,
            date_window_days=settings.date_window_days,
            agreement_floor=settings.high_confidence_threshold,
            descriptions_agree=False,
        )
        assert score < settings.high_confidence_threshold
        assert score >= settings.review_threshold

    def test_agreement_floor_preserves_description_ordering(self) -> None:
        """Description stays a *tiebreaker*: higher similarity ranks higher, both >= floor."""
        high = compute_confidence(
            date_distance_days=0,
            description_similarity=0.9,
            agreement_floor=0.95,
            descriptions_agree=True,
        )
        low = compute_confidence(
            date_distance_days=0,
            description_similarity=0.1,
            agreement_floor=0.95,
            descriptions_agree=True,
        )
        assert high > low
        assert low >= 0.95
        assert high <= 1.0

    def test_agreement_floor_ignored_when_descriptions_disagree(self) -> None:
        """A supplied floor does nothing unless the descriptions actually agree."""
        with_floor = compute_confidence(
            date_distance_days=0,
            description_similarity=0.2,
            agreement_floor=0.95,
            descriptions_agree=False,
        )
        weighted = compute_confidence(date_distance_days=0, description_similarity=0.2)
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

    def test_truncated_description_auto_merges_despite_a_date_gap(
        self, unioned_table: Database
    ) -> None:
        """One source truncating the other's description is agreement, not conflict.

        OFX truncates where CSV spells the merchant out, so jaro_winkler alone is
        low. The dates are deliberately two days apart so the date cannot be the
        reason this merges — the truncation relationship is doing the work.
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
            transaction_date="2026-03-17",
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
        assert candidates[0].date_distance_days == 2  # the date is not the evidence
        assert candidates[0].description_similarity < 0.95  # neither is raw similarity
        assert candidates[0].confidence_score >= 0.95

    def test_shared_merchant_inside_differing_wrappers_auto_merges(
        self, unioned_table: Database
    ) -> None:
        """The shared part is not always at the front.

        One source prefixes a transaction-type preamble and appends a card
        reference while the other carries the bare merchant, so the common text
        sits in the middle of the longer string. A prefix-only rule misses this
        entirely, yet the two are plainly the same purchase.
        """
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-29.00",
            description="RECURRING PAYMENT AUTHORIZED ON 01/25 TASKAPP TASKAPP.COM DE",
            source_type="csv",
            source_origin="wells",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-29.00",
            description="TASKAPP",
            source_type="ofx",
            source_origin="wells_ofx",
        )
        candidates = get_candidates_cross_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=3,
            high_confidence_threshold=0.95,
        )
        assert len(candidates) == 1
        assert candidates[0].confidence_score >= 0.95

    def test_internal_whitespace_runs_do_not_block_agreement(
        self, unioned_table: Database
    ) -> None:
        """Column-padded spacing is a rendering artifact, not a difference.

        Sources pad descriptions to fixed widths, so the same string arrives with
        different runs of spaces. Comparing raw text would treat two renderings
        of one merchant as unrelated.
        """
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-22.97",
            description="UBER   *TRIP",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="pdf_abc",
            account_id="acct1",
            transaction_date="2026-03-16",
            amount="-22.97",
            description="UBER *TRIP HELP.UBER.COM CA",
            source_type="pdf",
            source_origin="chase_pdf",
        )
        candidates = get_candidates_cross_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=3,
            high_confidence_threshold=0.95,
        )
        assert len(candidates) == 1
        assert candidates[0].confidence_score >= 0.95

    def test_unrelated_same_day_descriptions_do_not_auto_merge(
        self, unioned_table: Database
    ) -> None:
        """Two different merchants that collide on amount and date must not merge.

        Same account, exact amount, same day — everything blocking checks agrees,
        and only the description dissents. Merging here would silently destroy a
        real transaction, so the pair has to stay below the auto-merge bar and be
        offered for review instead.
        """
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="AMAZON MKTPL*NQ9RG3UP2",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="LAGARDE FLOWERS",
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
        assert candidates[0].confidence_score < 0.95

    def test_blank_description_is_not_agreement(self, unioned_table: Database) -> None:
        """An empty description is a prefix of every string — and evidence of nothing.

        Left unguarded this is the worst case in the design: a source that omits
        descriptions would satisfy the prefix relation against every row it met
        and merge the entire account silently.
        """
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="   ",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS STORE 1234",
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
        assert candidates[0].confidence_score < 0.95

    def test_boilerplate_only_description_is_not_agreement(
        self, unioned_table: Database
    ) -> None:
        """A description made only of transaction-type words identifies no merchant.

        Containment asks whether one string sits inside another, never whether the
        shared text is worth anything. `DEBIT` sits inside most card descriptions,
        so a source rendering a row as bare boilerplate would agree with an
        unrelated transaction that merely collided on amount inside the window —
        and merge it silently, which is the costliest direction to be wrong.

        Isolation: both sides are non-empty (so the blank guard cannot claim this)
        and containment genuinely holds (so the unrelated-descriptions case cannot
        either). Only the boilerplate test can keep this pair out of auto-merge.
        """
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-47.50",
            description="DEBIT",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-18",
            amount="-47.50",
            description="DEBIT CARD PURCHASE AMAZON MKTPL*NQ9RG3UP2",
            source_type="csv",
            source_origin="chase",
        )
        candidates = get_candidates_cross_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=5,
            high_confidence_threshold=0.95,
        )
        assert len(candidates) == 1
        assert candidates[0].confidence_score < 0.95

    def test_identical_descriptions_agree_even_when_boilerplate(
        self, unioned_table: Database
    ) -> None:
        """Two sources writing the same word is agreement, boilerplate or not.

        The merchant-token requirement exists because containment on a *fragment*
        is weak evidence: a bare `DEBIT` sits inside `DEBIT CARD PURCHASE AMAZON`
        and identifies nothing. Exact equality is not a fragment match — there is
        no longer string for the short one to hide inside. A `DEPOSIT` of the same
        amount, on the same day, in the same account, described identically by two
        sources is one transaction; requiring a merchant word there asks a bank to
        name a merchant that does not exist.

        Isolation: both sides are non-empty and the shared text is pure
        boilerplate, so the merchant-token rule would reject this pair. Only the
        equality carve-out can admit it.
        """
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-01-28",
            amount="5000.00",
            description="Deposit",
            source_type="ofx",
            source_origin="wf_ofx",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-01-28",
            amount="5000.00",
            description="DEPOSIT",
            source_type="csv",
            source_origin="wf",
        )
        candidates = get_candidates_cross_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=5,
            high_confidence_threshold=0.95,
        )
        assert len(candidates) == 1
        assert candidates[0].descriptions_agree is True

    def test_boilerplate_beside_a_merchant_token_still_agrees(
        self, unioned_table: Database
    ) -> None:
        """One merchant word is enough — the guard rejects only pure boilerplate.

        Sources prepend their own transaction-type words to a real merchant string,
        so requiring the contained side to be free of boilerplate would reject the
        agreements this gate exists to find. It must reject only the side that
        carries nothing else.
        """
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-47.50",
            description="POS AMAZON",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-18",
            amount="-47.50",
            description="POS AMAZON MKTPL*NQ9RG3UP2 SEATTLE WA",
            source_type="csv",
            source_origin="chase",
        )
        candidates = get_candidates_cross_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=5,
            high_confidence_threshold=0.95,
        )
        assert len(candidates) == 1
        assert candidates[0].confidence_score >= 0.95

    def test_a_digit_only_token_is_not_merchant_evidence(
        self, unioned_table: Database
    ) -> None:
        """A reference number names no merchant, so it cannot earn a silent merge.

        The merchant-token guard asks whether the contained side carries anything
        beyond transaction-type boilerplate. A card or reference number clears
        that bar without meaning anything: `POS 1234` is boilerplate plus a
        number, and it sits inside every longer description from the other source
        that happens to print the same card digits. Two distinct charges on one
        card, equal in amount and inside the window, would then auto-merge and one
        would disappear with no review entry.

        Isolation: both sides are non-empty (the blank guard cannot claim this),
        containment genuinely holds (the unrelated-description case cannot),
        `1234` is not in `_BOILERPLATE_TOKENS` (the boilerplate guard cannot), and
        the two strings differ (the equality carve-out cannot). Only the
        letter-bearing requirement can keep this pair out of auto-merge.
        """
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-47.50",
            description="POS 1234",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-47.50",
            description="POS 1234 STARBUCKS SEATTLE WA",
            source_type="csv",
            source_origin="chase",
        )
        candidates = get_candidates_cross_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=3,
            high_confidence_threshold=0.95,
        )
        assert len(candidates) == 1
        assert candidates[0].descriptions_agree is False

    def test_a_merchant_token_may_still_contain_digits(
        self, unioned_table: Database
    ) -> None:
        """The rule rejects digit-*only* tokens, not merchant names carrying digits.

        Real merchant strings are full of them — `7-ELEVEN`, `STORE 1234 CVS`.
        Requiring the whole token to be alphabetic would throw away the ordinary
        truncation agreements this gate exists to find, so the requirement is one
        letter somewhere in the token, not the absence of digits.
        """
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-12.75",
            description="POS 7ELEVEN",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-12.75",
            description="POS 7ELEVEN 22140 SEATTLE WA",
            source_type="csv",
            source_origin="chase",
        )
        candidates = get_candidates_cross_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=3,
            high_confidence_threshold=0.95,
        )
        assert len(candidates) == 1
        assert candidates[0].descriptions_agree is True

    def test_a_fragment_starting_mid_token_is_not_agreement(
        self, unioned_table: Database
    ) -> None:
        """A merchant name buried inside another word names a different merchant.

        `ARCO` sits inside `MARCOS PIZZA` at character level, so a raw substring
        test reads two unrelated merchants as the same one. Nothing else stops it:
        the fragment is a real merchant name, so it is neither boilerplate nor
        digits, and equal-amount pairs inside the window are common. The merge
        would delete one of the two charges with no review entry.

        The rule that separates this from the truncation case is *where* the
        contained side starts. A source truncating a shared string cuts the tail,
        and a source wrapping it in a preamble leaves a space in front — both begin
        at a token boundary. A fragment that begins inside a word cannot be
        produced either way.

        Isolation: both sides are non-empty, containment genuinely holds, `ARCO` is
        not in `_BOILERPLATE_TOKENS`, it carries a letter, and the two strings
        differ. Only the token-boundary requirement can refuse this pair.
        """
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-31.20",
            description="ARCO",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-31.20",
            description="MARCOS PIZZA",
            source_type="csv",
            source_origin="chase",
        )
        candidates = get_candidates_cross_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=3,
            high_confidence_threshold=0.95,
        )
        assert len(candidates) == 1
        assert candidates[0].descriptions_agree is False

    def test_punctuation_alone_does_not_defeat_agreement(
        self, unioned_table: Database
    ) -> None:
        """One source writing `#` where the other writes nothing still agrees.

        Agreement is the only route to auto-merge, so a character of punctuation
        cannot be allowed to sink an otherwise identical pair. Two sources
        rendering one transaction as `STARBUCKS #1234` and `STARBUCKS 1234` is
        the ordinary case this feature exists to absorb; without this, it costs a
        human decision every time.

        Isolation: both sides are non-empty, both carry a merchant token, and the
        two differ *only* in punctuation — so nothing but the punctuation
        handling can decide this pair.
        """
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
            description="STARBUCKS 1234",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        candidates = get_candidates_cross_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=5,
            high_confidence_threshold=0.95,
        )
        assert len(candidates) == 1
        assert candidates[0].descriptions_agree is True

    def test_a_differing_reference_number_is_not_agreement(
        self, unioned_table: Database
    ) -> None:
        """Dropping punctuation must not make two different references equal.

        This is the pair the whole agreement gate exists to refuse: same
        merchant, same amount, same day, descriptions differing only in a
        trailing digit. Jaro-Winkler puts it near 0.96, high enough that the
        weighted formula alone would have merged it silently. Punctuation
        handling widens what agrees, so it is pinned here that it does not widen
        this far — a digit is not punctuation.

        Isolation: both sides are non-empty and both carry a merchant token, so
        only the containment test can keep this pair out of agreement.
        """
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_abc",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="SHELL 1234",
            source_type="csv",
            source_origin="chase",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="ofx_xyz",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="SHELL 1235",
            source_type="ofx",
            source_origin="chase_ofx",
        )
        candidates = get_candidates_cross_source(
            unioned_table,
            table="main._test_unioned",
            date_window_days=5,
            high_confidence_threshold=0.95,
        )
        assert len(candidates) == 1
        assert candidates[0].descriptions_agree is False

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

    def test_truncated_description_does_not_auto_merge_within_a_source(
        self, unioned_table: Database
    ) -> None:
        """The description-agreement floor is cross-source only.

        A single source renders a given transaction the same way every time, so
        two rows it wrote *differently* are evidence of two transactions, not of
        one transaction described twice. The truncation relationship that earns a
        silent merge across sources therefore must not earn one inside a source.

        This is the exact fixture that auto-merges under tier 3; if the floor ever
        leaks into tier 2b, this test is what catches it.
        """
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_a",
            account_id="acct1",
            transaction_date="2026-03-15",
            amount="-42.50",
            description="STARBUCKS STORE 1234 NEW YORK NY",
            source_type="csv",
            source_origin="chase",
            source_file="jan.csv",
        )
        _insert_unioned_row(
            unioned_table,
            source_transaction_id="csv_b",
            account_id="acct1",
            transaction_date="2026-03-17",
            amount="-42.50",
            description="STARBUCK",
            source_type="csv",
            source_origin="chase",
            source_file="feb.csv",
        )
        candidates = get_candidates_within_source(
            unioned_table, table="main._test_unioned", date_window_days=3
        )
        assert len(candidates) == 1
        assert (
            candidates[0].confidence_score
            < MatchingSettings().high_confidence_threshold
        )

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
