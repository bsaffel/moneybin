"""Integration tests for the transfer detection pipeline."""

import json

import pytest

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.engine import TransactionMatcher
from moneybin.matching.persistence import (
    get_active_matches,
    get_pending_matches,
)
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.services.matching_service import MatchingService


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
    stype: str = "csv",
    sorigin: str = "bank",
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

        MatchDecisionsRepo(db).update_status(
            pending[0]["match_id"], status="accepted", decided_by="user", actor="cli"
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
        repo = MatchDecisionsRepo(db)
        repo.update_status(
            pending[0]["match_id"],
            status="accepted",
            decided_by="user",
            actor="cli",
        )

        active = get_active_matches(db, match_type="transfer")
        assert len(active) == 1

        repo.reverse(active[0]["match_id"], reversed_by="user", actor="cli")
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


def _insert_transfer(
    db: Database,
    *,
    match_id: str,
    stid_a: str,
    stid_b: str,
    account_id: str,
    account_id_b: str,
    decided_at: str,
    type_a: str = "ofx",
    type_b: str = "ofx",
) -> None:
    """Seed one accepted transfer decision with an explicit decision time.

    ``decided_at`` is explicit because the retirement keeps the *earliest*
    claimant of a dedup component; rows stamped CURRENT_TIMESTAMP in one
    statement are indistinguishable and the tiebreak would fall to match_id.
    ``type_a``/``type_b`` are explicit because a leg's node identity is
    ``(source_type, source_transaction_id, account_id)`` — a leg typed
    differently from the dedup edge naming it lands in no component at all.
    """
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES (?, ?, ?, 'bank', ?, ?, 'bank', ?, 0.95, '{}',
                  'transfer', '4', ?, 'accepted', NULL, 'user', ?)
        """,  # noqa: S608 — test input, not user data
        [
            match_id,
            stid_a,
            type_a,
            stid_b,
            type_b,
            account_id,
            account_id_b,
            decided_at,
        ],
    )


def _insert_dedup(
    db: Database,
    *,
    match_id: str,
    stid_a: str,
    stid_b: str,
    account_id: str,
    status: str = "accepted",
) -> None:
    """Seed one dedup edge; only an accepted one actually forms a component.

    ``status`` is explicit because ``prep.int_transactions__matched`` folds
    ``match_status = 'accepted'`` rows only — a pending dedup row is an
    unreviewed proposal and both source rows stay distinct in ``core``.
    """
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type, match_tier,
            account_id_b, match_status, match_reason, decided_by, decided_at
        ) VALUES (?, ?, 'ofx', 'bank', ?, 'csv', 'bank', ?, 0.95, '{}',
                  'dedup', '3', NULL, ?, NULL, 'auto',
                  CURRENT_TIMESTAMP)
        """,  # noqa: S608 — test input, not user data
        [match_id, stid_a, stid_b, account_id, status],
    )


def _transfer_statuses(db: Database) -> dict[str, str]:
    return dict(
        db.execute(
            "SELECT match_id, match_status FROM app.match_decisions "
            "WHERE match_type = 'transfer'"
        ).fetchall()
    )


def _dedup_statuses(db: Database) -> dict[str, str]:
    return dict(
        db.execute(
            "SELECT match_id, match_status FROM app.match_decisions "
            "WHERE match_type = 'dedup'"
        ).fetchall()
    )


def _seed_two_doomed_transfers(db: Database, *, edge_status: str = "accepted") -> None:
    """One surviving transfer and two that lose the same component to it.

    Two retirements is the minimum that can distinguish "the count survived the
    exception" from "the count was zero anyway": with one, a carrier that always
    reported zero would still be reporting the truth.
    """
    _setup_tables(db)
    _insert_transfer(
        db,
        match_id="tx_keep00001",
        stid_a="ofx_p",
        stid_b="ofx_x",
        account_id="checking",
        account_id_b="brokerage",
        decided_at="2026-01-01 00:00:00",
    )
    for n, (stid, counterpart, month) in enumerate(
        (("csv_c", "ofx_y", "02"), ("csv_d", "ofx_z", "03")), start=1
    ):
        _insert_transfer(
            db,
            match_id=f"tx_drop0000{n}",
            stid_a=stid,
            stid_b=counterpart,
            type_a="csv",
            account_id="checking",
            account_id_b="savings",
            decided_at=f"2026-{month}-01 00:00:00",
        )
        _insert_dedup(
            db,
            match_id=f"dd_100000000{n}",
            stid_a="ofx_p",
            stid_b=stid,
            account_id="checking",
            status=edge_status,
        )


def _seed_a_stale_pending_transfer(db: Database) -> None:
    """An accepted transfer, a merged component, and a queued transfer over it.

    ``tx_stale00001`` was proposed before ``dd_1000000001`` merged the leg
    ``tx_keep00001`` already claims. Tier 4 refuses to raise that shape and never
    revisits what it raised earlier, so the queue is the only place it survives —
    which makes accepting it the one way a decision can reverse itself.
    """
    _setup_tables(db)
    _insert_transfer(
        db,
        match_id="tx_keep00001",
        stid_a="ofx_p",
        stid_b="ofx_x",
        account_id="checking",
        account_id_b="brokerage",
        decided_at="2026-01-01 00:00:00",
    )
    _insert_dedup(
        db,
        match_id="dd_1000000001",
        stid_a="ofx_p",
        stid_b="csv_c",
        account_id="checking",
    )
    db.execute(
        """
        INSERT INTO app.match_decisions (
            match_id, source_transaction_id_a, source_type_a, source_origin_a,
            source_transaction_id_b, source_type_b, source_origin_b,
            account_id, confidence_score, match_signals, match_type,
            match_tier, account_id_b, match_status, match_reason, decided_by,
            decided_at
        ) VALUES ('tx_stale00001', 'csv_c', 'csv', 'bank', 'ofx_y', 'ofx',
                  'bank', 'checking', 0.95, '{}', 'transfer', '4', 'savings',
                  'pending', NULL, 'auto', '2026-02-01 00:00:00')
        """  # noqa: S608 — test input, not user data
    )


class TestTransferRetirement:
    """The matcher's own reconciliation of transfers its dedup pass invalidated.

    ``bridge_transfers`` resolves each leg through the dedup mapping
    (``MAX(transaction_id)`` per group), so two accepted transfers whose legs
    landed in one dedup component name the same physical transaction and
    double-count it in every report joining ``fct_transactions`` to
    ``bridge_transfers``. Tier 4 refuses to *propose* that shape; this is the
    other direction — decisions accepted before the collapse.

    The unioned table is left empty wherever the run's own tiers are not the
    subject, so each fixture isolates the retirement rather than the matcher.
    """

    def test_a_plain_matcher_run_retires_a_transfer_invalidated_by_dedup(
        self, db: Database
    ) -> None:
        """No account merge anywhere — every matcher run owes this reconciliation.

        The dedup edge here is already accepted, which is what the ordinary
        review path leaves behind: ``reviews_decide`` writes the decision and
        returns, and nothing folds it into ``core`` until the next refresh. If
        the reconciliation is reachable only from the post-merge re-match, that
        refresh builds a corrupt ``bridge_transfers`` and no trigger ever
        revisits it.
        """
        _setup_tables(db)
        _insert_transfer(
            db,
            match_id="tx_keep00001",
            stid_a="ofx_p",
            stid_b="ofx_x",
            account_id="checking",
            account_id_b="brokerage",
            decided_at="2026-01-01 00:00:00",
        )
        _insert_transfer(
            db,
            match_id="tx_drop00001",
            stid_a="csv_c",
            stid_b="ofx_y",
            type_a="csv",
            account_id="checking",
            account_id_b="savings",
            decided_at="2026-02-01 00:00:00",
        )
        _insert_dedup(
            db,
            match_id="dd_1000000001",
            stid_a="ofx_p",
            stid_b="csv_c",
            account_id="checking",
        )

        result = TransactionMatcher(
            db, MatchingSettings(), table="main._test_unioned"
        ).run()

        statuses = _transfer_statuses(db)
        assert statuses["tx_keep00001"] == "accepted", (
            "the earliest transfer keeps the component it claimed first"
        )
        assert statuses["tx_drop00001"] == "reversed", (
            "the later transfer now names the same physical transaction as its "
            "debit leg, so it must not reach bridge_transfers"
        )
        assert result.transfers_retired == 1

    def test_a_leg_the_retirement_frees_is_paired_in_the_same_run(
        self, db: Database
    ) -> None:
        """Tier 4 must see the legs this run's own retirement released.

        ``_get_transfer_matched_ids`` excludes every leg of an active transfer,
        so ``ofx_y`` is invisible to Tier 4 while ``tx_drop00001`` still stands.
        Retiring that transfer after the transfer tier has already run leaves
        ``ofx_y`` waiting for an unrelated later refresh to notice it is free.
        """
        _setup_tables(db)
        _insert(
            db,
            "ofx_y",
            "savings",
            "2026-03-15",
            "500.00",
            "TRANSFER FROM CHK",
            stype="ofx",
        )
        _insert(
            db,
            "csv_new",
            "checking",
            "2026-03-15",
            "-500.00",
            "ONLINE TRANSFER TO SAV",
        )
        _insert_transfer(
            db,
            match_id="tx_keep00001",
            stid_a="ofx_p",
            stid_b="ofx_x",
            account_id="checking",
            account_id_b="brokerage",
            decided_at="2026-01-01 00:00:00",
        )
        _insert_transfer(
            db,
            match_id="tx_drop00001",
            stid_a="csv_c",
            stid_b="ofx_y",
            type_a="csv",
            account_id="checking",
            account_id_b="savings",
            decided_at="2026-02-01 00:00:00",
        )
        _insert_dedup(
            db,
            match_id="dd_1000000001",
            stid_a="ofx_p",
            stid_b="csv_c",
            account_id="checking",
        )

        result = TransactionMatcher(
            db, MatchingSettings(), table="main._test_unioned"
        ).run()

        assert result.pending_transfers == 1, (
            "the freed leg has a valid same-day counterpart and must be "
            "proposed by the run that freed it"
        )
        pending = get_pending_matches(db, match_type="transfer")
        assert {pending[0]["source_transaction_id_a"]} | {
            pending[0]["source_transaction_id_b"]
        } == {"csv_new", "ofx_y"}

    def test_the_same_two_rows_stay_unpaired_while_the_transfer_stands(
        self, db: Database
    ) -> None:
        """The fixture above minus the dedup edge — nothing is freed, nothing pairs.

        Differs by exactly one property: whether a dedup decision joins the two
        debit legs. Without this partner the test above would pass on a matcher
        that never retires anything, because ``csv_new`` and ``ofx_y`` are a
        transfer pair on their own merits.
        """
        _setup_tables(db)
        _insert(
            db,
            "ofx_y",
            "savings",
            "2026-03-15",
            "500.00",
            "TRANSFER FROM CHK",
            stype="ofx",
        )
        _insert(
            db,
            "csv_new",
            "checking",
            "2026-03-15",
            "-500.00",
            "ONLINE TRANSFER TO SAV",
        )
        _insert_transfer(
            db,
            match_id="tx_keep00001",
            stid_a="ofx_p",
            stid_b="ofx_x",
            account_id="checking",
            account_id_b="brokerage",
            decided_at="2026-01-01 00:00:00",
        )
        _insert_transfer(
            db,
            match_id="tx_drop00001",
            stid_a="csv_c",
            stid_b="ofx_y",
            type_a="csv",
            account_id="checking",
            account_id_b="savings",
            decided_at="2026-02-01 00:00:00",
        )

        result = TransactionMatcher(
            db, MatchingSettings(), table="main._test_unioned"
        ).run()

        assert result.transfers_retired == 0
        assert result.pending_transfers == 0, (
            "ofx_y is still claimed by an accepted transfer, so Tier 4 must "
            "leave it alone"
        )

    def test_a_pending_dedup_edge_retires_no_transfer(self, db: Database) -> None:
        """The first fixture with the dedup edge left pending — nothing retires.

        Differs by exactly one property: ``match_status`` on the dedup row. A
        pending dedup decision is an unreviewed *proposal*;
        ``prep.int_transactions__matched`` folds accepted rows only, so `ofx_p`
        and `csv_c` are still two distinct transactions in ``core`` and neither
        transfer is invalid yet. Retiring one here would reverse a decision the
        user made on the strength of a proposal nobody confirmed — and the
        human may go on to reject it.
        """
        _setup_tables(db)
        _insert_transfer(
            db,
            match_id="tx_keep00001",
            stid_a="ofx_p",
            stid_b="ofx_x",
            account_id="checking",
            account_id_b="brokerage",
            decided_at="2026-01-01 00:00:00",
        )
        _insert_transfer(
            db,
            match_id="tx_keep00002",
            stid_a="csv_c",
            stid_b="ofx_y",
            type_a="csv",
            account_id="checking",
            account_id_b="savings",
            decided_at="2026-02-01 00:00:00",
        )
        _insert_dedup(
            db,
            match_id="dd_1000000001",
            stid_a="ofx_p",
            stid_b="csv_c",
            account_id="checking",
            status="pending",
        )

        result = TransactionMatcher(
            db, MatchingSettings(), table="main._test_unioned"
        ).run()

        assert result.transfers_retired == 0
        assert _transfer_statuses(db) == {
            "tx_keep00001": "accepted",
            "tx_keep00002": "accepted",
        }, "an unreviewed dedup proposal must not reverse an accepted transfer"

    def test_transfers_on_distinct_components_all_survive(self, db: Database) -> None:
        """The same two transfers with no dedup edge joining them — nothing retires.

        Differs from the first fixture by exactly one property: whether a dedup
        decision links the two debit legs. Without this partner, a retirement
        that keyed on "two accepted transfers on one account" rather than on a
        shared component would pass that test and quietly delete correct
        transfers.
        """
        _setup_tables(db)
        _insert_transfer(
            db,
            match_id="tx_keep00001",
            stid_a="ofx_p",
            stid_b="ofx_x",
            account_id="checking",
            account_id_b="brokerage",
            decided_at="2026-01-01 00:00:00",
        )
        _insert_transfer(
            db,
            match_id="tx_keep00002",
            stid_a="csv_c",
            stid_b="ofx_y",
            account_id="checking",
            account_id_b="savings",
            decided_at="2026-02-01 00:00:00",
        )

        result = TransactionMatcher(
            db, MatchingSettings(), table="main._test_unioned"
        ).run()

        assert result.transfers_retired == 0
        assert _transfer_statuses(db) == {
            "tx_keep00001": "accepted",
            "tx_keep00002": "accepted",
        }

    def test_a_transfer_whose_own_two_legs_dedup_together_is_retired(
        self, db: Database
    ) -> None:
        """Both legs in one component — a transfer from a transaction to itself.

        ``repoint_account`` already retires the account-level form of this (the
        two legs collapsing onto one account). This is the transaction-level
        form, and it is equally impossible: ``bridge_transfers`` would emit a
        row whose debit and credit ``transaction_id`` are the same.
        """
        _setup_tables(db)
        _insert_transfer(
            db,
            match_id="tx_self00001",
            stid_a="ofx_p",
            stid_b="csv_c",
            type_b="csv",
            account_id="checking",
            account_id_b="checking",
            decided_at="2026-01-01 00:00:00",
        )
        _insert_dedup(
            db,
            match_id="dd_1000000001",
            stid_a="ofx_p",
            stid_b="csv_c",
            account_id="checking",
        )

        result = TransactionMatcher(
            db, MatchingSettings(), table="main._test_unioned"
        ).run()

        assert result.transfers_retired == 1
        assert _transfer_statuses(db) == {"tx_self00001": "reversed"}


def _seed_two_transfers_one_pending_edge(db: Database, *, edge_status: str) -> None:
    """Two valid transfers plus the dedup edge that would collide their legs.

    ``tx_keep00001`` and ``tx_drop00001`` are independently valid: their legs sit
    in four distinct components while the edge is unaccepted. Accepting
    ``dd_1000000001`` merges ``ofx_p`` with ``csv_c``, at which point both
    transfers resolve their debit leg to the same physical transaction.
    """
    _setup_tables(db)
    _insert_transfer(
        db,
        match_id="tx_keep00001",
        stid_a="ofx_p",
        stid_b="ofx_x",
        account_id="checking",
        account_id_b="brokerage",
        decided_at="2026-01-01 00:00:00",
    )
    _insert_transfer(
        db,
        match_id="tx_drop00001",
        stid_a="csv_c",
        stid_b="ofx_y",
        type_a="csv",
        account_id="checking",
        account_id_b="savings",
        decided_at="2026-02-01 00:00:00",
    )
    _insert_dedup(
        db,
        match_id="dd_1000000001",
        stid_a="ofx_p",
        stid_b="csv_c",
        account_id="checking",
        status=edge_status,
    )


class TestRetirementOnDedupAccept:
    """The reconciliation on the review-queue accept, where no matcher runs.

    ``MatchingService.set_status`` writes the decision and returns; nothing
    downstream re-derives anything, and nothing needs to. ``core.fct_transactions``
    and ``core.bridge_transfers`` are ``kind VIEW`` over ``app.match_decisions``,
    so the collision this reconciliation prevents is live on the next read rather
    than deferred to the next refresh. Every fixture here leaves the unioned
    table empty — the accept path is the subject, not the tiers.
    """

    def test_accepting_a_queued_dedup_retires_the_transfer_it_invalidates(
        self, db: Database
    ) -> None:
        """The queue accept, driven the way ``review --confirm`` drives it.

        The edge starts ``pending`` — an unreviewed proposal collapses nothing,
        so both transfers are still valid when this test begins. Accepting it is
        what makes ``tx_drop00001``'s debit leg the same physical transaction as
        ``tx_keep00001``'s.
        """
        _seed_two_transfers_one_pending_edge(db, edge_status="pending")

        outcome = MatchingService(db).set_status(
            "dd_1000000001", status="accepted", actor="cli"
        )

        assert outcome.transfers_retired == 1
        # The reconciliation retired a *different* row, so this accept committed
        # exactly what was asked for. Twin of the stale-accept case below: without
        # it, an implementation that reported "reversed" whenever anything was
        # retired would pass that test.
        assert outcome.match_status == "accepted"
        assert _transfer_statuses(db) == {
            "tx_keep00001": "accepted",
            "tx_drop00001": "reversed",
        }

    def test_rejecting_the_queued_dedup_retires_nothing(self, db: Database) -> None:
        """Negative twin: the same fixture, decided the other way.

        Without it the test above would pass on an implementation that
        reconciles after *any* decision, which would reverse an accepted
        transfer on the strength of a duplicate the user explicitly said was
        not one.
        """
        _seed_two_transfers_one_pending_edge(db, edge_status="pending")

        outcome = MatchingService(db).set_status(
            "dd_1000000001", status="rejected", actor="cli"
        )

        assert outcome.transfers_retired == 0
        assert outcome.match_status == "rejected"
        assert _transfer_statuses(db) == {
            "tx_keep00001": "accepted",
            "tx_drop00001": "accepted",
        }

    def test_accepting_every_queued_match_at_once_retires_what_it_invalidates(
        self, db: Database
    ) -> None:
        """``review --confirm-all`` folds every pending edge in one transaction.

        It routes through ``MatchDecisionsRepo.accept_pending`` rather than
        ``set_status``, so it is a second entry point to the same corruption and
        owes the same reconciliation.
        """
        _seed_two_transfers_one_pending_edge(db, edge_status="pending")

        outcome = MatchingService(db).accept_all_pending(actor="cli")

        assert outcome.accepted == 1
        assert outcome.transfers_retired == 1
        # The row this call flipped is not the row the reconciliation reversed,
        # so the accepted count stands. Twin of the self-reversing case below:
        # without it, reporting `accepted - transfers_retired` would pass there
        # and quietly report 0 here.
        assert outcome.reversed_by_reconciliation == 0
        assert _transfer_statuses(db) == {
            "tx_keep00001": "accepted",
            "tx_drop00001": "reversed",
        }

    def test_bulk_accept_does_not_count_a_row_its_own_reconciliation_reversed(
        self, db: Database
    ) -> None:
        """The bulk twin of the self-reversing accept.

        ``accept_pending`` flips the stale transfer, then the reconciliation in
        the same transaction reverses it because an earlier transfer already
        claims the component. Reporting the pre-reconciliation count calls that
        row accepted while it committed as ``reversed``, and the aggregate
        retirement warning underneath does not contradict it.
        """
        _seed_a_stale_pending_transfer(db)

        outcome = MatchingService(db).accept_all_pending(actor="cli")

        assert outcome.accepted == 0
        assert outcome.reversed_by_reconciliation == 1
        assert outcome.transfers_retired == 1
        assert _transfer_statuses(db) == {
            "tx_keep00001": "accepted",
            "tx_stale00001": "reversed",
        }

    def test_accepting_a_stale_queued_transfer_refuses_the_claimed_component(
        self, db: Database
    ) -> None:
        """A transfer proposed before the dedup edge that invalidated it.

        Tier 4 refuses to *propose* a transfer over an already-merged component,
        but a proposal raised earlier survives in the queue and Tier 4 never
        revisits it. This is why the reconciliation is not scoped to dedup
        accepts. The newest claimant loses, so the effect is that the stale
        accept is refused rather than the standing decision undone.
        """
        _seed_a_stale_pending_transfer(db)

        outcome = MatchingService(db).set_status(
            "tx_stale00001", status="accepted", actor="cli"
        )

        assert outcome.transfers_retired == 1
        # The row this call asked to accept is the one the reconciliation
        # reversed. Echoing the requested status here reports the opposite of
        # what committed, and both surfaces print it as a success.
        assert outcome.match_status == "reversed"
        assert _transfer_statuses(db) == {
            "tx_keep00001": "accepted",
            "tx_stale00001": "reversed",
        }


class TestPartialMatchRunDisclosure:
    """A run that dies partway still owes whatever it already committed.

    The matcher opens no transaction, so every dedup decision and every reversal
    is durable the moment it is written — and ``run()`` returns none of them when
    a later step raises. The disclosure is the same in both halves: work the user
    can see in the ledger, reported as zero. Its clean-run twin is the existing
    pass that asserts the same counts off a returned ``MatchResult``.
    """

    def test_a_tier_4_failure_still_reports_the_transfers_already_reversed(
        self, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Tier 4 raises after a real reversal has already committed."""
        from moneybin.matching.engine import MatchRunError

        _seed_two_transfers_one_pending_edge(db, edge_status="accepted")

        def _boom(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("tier 4 boom")

        monkeypatch.setattr(TransactionMatcher, "_run_transfer_tier", _boom)

        with pytest.raises(MatchRunError) as excinfo:
            TransactionMatcher(db, MatchingSettings(), table="main._test_unioned").run()

        assert excinfo.value.partial.transfers_retired == 1
        assert str(excinfo.value) == "tier 4 boom"
        # The reversal is durable: the wrapper reports it, it does not undo it.
        assert _transfer_statuses(db) == {
            "tx_keep00001": "accepted",
            "tx_drop00001": "reversed",
        }

    def test_a_reconciliation_failure_still_reports_the_reversals_it_committed(
        self, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The guard has to cover the reconciliation, not just what follows it.

        Its reversals commit one at a time, so a crash partway through the loop
        leaves the earlier ones durable — the same shape the Tier 4 test above
        pins, one call earlier. The count lives only in a local until the
        function returns, so an exception inside it loses every reversal it had
        already made.
        """
        from moneybin.matching.engine import MatchRunError

        _seed_two_doomed_transfers(db)
        real_reverse = MatchDecisionsRepo.reverse
        calls = {"n": 0}

        def _flaky(self: MatchDecisionsRepo, match_id: str, **kwargs: object) -> object:
            calls["n"] += 1
            if calls["n"] > 1:
                raise RuntimeError("reconciliation boom")
            return real_reverse(self, match_id, **kwargs)  # pyright: ignore[reportArgumentType]  # passthrough kwargs

        monkeypatch.setattr(MatchDecisionsRepo, "reverse", _flaky)

        with pytest.raises(MatchRunError) as excinfo:
            TransactionMatcher(db, MatchingSettings(), table="main._test_unioned").run()

        assert excinfo.value.partial.transfers_retired == 1
        assert str(excinfo.value) == "reconciliation boom"
        statuses = _transfer_statuses(db)
        assert sum(1 for s in statuses.values() if s == "reversed") == 1
        assert statuses["tx_keep00001"] == "accepted"

    def test_a_rolled_back_reconciliation_reports_no_reversals(
        self, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Negative twin: the count is only owed when the reversals are durable.

        An accept path folds the reconciliation into its own transaction, so a
        crash inside it takes the reversals down with the accept. Reporting a
        count there would name a decision as undone that the rollback restored
        — the same over-reporting the committed-status re-read exists to stop.
        """
        _seed_two_doomed_transfers(db, edge_status="pending")

        def _boom(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("reconciliation boom")

        monkeypatch.setattr(MatchDecisionsRepo, "reverse", _boom)

        with pytest.raises(RuntimeError, match="reconciliation boom"):
            MatchingService(db).accept_all_pending(actor="cli")

        assert "reversed" not in set(_transfer_statuses(db).values())
        # The accepts rolled back with the reversals, which is what makes the
        # count meaningless rather than merely unavailable.
        assert _dedup_statuses(db) == {
            "dd_1000000001": "pending",
            "dd_1000000002": "pending",
        }

    def test_a_dedup_tier_crash_still_reports_the_decisions_it_committed(
        self, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The tiers commit as they go, exactly like the reconciliation below them.

        A tier persists one decision per assigned pair with no transaction around
        the loop, so a pair that raises leaves every earlier pair's decision in
        the ledger. Those merges suppress the duplicate side in
        ``core.fct_transactions`` — the ledger changes — while a caller told only
        "matching failed" reads zero and concludes nothing was written.
        """
        from moneybin.matching.engine import MatchRunError

        _setup_tables(db)
        for n in (1, 2):
            _insert(
                db, f"csv_dup{n}", "acct1", "2026-03-15", f"-{n}0.00", f"COFFEE {n}"
            )
            _insert(
                db,
                f"ofx_dup{n}",
                "acct1",
                "2026-03-15",
                f"-{n}0.00",
                f"COFFEE {n}",
                "ofx",
                "bank_ofx",
            )

        real_persist = TransactionMatcher._persist_dedup_match  # pyright: ignore[reportPrivateUsage]
        calls = {"n": 0}

        def _flaky(self: TransactionMatcher, *args: object, **kwargs: object) -> None:
            calls["n"] += 1
            if calls["n"] > 1:
                raise RuntimeError("tier 3 boom")
            real_persist(self, *args, **kwargs)  # pyright: ignore[reportPrivateUsage, reportArgumentType]  # passthrough

        monkeypatch.setattr(TransactionMatcher, "_persist_dedup_match", _flaky)

        with pytest.raises(MatchRunError) as excinfo:
            TransactionMatcher(db, MatchingSettings(), table="main._test_unioned").run()

        assert str(excinfo.value) == "tier 3 boom"
        assert excinfo.value.partial.auto_merged == 1
        # The count names what committed, not what the loop reached: the second
        # pair incremented no counter because its write never landed.
        assert len(_dedup_statuses(db)) == 1

    def test_a_run_that_committed_nothing_raises_its_own_error_unwrapped(
        self, db: Database, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Negative twin: the carrier is earned by a durable write, not by failing.

        ``refresh()`` reads a bare ``CatalogException`` from the tiers as the
        first-load "views not built yet" precondition and stays quiet. Wrapping
        every failure would turn that expected first run into a reported error,
        so a run with nothing in the ledger has to let its own exception through.
        """
        import duckdb

        from moneybin.matching.engine import MatchRunError

        _setup_tables(db)

        def _boom(*_args: object, **_kwargs: object) -> None:
            raise duckdb.CatalogException("no view")

        monkeypatch.setattr(TransactionMatcher, "_run_tier", _boom)

        with pytest.raises(duckdb.CatalogException):
            TransactionMatcher(db, MatchingSettings(), table="main._test_unioned").run()

        # Not merely "some exception": the wrapper must not be what escaped.
        with pytest.raises(BaseException) as excinfo:  # noqa: B017, PT011  # identity check
            TransactionMatcher(db, MatchingSettings(), table="main._test_unioned").run()
        assert not isinstance(excinfo.value, MatchRunError)
