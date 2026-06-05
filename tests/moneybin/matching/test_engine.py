"""Tests for TransactionMatcher orchestrator."""

from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.config import MatchingSettings
from moneybin.database import Database
from moneybin.matching.engine import MatchResult, TransactionMatcher
from moneybin.matching.persistence import (
    get_active_matches,
    get_pending_matches,
)
from moneybin.matching.scoring import CandidatePair
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo


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
        assert matcher._classify_pair(_make_pair(0.95), "2b") == ("accepted", "auto")  # pyright: ignore[reportPrivateUsage]

    def test_high_confidence_returns_accepted_for_3(self) -> None:
        matcher = _matcher_with_settings(
            high_confidence_threshold=0.90, review_threshold=0.70
        )
        assert matcher._classify_pair(_make_pair(0.95), "3") == ("accepted", "auto")  # pyright: ignore[reportPrivateUsage]

    def test_tier3_above_review_threshold_returns_pending(self) -> None:
        matcher = _matcher_with_settings(
            high_confidence_threshold=0.90, review_threshold=0.70
        )
        assert matcher._classify_pair(_make_pair(0.80), "3") == ("pending", "auto")  # pyright: ignore[reportPrivateUsage]

    def test_tier2b_above_review_threshold_returns_none(self) -> None:
        # Same confidence range as pending case, but tier 2b has no review bucket.
        matcher = _matcher_with_settings(
            high_confidence_threshold=0.90, review_threshold=0.70
        )
        assert matcher._classify_pair(_make_pair(0.80), "2b") is None  # pyright: ignore[reportPrivateUsage]

    def test_below_all_thresholds_returns_none(self) -> None:
        matcher = _matcher_with_settings(
            high_confidence_threshold=0.90, review_threshold=0.70
        )
        for tier in ("2b", "3"):
            result = matcher._classify_pair(_make_pair(0.50), tier)  # type: ignore[arg-type]  # pyright: ignore[reportPrivateUsage]
            assert result is None, f"Expected None for tier {tier!r}"


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Generator[Database, None, None]:
    """Provide a test Database instance scoped to this module."""
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    yield database
    database.close()


class TestFetchActiveDedupDecisions:
    """Equivalence check: _fetch_active_dedup_decisions covers pre-seeded and newly-created matches."""

    def test_returns_pre_seeded_and_new_matches(self, db: Database) -> None:
        """Verify _fetch_active_dedup_decisions covers pre-seeded and newly-created matches.

        Pre-seeds two dedup decisions, runs the matcher on a fresh pair, then
        asserts both active_edges and secondary_ids include all three pairs.
        """
        _create_test_table(db)

        # Pre-seed two accepted dedup decisions before any run.
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
        decisions = matcher._fetch_active_dedup_decisions()  # pyright: ignore[reportPrivateUsage]

        # active_edges must include one full-triple edge per pair (pre-seeded + new).
        # Each side is (source_type, source_transaction_id, account_id).
        assert (
            ("csv", "csv_pre1", "acct1"),
            ("ofx", "ofx_pre1", "acct1"),
        ) in decisions.active_edges
        assert (
            ("csv", "csv_pre2", "acct1"),
            ("ofx", "ofx_pre2", "acct1"),
        ) in decisions.active_edges
        # The new pair is persisted (source_type,source_transaction_id) ordered by
        # the candidate query's (source_type, source_origin, source_transaction_id)
        # tuple comparison: 'csv' < 'ofx', so csv is side a.
        assert (
            ("csv", "csv_new", "acct1"),
            ("ofx", "ofx_new", "acct1"),
        ) in decisions.active_edges

        # secondary_ids: ofx is lower-priority than csv per default source_priority,
        # so the ofx side of each pair is the secondary (excluded from transfers).
        assert ("ofx_pre1", "ofx", "acct1") in decisions.secondary_ids
        assert ("ofx_pre2", "ofx", "acct1") in decisions.secondary_ids
        assert ("ofx_new", "ofx", "acct1") in decisions.secondary_ids

    def test_secondary_ids_excludes_all_non_primary_members(self, db: Database) -> None:
        """Component-based exclusion must exclude ALL non-primary members.

        Topology that exposes the pairwise bug
        ----------------------------------------
        Three copies of one transaction in acct1:
          manual (priority index 0 — highest)
          parquet (priority index 6)
          ofx     (priority index 8 — lowest)

        Stored edges: manual–ofx and parquet–ofx  (a "V" shape, both pointing to ofx)

        Pairwise analysis (old logic):
          edge manual(0)–ofx(8):   pri_a(0) <= pri_b(8) → exclude ofx      ✓
          edge parquet(6)–ofx(8):  pri_a(6) <= pri_b(8) → exclude ofx      (already in secondary)
          Result: secondary = {ofx}.  parquet NOT excluded — BUG.
          parquet is non-primary (manual is primary) but pairwise misses it
          because parquet is the HIGHER-priority side of its own edge.

        Component-based analysis (correct):
          components = {{manual, parquet, ofx}}
          primary = min priority_index member = manual
          secondary = {parquet, ofx}  ← all non-primary members excluded
        """
        _create_test_table(db)

        now = datetime.now(tz=UTC).isoformat()
        db.execute(
            """
            INSERT INTO app.match_decisions
            (match_id, source_transaction_id_a, source_type_a, source_origin_a,
             source_transaction_id_b, source_type_b, source_origin_b,
             account_id, confidence_score, match_signals, match_type, match_tier,
             match_status, decided_by, decided_at)
            VALUES
            ('nway000001', 'man_x', 'manual', 'bank',
             'ofx_x', 'ofx', 'bank',
             'acct1', 0.99, '{}', 'dedup', '3', 'accepted', 'auto', ?),
            ('nway000002', 'parq_x', 'parquet', 'bank',
             'ofx_x', 'ofx', 'bank',
             'acct1', 0.99, '{}', 'dedup', '3', 'accepted', 'auto', ?)
            """,
            [now, now],
        )  # noqa: S608  # test fixture data, not user input

        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        decisions = matcher._fetch_active_dedup_decisions()  # pyright: ignore[reportPrivateUsage]
        secondary = decisions.secondary_ids

        # manual is primary (priority index 0 = highest): must NOT be excluded
        assert ("man_x", "manual", "acct1") not in secondary, (
            "manual is primary (highest priority) and must remain eligible for transfers"
        )
        # parquet (priority index 6) is non-primary: must be excluded
        assert ("parq_x", "parquet", "acct1") in secondary, (
            "parquet is non-primary — pairwise missed it because parquet is the "
            "higher-priority side of the parquet–ofx edge, but component-based "
            "exclusion must still exclude it"
        )
        # ofx (priority index 8) is non-primary: must be excluded
        assert ("ofx_x", "ofx", "acct1") in secondary, (
            "ofx is non-primary and must be excluded"
        )


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
        matches = get_active_matches(db, match_type="dedup")
        repo = MatchDecisionsRepo(db)
        repo.reverse(matches[0]["match_id"], reversed_by="user", actor="cli")
        repo.update_status(
            matches[0]["match_id"], status="rejected", decided_by="user", actor="cli"
        )

        # Second run: should not re-propose
        result2 = matcher.run()
        assert result2.auto_merged == 0
        assert result2.pending_review == 0

    def test_match_result_summary(self) -> None:
        result = MatchResult(auto_merged=5, pending_review=2)
        assert "5 auto-merged" in result.summary()
        assert "2 pending review" in result.summary()


def _active_dedup_edges(
    db: Database,
) -> list[tuple[tuple[str, str, str], tuple[str, str, str]]]:
    """Read active/pending dedup edges from app.match_decisions as node-key pairs.

    Each node is the full (source_type, source_transaction_id, account_id) triple
    — the same identity assign_components and the prep fold key on.
    """
    rows = db.execute(
        """
        SELECT source_type_a, source_transaction_id_a,
               source_type_b, source_transaction_id_b,
               account_id
        FROM app.match_decisions
        WHERE match_status IN ('accepted', 'pending')
          AND reversed_at IS NULL
          AND match_type = 'dedup'
        """
    ).fetchall()
    edges: list[tuple[tuple[str, str, str], tuple[str, str, str]]] = []
    for st_a, stid_a, st_b, stid_b, acct in rows:
        edges.append(((st_a, stid_a, acct), (st_b, stid_b, acct)))
    return edges


def _component_count(
    edges: list[tuple[tuple[str, str, str], tuple[str, str, str]]],
) -> int:
    """Count connected components formed by the given edges (union-find)."""
    parent: dict[tuple[str, str, str], tuple[str, str, str]] = {}

    def find(x: tuple[str, str, str]) -> tuple[str, str, str]:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in edges:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb
    roots = {find(n) for edge in edges for n in edge}
    return len(roots)


def _node_count(
    edges: list[tuple[tuple[str, str, str], tuple[str, str, str]]],
) -> int:
    """Count distinct nodes touched by the given edges."""
    nodes: set[tuple[str, str, str]] = set()
    for a, b in edges:
        nodes.add(a)
        nodes.add(b)
    return len(nodes)


class TestNWayDedup:
    """Tests for N-way (3+ copy) dedup forming a single component."""

    def test_three_copies_form_one_component(self, db: Database) -> None:
        """Three identical copies of one transaction must form ONE component.

        Fixture: same account, amount, date, and description across three rows —
        two csv (same source_type/source_origin, different source_file → a Tier-2b
        within-source pair) and one ofx (→ Tier-3 cross-source).

        Confidence math (date_window_days=3): identical dates → date_distance=0 →
        date_score=1.0; identical descriptions → jaro_winkler=1.0 → desc_sim=1.0;
        confidence = 0.40*1.0 + 0.60*1.0 = 1.0 for every pair. 1.0 >= 0.95
        (high_confidence_threshold) so every kept edge auto-merges as 'accepted'.

        Candidate pairs: Tier 2b → (csv_1, csv_2); Tier 3 → (csv_1, ofx_1),
        (csv_2, ofx_1). Union-find over 3 nodes keeps N-1 = 2 edges:
        Tier 2b adds (csv_1, csv_2); Tier 3 adds exactly one csv↔ofx edge (the
        second is already within the merged component). Expected: 2 edges,
        1 component, 3 nodes — before this stage only 1 edge was written and the
        ofx row was excluded entirely (1 component, 2 nodes).
        """
        _create_test_table(db)
        _insert(
            db,
            "csv_1",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS",
            "csv",
            "bank",
            sfile="a.csv",
        )
        _insert(
            db,
            "csv_2",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS",
            "csv",
            "bank",
            sfile="b.csv",
        )
        _insert(
            db,
            "ofx_1",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS",
            "ofx",
            "bank",
            sfile="x.ofx",
        )

        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        matcher.run()

        edges = _active_dedup_edges(db)
        assert _node_count(edges) == 3, (
            f"Expected all three copies covered by edges, got nodes from {edges}"
        )
        assert _component_count(edges) == 1, (
            f"Expected one connected component, got {edges}"
        )

    def test_cross_run_attachment(self, db: Database) -> None:
        """A copy imported in a later run attaches to the existing component.

        First run sees only the two csv copies → one accepted Tier-2b edge
        (csv_1, csv_2). The ofx copy is then inserted and the matcher re-runs;
        the seed_edges carry the prior edge, so Tier 3 attaches ofx_1 to the
        existing {csv_1, csv_2} component with a single new edge rather than
        forming a separate edge-less row. Expected after second run: 2 edges,
        1 component, 3 nodes.
        """
        _create_test_table(db)
        _insert(
            db,
            "csv_1",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS",
            "csv",
            "bank",
            sfile="a.csv",
        )
        _insert(
            db,
            "csv_2",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS",
            "csv",
            "bank",
            sfile="b.csv",
        )

        settings = MatchingSettings()
        matcher = TransactionMatcher(db, settings, table="main._test_unioned")
        matcher.run()

        first_edges = _active_dedup_edges(db)
        assert _node_count(first_edges) == 2
        assert _component_count(first_edges) == 1

        # Now import the third copy and re-run.
        _insert(
            db,
            "ofx_1",
            "acct1",
            "2026-03-15",
            "-42.50",
            "STARBUCKS",
            "ofx",
            "bank",
            sfile="x.ofx",
        )
        matcher.run()

        edges = _active_dedup_edges(db)
        assert _node_count(edges) == 3, (
            f"Expected ofx copy to attach to the existing component, got {edges}"
        )
        assert _component_count(edges) == 1, (
            f"Expected one connected component after attachment, got {edges}"
        )


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

        pending = get_pending_matches(db, match_type="transfer")
        repo = MatchDecisionsRepo(db)
        for m in pending:
            repo.update_status(
                m["match_id"], status="rejected", decided_by="user", actor="cli"
            )

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
