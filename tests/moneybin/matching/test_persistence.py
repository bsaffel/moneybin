"""Tests for match decision read queries.

Mutations (insert / status update / reverse) are owned by
``MatchDecisionsRepo`` and tested in
``tests/moneybin/test_repositories/test_match_decisions_repo.py``; this module
sets up rows via the repo and exercises the read projections in
``matching/persistence.py``.
"""

import uuid
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.matching.persistence import (
    MatchStatus,
    get_active_dedup_edges,
    get_active_matches,
    get_match_decision,
    get_match_log,
    get_pending_matches,
    get_rejected_pairs,
)
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Generator[Database, None, None]:
    """Provide a fresh test database with all schemas initialised."""
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
        read_only=False,
    )
    yield database
    database.close()


def _make_match_id() -> str:
    return uuid.uuid4().hex[:12]


def _create_test_match(
    db: Database,
    *,
    match_id: str | None = None,
    status: MatchStatus = "accepted",
    confidence: float = 0.98,
    stid_a: str = "a",
    stid_b: str = "b",
    match_type: str = "dedup",
    match_tier: str | None = "3",
    account_id_b: str | None = None,
) -> str:
    """Helper to create a match decision with sensible defaults (via the repo)."""
    mid = match_id or _make_match_id()
    MatchDecisionsRepo(db).insert(
        match_id=mid,
        source_transaction_id_a=stid_a,
        source_type_a="csv",
        source_origin_a="c",
        source_transaction_id_b=stid_b,
        source_type_b="ofx",
        source_origin_b="c",
        account_id="acct1",
        confidence_score=confidence,
        match_signals={},
        match_tier=match_tier,
        match_status=status,
        decided_by="auto",
        match_type=match_type,
        account_id_b=account_id_b,
        actor="system",
    )
    return mid


class TestGetActiveMatches:
    """Tests for get_active_matches."""

    def test_returns_accepted_non_reversed(self, db: Database) -> None:
        match_id = _create_test_match(db)
        matches = get_active_matches(db)
        assert len(matches) == 1
        assert matches[0]["match_id"] == match_id

    def test_excludes_reversed_matches(self, db: Database) -> None:
        match_id = _create_test_match(db)
        MatchDecisionsRepo(db).reverse(match_id, reversed_by="user", actor="cli")
        matches = get_active_matches(db)
        assert len(matches) == 0

    def test_excludes_pending_matches(self, db: Database) -> None:
        _create_test_match(db, status="pending", stid_a="p1", stid_b="p2")
        matches = get_active_matches(db)
        assert len(matches) == 0

    def test_excludes_rejected_matches(self, db: Database) -> None:
        _create_test_match(db, status="rejected", stid_a="r1", stid_b="r2")
        matches = get_active_matches(db)
        assert len(matches) == 0

    def test_filters_by_match_type(self, db: Database) -> None:
        _create_test_match(
            db,
            stid_a="t1",
            stid_b="t2",
            confidence=0.95,
            match_type="transfer",
            match_tier=None,
            account_id_b="acct2",
        )
        dedup_matches = get_active_matches(db, match_type="dedup")
        transfer_matches = get_active_matches(db, match_type="transfer")
        assert len(dedup_matches) == 0
        assert len(transfer_matches) == 1


class TestGetActiveDedupEdges:
    """Tests for get_active_dedup_edges."""

    def test_returns_accepted_and_pending_dedup(self, db: Database) -> None:
        _create_test_match(db, status="accepted", stid_a="a1", stid_b="b1")
        _create_test_match(db, status="pending", stid_a="a2", stid_b="b2")
        _create_test_match(db, status="rejected", stid_a="a3", stid_b="b3")
        edges = get_active_dedup_edges(db)
        # accepted + pending only; rejected excluded.
        assert len(edges) == 2

    def test_ordered_deterministically(self, db: Database) -> None:
        # Insert side-A ids out of order; the query must return them sorted so
        # downstream component assignment is stable across DuckDB scan order
        # (VACUUM, storage reorg) without relying on insertion order.
        for stid_a in ("t3", "t1", "t2"):
            _create_test_match(db, status="accepted", stid_a=stid_a, stid_b="z")
        edges = get_active_dedup_edges(db)
        assert [e["source_transaction_id_a"] for e in edges] == ["t1", "t2", "t3"]


class TestGetPendingMatches:
    """Tests for get_pending_matches."""

    def test_returns_pending_only(self, db: Database) -> None:
        pending_id = _create_test_match(db, status="pending")
        _create_test_match(db, status="accepted", stid_a="c", stid_b="d")
        pending = get_pending_matches(db)
        assert len(pending) == 1
        assert pending[0]["match_id"] == pending_id

    def test_ordered_by_confidence_desc(self, db: Database) -> None:
        _create_test_match(
            db, status="pending", confidence=0.70, stid_a="p1", stid_b="p2"
        )
        _create_test_match(
            db, status="pending", confidence=0.90, stid_a="p3", stid_b="p4"
        )
        pending = get_pending_matches(db)
        assert pending[0]["confidence_score"] >= pending[1]["confidence_score"]


class TestGetRejectedPairs:
    """Tests for get_rejected_pairs."""

    def test_returns_rejected_pair_keys(self, db: Database) -> None:
        _create_test_match(db, status="rejected", confidence=0.75)
        rejected = get_rejected_pairs(db)
        assert len(rejected) == 1

    def test_excludes_accepted_matches(self, db: Database) -> None:
        _create_test_match(db, status="accepted", stid_a="a1", stid_b="b1")
        rejected = get_rejected_pairs(db)
        assert len(rejected) == 0

    def test_rejected_pair_has_expected_keys(self, db: Database) -> None:
        _create_test_match(db, status="rejected", stid_a="src_a", stid_b="src_b")
        rejected = get_rejected_pairs(db)
        assert "source_transaction_id_a" in rejected[0]
        assert "source_transaction_id_b" in rejected[0]
        assert "account_id" in rejected[0]


class TestGetMatchDecision:
    """Tests for get_match_decision."""

    def test_returns_row_when_present(self, db: Database) -> None:
        _create_test_match(
            db,
            match_id="m_abc123",
            status="pending",
            stid_a="a1",
            stid_b="b1",
        )
        row = get_match_decision(db, "m_abc123")
        assert row is not None
        assert row["match_id"] == "m_abc123"
        assert row["match_status"] == "pending"

    def test_returns_none_when_absent(self, db: Database) -> None:
        assert get_match_decision(db, "nope") is None


class TestGetMatchLog:
    """Tests for get_match_log."""

    def test_returns_recent_decisions_excluding_pending(self, db: Database) -> None:
        _create_test_match(db, stid_a="l1", stid_b="l2")  # accepted (default)
        _create_test_match(db, status="pending", stid_a="l3", stid_b="l4")
        log = get_match_log(db)
        # Pending proposals are not decisions — the log excludes them.
        assert len(log) == 1
        assert all(row["match_status"] != "pending" for row in log)

    def test_respects_limit(self, db: Database) -> None:
        for i in range(5):
            _create_test_match(db, stid_a=f"a{i}", stid_b=f"b{i}")
        log = get_match_log(db, limit=3)
        assert len(log) == 3

    def test_filters_by_match_type(self, db: Database) -> None:
        _create_test_match(db, stid_a="d1", stid_b="d2")  # dedup
        _create_test_match(
            db,
            stid_a="t1",
            stid_b="t2",
            confidence=0.95,
            match_type="transfer",
            match_tier=None,
            account_id_b="acct2",
        )
        transfer_log = get_match_log(db, match_type="transfer")
        assert len(transfer_log) == 1
        assert transfer_log[0]["match_type"] == "transfer"
