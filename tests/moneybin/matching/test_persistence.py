"""Tests for match decision persistence."""

import uuid
from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin.database import Database
from moneybin.matching.persistence import (
    MatchStatus,
    create_match_decision,
    get_active_matches,
    get_match_log,
    get_pending_matches,
    get_rejected_pairs,
    undo_match,
    update_match_status,
)


@pytest.fixture()
def db(tmp_path: Path, mock_secret_store: MagicMock) -> Generator[Database, None, None]:
    """Provide a fresh test database with all schemas initialised."""
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
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
) -> str:
    """Helper to create a match decision with sensible defaults."""
    mid = match_id or _make_match_id()
    create_match_decision(
        db,
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
        match_tier="3",
        match_status=status,
        decided_by="auto",
    )
    return mid


class TestCreateMatchDecision:
    """Tests for create_match_decision."""

    def test_creates_accepted_match(self, db: Database) -> None:
        match_id = _create_test_match(db, status="accepted", confidence=0.97)
        result = db.execute(
            "SELECT match_id, match_status, confidence_score "
            "FROM app.match_decisions WHERE match_id = ?",
            [match_id],
        ).fetchone()
        assert result is not None
        assert result[0] == match_id
        assert result[1] == "accepted"

    def test_creates_pending_match(self, db: Database) -> None:
        match_id = _create_test_match(db, status="pending", confidence=0.82)
        result = db.execute(
            "SELECT match_status FROM app.match_decisions WHERE match_id = ?",
            [match_id],
        ).fetchone()
        assert result is not None
        assert result[0] == "pending"

    def test_stores_match_signals_as_json(self, db: Database) -> None:
        mid = _make_match_id()
        create_match_decision(
            db,
            match_id=mid,
            source_transaction_id_a="x",
            source_type_a="csv",
            source_origin_a="bank",
            source_transaction_id_b="y",
            source_type_b="ofx",
            source_origin_b="bank",
            account_id="acct2",
            confidence_score=0.90,
            match_signals={"date_distance": 0, "amount_match": 1.0},
            match_tier="3",
            match_status="pending",
            decided_by="auto",
        )
        result = db.execute(
            "SELECT match_signals FROM app.match_decisions WHERE match_id = ?",
            [mid],
        ).fetchone()
        assert result is not None
        # match_signals stored as JSON string; non-empty
        assert "date_distance" in result[0]


class TestGetActiveMatches:
    """Tests for get_active_matches."""

    def test_returns_accepted_non_reversed(self, db: Database) -> None:
        match_id = _create_test_match(db)
        matches = get_active_matches(db)
        assert len(matches) == 1
        assert matches[0]["match_id"] == match_id

    def test_excludes_reversed_matches(self, db: Database) -> None:
        match_id = _create_test_match(db)
        undo_match(db, match_id, reversed_by="user")
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
        mid = _make_match_id()
        create_match_decision(
            db,
            match_id=mid,
            source_transaction_id_a="t1",
            source_type_a="csv",
            source_origin_a="bank",
            source_transaction_id_b="t2",
            source_type_b="ofx",
            source_origin_b="bank",
            account_id="acct1",
            confidence_score=0.95,
            match_signals={},
            match_tier=None,
            match_status="accepted",
            decided_by="auto",
            match_type="transfer",
            account_id_b="acct2",
        )
        dedup_matches = get_active_matches(db, match_type="dedup")
        transfer_matches = get_active_matches(db, match_type="transfer")
        assert len(dedup_matches) == 0
        assert len(transfer_matches) == 1


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


class TestUpdateMatchStatus:
    """Tests for update_match_status."""

    def test_updates_status_to_accepted(self, db: Database) -> None:
        match_id = _create_test_match(db, status="pending")
        update_match_status(db, match_id, status="accepted", decided_by="user")
        result = db.execute(
            "SELECT match_status, decided_by FROM app.match_decisions WHERE match_id = ?",
            [match_id],
        ).fetchone()
        assert result is not None
        assert result[0] == "accepted"
        assert result[1] == "user"

    def test_updates_status_to_rejected(self, db: Database) -> None:
        match_id = _create_test_match(db, status="pending")
        update_match_status(db, match_id, status="rejected", decided_by="user")
        result = db.execute(
            "SELECT match_status FROM app.match_decisions WHERE match_id = ?",
            [match_id],
        ).fetchone()
        assert result is not None
        assert result[0] == "rejected"


class TestUndoMatch:
    """Tests for undo_match."""

    def test_sets_reversed_fields(self, db: Database) -> None:
        match_id = _create_test_match(db)
        undo_match(db, match_id, reversed_by="user")
        result = db.execute(
            "SELECT reversed_at, reversed_by FROM app.match_decisions WHERE match_id = ?",
            [match_id],
        ).fetchone()
        assert result is not None
        assert result[0] is not None
        assert result[1] == "user"

    def test_reversed_match_no_longer_active(self, db: Database) -> None:
        match_id = _create_test_match(db)
        undo_match(db, match_id, reversed_by="system")
        active = get_active_matches(db)
        assert all(m["match_id"] != match_id for m in active)


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


class TestGetMatchLog:
    """Tests for get_match_log."""

    def test_returns_recent_decisions(self, db: Database) -> None:
        _create_test_match(db, stid_a="l1", stid_b="l2")
        _create_test_match(db, status="pending", stid_a="l3", stid_b="l4")
        log = get_match_log(db)
        assert len(log) == 2

    def test_respects_limit(self, db: Database) -> None:
        for i in range(5):
            _create_test_match(db, stid_a=f"a{i}", stid_b=f"b{i}")
        log = get_match_log(db, limit=3)
        assert len(log) == 3

    def test_filters_by_match_type(self, db: Database) -> None:
        _create_test_match(db, stid_a="d1", stid_b="d2")  # dedup
        mid = _make_match_id()
        create_match_decision(
            db,
            match_id=mid,
            source_transaction_id_a="t1",
            source_type_a="csv",
            source_origin_a="bank",
            source_transaction_id_b="t2",
            source_type_b="ofx",
            source_origin_b="bank",
            account_id="acct1",
            confidence_score=0.95,
            match_signals={},
            match_tier=None,
            match_status="accepted",
            decided_by="auto",
            match_type="transfer",
            account_id_b="acct2",
        )
        transfer_log = get_match_log(db, match_type="transfer")
        assert len(transfer_log) == 1
        assert transfer_log[0]["match_type"] == "transfer"
