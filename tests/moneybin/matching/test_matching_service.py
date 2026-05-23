"""Unit tests for MatchingService accept/reject transition logic."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.matching.persistence import (
    MatchStatus,
    create_match_decision,
    get_match_decision,
)
from moneybin.services.matching_service import MatchingService


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


def _seed(db: Database, match_id: str, status: MatchStatus) -> None:
    create_match_decision(
        db,
        match_id=match_id,
        source_transaction_id_a="a1",
        source_type_a="csv",
        source_origin_a="o1",
        source_transaction_id_b="b1",
        source_type_b="ofx",
        source_origin_b="o2",
        account_id="acct1",
        confidence_score=0.9,
        match_signals={},
        match_tier="3",
        match_status=status,
        decided_by="matcher",
    )


def _status_of(db: Database, match_id: str) -> str:
    row = get_match_decision(db, match_id)
    assert row is not None
    return row["match_status"]


def test_set_status_accepts_pending(db: Database) -> None:
    _seed(db, "m1", "pending")
    MatchingService(db).set_status("m1", status="accepted")
    assert _status_of(db, "m1") == "accepted"


def test_set_status_rejects_pending(db: Database) -> None:
    _seed(db, "m2", "pending")
    MatchingService(db).set_status("m2", status="rejected")
    assert _status_of(db, "m2") == "rejected"


def test_set_status_same_status_is_idempotent(db: Database) -> None:
    _seed(db, "m3", "accepted")
    MatchingService(db).set_status("m3", status="accepted")  # no error
    assert _status_of(db, "m3") == "accepted"


def test_set_status_rejected_to_rejected_is_idempotent(db: Database) -> None:
    _seed(db, "m3b", "rejected")
    MatchingService(db).set_status("m3b", status="rejected")  # no error
    assert _status_of(db, "m3b") == "rejected"


def test_set_status_unknown_id_raises_not_found_with_recovery(db: Database) -> None:
    with pytest.raises(UserError) as exc:
        MatchingService(db).set_status("missing", status="accepted")
    err = exc.value
    assert err.code == error_codes.MUTATION_NOT_FOUND
    assert err.recovery_actions
    assert err.recovery_actions[0].tool == "transactions_matches_pending"


def test_reject_accepted_raises_constraint_with_undo_recovery(db: Database) -> None:
    _seed(db, "m4", "accepted")
    with pytest.raises(UserError) as exc:
        MatchingService(db).set_status("m4", status="rejected")
    err = exc.value
    assert err.code == error_codes.MUTATION_CONSTRAINT_VIOLATION
    assert err.recovery_actions
    action = err.recovery_actions[0]
    # Recovery points at the audit-log undo (the coming M2D MCP tool), not a
    # phantom transactions_matches_undo; the CLI interim is named in rationale.
    assert action.tool == "system_audit_undo"
    assert "matches undo" in action.rationale


def test_set_rejected_match_recovery_points_at_history(db: Database) -> None:
    # A rejected row isn't in the pending queue; recovery must point at history.
    _seed(db, "m4b", "rejected")
    with pytest.raises(UserError) as exc:
        MatchingService(db).set_status("m4b", status="accepted")
    err = exc.value
    assert err.recovery_actions
    assert err.recovery_actions[0].tool == "transactions_matches_history"


def test_set_reversed_match_recovery_points_at_history(db: Database) -> None:
    # The other terminal non-pending status: reversed routes to history too.
    _seed(db, "m4c", "reversed")
    with pytest.raises(UserError) as exc:
        MatchingService(db).set_status("m4c", status="accepted")
    err = exc.value
    assert err.recovery_actions
    assert err.recovery_actions[0].tool == "transactions_matches_history"


def test_invalid_status_value_raises(db: Database) -> None:
    _seed(db, "m5", "pending")
    with pytest.raises(UserError) as exc:
        MatchingService(db).set_status("m5", status="bogus")
    assert exc.value.code == error_codes.MUTATION_INVALID_INPUT


def test_get_pending_returns_only_pending(db: Database) -> None:
    _seed(db, "p1", "pending")
    _seed(db, "p2", "accepted")
    pending = MatchingService(db).get_pending()
    ids = {row["match_id"] for row in pending}
    assert ids == {"p1"}


def test_accept_all_pending_accepts_and_counts(db: Database) -> None:
    _seed(db, "q1", "pending")
    _seed(db, "q2", "pending")
    _seed(db, "q3", "accepted")
    count = MatchingService(db).accept_all_pending()
    assert count == 2
    assert _status_of(db, "q1") == "accepted"
    assert MatchingService(db).get_pending() == []


def test_count_pending_filters_by_match_type(db: Database) -> None:
    _seed(db, "c1", "pending")  # _seed defaults match_type="dedup"
    _seed(db, "c2", "pending")
    _seed(db, "c3", "accepted")
    svc = MatchingService(db)
    assert svc.count_pending() == 2
    assert svc.count_pending(match_type="dedup") == 2
    assert svc.count_pending(match_type="transfer") == 0


def test_get_pending_limit_caps_rows_while_count_sees_all(db: Database) -> None:
    # Backs transactions_matches_pending's has_more: limit caps returned rows,
    # count_pending reports the true total so the envelope can flag has_more.
    for i in range(3):
        _seed(db, f"lim{i}", "pending")
    svc = MatchingService(db)
    assert len(svc.get_pending(limit=2)) == 2
    assert svc.count_pending() == 3
