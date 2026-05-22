"""Unit tests for MatchingService accept/reject transition logic."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.matching.persistence import create_match_decision, get_match_decision
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


def _seed(db: Database, match_id: str, status: str) -> None:
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


def test_set_status_accepts_pending(db: Database) -> None:
    _seed(db, "m1", "pending")
    MatchingService(db).set_status("m1", status="accepted")
    assert get_match_decision(db, "m1")["match_status"] == "accepted"


def test_set_status_rejects_pending(db: Database) -> None:
    _seed(db, "m2", "pending")
    MatchingService(db).set_status("m2", status="rejected")
    assert get_match_decision(db, "m2")["match_status"] == "rejected"


def test_set_status_same_status_is_idempotent(db: Database) -> None:
    _seed(db, "m3", "accepted")
    MatchingService(db).set_status("m3", status="accepted")  # no error
    assert get_match_decision(db, "m3")["match_status"] == "accepted"


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
    assert action.tool == "transactions_matches_undo"
    assert action.arguments == {"match_id": "m4"}


def test_invalid_status_value_raises(db: Database) -> None:
    _seed(db, "m5", "pending")
    with pytest.raises(UserError) as exc:
        MatchingService(db).set_status("m5", status="bogus")
    assert exc.value.code == error_codes.MUTATION_INVALID_INPUT
