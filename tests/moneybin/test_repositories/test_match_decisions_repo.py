"""Tests for ``MatchDecisionsRepo``.

Every mutating test asserts both the row mutation and the paired
``app.audit_log`` entry land in one transaction, and that ``before_value``
captures the FULL prior row (Req 4).
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo


def _audit_rows_for(db: Database, target_id: str) -> list[tuple[Any, ...]]:
    return db.conn.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor, parent_audit_id
          FROM app.audit_log
         WHERE target_id = ?
         ORDER BY occurred_at ASC, audit_id ASC
        """,
        [target_id],
    ).fetchall()


def _metric(action: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "moneybin_app_mutation_audit_emitted_total",
            {"repository": "match_decisions", "action": action},
        )
        or 0.0
    )


def _insert(repo: MatchDecisionsRepo, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "match_id": "m0000000001",
        "source_transaction_id_a": "a",
        "source_type_a": "csv",
        "source_origin_a": "chase",
        "source_transaction_id_b": "b",
        "source_type_b": "ofx",
        "source_origin_b": "chase",
        "account_id": "acct1",
        "confidence_score": 0.98,
        "match_signals": {"date_distance": 0, "description_similarity": 0.9},
        "match_tier": "3",
        "match_status": "pending",
        "decided_by": "auto",
        "actor": "system",
    }
    kwargs.update(overrides)
    return repo.insert(**kwargs)


def test_insert_writes_row_and_audit_row(db: Database) -> None:
    repo = MatchDecisionsRepo(db)
    before_metric = _metric("match_decision.insert")

    event = _insert(repo)
    assert event.target_id == "m0000000001"

    row = db.conn.execute(
        "SELECT match_status, decided_by, account_id "
        "FROM app.match_decisions WHERE match_id = ?",
        ["m0000000001"],
    ).fetchone()
    assert row == ("pending", "auto", "acct1")

    audit = _audit_rows_for(db, "m0000000001")
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "match_decision.insert"
    assert (schema, table, target_id) == ("app", "match_decisions", "m0000000001")
    assert before is None
    after_json = json.loads(after)
    assert after_json["match_status"] == "pending"
    # match_signals decodes to a nested object in the audit payload (not a
    # doubly-encoded string).
    assert after_json["match_signals"]["date_distance"] == 0
    assert actor == "system"

    assert _metric("match_decision.insert") - before_metric == 1.0


def test_insert_records_parent_audit_id(db: Database) -> None:
    repo = MatchDecisionsRepo(db)
    event = _insert(repo, parent_audit_id="p1")
    assert _audit_rows_for(db, event.target_id or "")[0][7] == "p1"


def test_insert_transfer_with_second_account(db: Database) -> None:
    repo = MatchDecisionsRepo(db)
    _insert(
        repo,
        match_id="m_transfer1",
        match_type="transfer",
        match_tier=None,
        account_id="acct1",
        account_id_b="acct2",
        match_status="accepted",
    )
    row = db.conn.execute(
        "SELECT match_type, account_id_b FROM app.match_decisions WHERE match_id = ?",
        ["m_transfer1"],
    ).fetchone()
    assert row == ("transfer", "acct2")


def test_update_status_captures_before_and_after(db: Database) -> None:
    repo = MatchDecisionsRepo(db)
    _insert(repo, match_status="pending")

    event = repo.update_status(
        "m0000000001", status="accepted", decided_by="user", actor="cli"
    )
    assert event.target_id == "m0000000001"

    row = db.conn.execute(
        "SELECT match_status, decided_by FROM app.match_decisions WHERE match_id = ?",
        ["m0000000001"],
    ).fetchone()
    assert row == ("accepted", "user")

    upd = next(
        r
        for r in _audit_rows_for(db, "m0000000001")
        if r[0] == "match_decision.update_status"
    )
    assert json.loads(upd[4])["match_status"] == "pending"
    assert json.loads(upd[5])["match_status"] == "accepted"
    assert upd[6] == "cli"


def test_update_status_raises_for_missing_match(db: Database) -> None:
    repo = MatchDecisionsRepo(db)
    with pytest.raises(ValueError, match="not found"):
        repo.update_status("nope", status="accepted", decided_by="user", actor="cli")


def test_reverse_sets_reversed_fields_and_captures_before(db: Database) -> None:
    repo = MatchDecisionsRepo(db)
    _insert(repo, match_status="accepted")

    event = repo.reverse("m0000000001", reversed_by="user", actor="cli")
    assert event.target_id == "m0000000001"

    row = db.conn.execute(
        "SELECT match_status, reversed_by, reversed_at IS NOT NULL "
        "FROM app.match_decisions WHERE match_id = ?",
        ["m0000000001"],
    ).fetchone()
    assert row == ("reversed", "user", True)

    rev = next(
        r
        for r in _audit_rows_for(db, "m0000000001")
        if r[0] == "match_decision.reverse"
    )
    assert json.loads(rev[4])["match_status"] == "accepted"
    assert json.loads(rev[5])["match_status"] == "reversed"


def test_reverse_raises_for_missing_match(db: Database) -> None:
    repo = MatchDecisionsRepo(db)
    with pytest.raises(ValueError, match="not found"):
        repo.reverse("nope", reversed_by="user", actor="cli")


def test_reverse_raises_when_already_reversed(db: Database) -> None:
    """Re-reversing an already-reversed match is rejected (audit integrity)."""
    repo = MatchDecisionsRepo(db)
    _insert(repo, match_status="accepted")
    repo.reverse("m0000000001", reversed_by="user", actor="cli")
    with pytest.raises(ValueError, match="accepted/rejected decisions can be reversed"):
        repo.reverse("m0000000001", reversed_by="user", actor="cli")


def test_reverse_raises_when_pending(db: Database) -> None:
    """A pending match has no accept/reject decision yet to undo.

    Reversing it would silently dequeue it from the pending queue with no
    decision ever recorded — the guarantee the match review queue exists to
    enforce.
    """
    repo = MatchDecisionsRepo(db)
    _insert(repo, match_status="pending")

    with pytest.raises(ValueError, match="accepted/rejected decisions can be reversed"):
        repo.reverse("m0000000001", reversed_by="user", actor="cli")

    row = db.conn.execute(
        "SELECT match_status, reversed_at FROM app.match_decisions WHERE match_id = ?",
        ["m0000000001"],
    ).fetchone()
    assert row == ("pending", None)


def test_insert_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = MatchDecisionsRepo(db, audit=audit)

    with pytest.raises(RuntimeError):
        _insert(repo, match_id="ghost_match")

    rows = db.conn.execute(
        "SELECT 1 FROM app.match_decisions WHERE match_id = ?", ["ghost_match"]
    ).fetchall()
    assert rows == []


def _statuses(db: Database) -> dict[str, str]:
    return {
        r[0]: r[1]
        for r in db.conn.execute(
            "SELECT match_id, match_status FROM app.match_decisions"
        ).fetchall()
    }


def test_accept_pending_filters_by_type_and_audits_each(db: Database) -> None:
    repo = MatchDecisionsRepo(db)
    _insert(repo, match_id="d1", match_status="pending")
    _insert(repo, match_id="d2", match_status="pending")
    _insert(
        repo,
        match_id="t1",
        match_status="pending",
        match_type="transfer",
        match_tier=None,
        account_id_b="acct2",
    )

    n = repo.accept_pending(match_type="dedup", decided_by="user", actor="cli")

    # count reflects exactly the matching rows; transfer is untouched.
    assert n == 2
    assert _statuses(db) == {"d1": "accepted", "d2": "accepted", "t1": "pending"}

    # each acceptance emitted its own audited update_status row (Invariant 10).
    updates = db.conn.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'match_decision.update_status'"
    ).fetchone()
    assert updates is not None and updates[0] == 2


def test_accept_pending_no_filter_accepts_all_pending(db: Database) -> None:
    repo = MatchDecisionsRepo(db)
    _insert(repo, match_id="p1", match_status="pending")
    _insert(repo, match_id="p2", match_status="pending")
    _insert(repo, match_id="r1", match_status="rejected")  # not pending — skipped

    n = repo.accept_pending(decided_by="user", actor="cli")

    assert n == 2
    assert _statuses(db) == {"p1": "accepted", "p2": "accepted", "r1": "rejected"}
