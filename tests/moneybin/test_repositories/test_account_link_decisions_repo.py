"""Tests for ``AccountLinkDecisionsRepo``.

Mirrors test_match_decisions_repo.py: the insert writes a pending proposal and a
paired ``app.audit_log`` row in one transaction, with ``match_signals`` stored as
JSON and decoded (not doubly-encoded) in the audit payload.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest
from prometheus_client import REGISTRY

from moneybin.database import Database
from moneybin.repositories.account_link_decisions_repo import (
    AccountLinkDecisionsRepo,
)


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
            {"repository": "account_link_decisions", "action": action},
        )
        or 0.0
    )


def _insert(repo: AccountLinkDecisionsRepo, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "decision_id": "dec00000001",
        "provisional_account_id": "acct_prov_1",
        "candidate_account_id": "acct_cand_1",
        "confidence_score": 0.72,
        "match_signals": {"signal": "institution_last4", "value": "4267"},
        "status": "pending",
        "decided_by": "auto",
        "actor": "system",
    }
    kwargs.update(overrides)
    return repo.insert(**kwargs)


def test_insert_writes_row_and_audit_row(db: Database) -> None:
    repo = AccountLinkDecisionsRepo(db)
    before_metric = _metric("account_link_decision.insert")

    event = _insert(repo)
    assert event.target_id == "dec00000001"

    row = db.conn.execute(
        "SELECT status, decided_by, provisional_account_id, candidate_account_id "
        "FROM app.account_link_decisions WHERE decision_id = ?",
        ["dec00000001"],
    ).fetchone()
    assert row == ("pending", "auto", "acct_prov_1", "acct_cand_1")

    audit = _audit_rows_for(db, "dec00000001")
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "account_link_decision.insert"
    assert (schema, table, target_id) == (
        "app",
        "account_link_decisions",
        "dec00000001",
    )
    assert before is None
    after_json = json.loads(after)
    assert after_json["status"] == "pending"
    # match_signals decodes to a nested object in the audit payload.
    assert after_json["match_signals"]["signal"] == "institution_last4"
    assert actor == "system"

    assert _metric("account_link_decision.insert") - before_metric == 1.0


@pytest.mark.parametrize("bad_value", ["bogus", "system"])
def test_insert_rejects_invalid_decided_by(db: Database, bad_value: str) -> None:
    """decided_by is constrained to this table's domain (auto/user) — not 'system'."""
    repo = AccountLinkDecisionsRepo(db)
    with pytest.raises(duckdb.ConstraintException):
        _insert(repo, decided_by=bad_value)


def test_rejects_invalid_reversed_by(db: Database) -> None:
    """reversed_by is domain-constrained (auto/user) even though reverse() lands later."""
    repo = AccountLinkDecisionsRepo(db)
    _insert(repo)
    with pytest.raises(duckdb.ConstraintException):
        db.conn.execute(  # noqa: S608  # test input, not executing user SQL
            "UPDATE app.account_link_decisions SET reversed_by = 'bogus' "
            "WHERE decision_id = 'dec00000001'"
        )


def test_insert_rolls_back_when_audit_raises(db: Database) -> None:
    audit = MagicMock()
    audit.record_audit_event.side_effect = RuntimeError("simulated audit failure")
    repo = AccountLinkDecisionsRepo(db, audit=audit)

    with pytest.raises(RuntimeError):
        _insert(repo, decision_id="ghost_decision")

    rows = db.conn.execute(
        "SELECT 1 FROM app.account_link_decisions WHERE decision_id = ?",
        ["ghost_decision"],
    ).fetchall()
    assert rows == []


# -- update_status --


def test_update_status_captures_before_and_after(db: Database) -> None:
    repo = AccountLinkDecisionsRepo(db)
    _insert(repo, status="pending")

    event = repo.update_status(
        "dec00000001", status="accepted", decided_by="user", actor="cli"
    )
    assert event.target_id == "dec00000001"

    row = db.conn.execute(
        "SELECT status, decided_by FROM app.account_link_decisions WHERE decision_id = ?",
        ["dec00000001"],
    ).fetchone()
    assert row == ("accepted", "user")

    upd = next(
        r
        for r in _audit_rows_for(db, "dec00000001")
        if r[0] == "account_link_decision.update_status"
    )
    assert json.loads(upd[4])["status"] == "pending"
    assert json.loads(upd[5])["status"] == "accepted"
    assert upd[6] == "cli"


def test_update_status_raises_for_missing_decision(db: Database) -> None:
    repo = AccountLinkDecisionsRepo(db)
    with pytest.raises(ValueError, match="not found"):
        repo.update_status("nope", status="accepted", decided_by="user", actor="cli")


# -- reverse --


def test_reverse_sets_reversed_fields_and_captures_before(db: Database) -> None:
    repo = AccountLinkDecisionsRepo(db)
    _insert(repo, status="accepted")

    event = repo.reverse("dec00000001", reversed_by="user", actor="cli")
    assert event.target_id == "dec00000001"

    row = db.conn.execute(
        "SELECT status, reversed_by, reversed_at IS NOT NULL "
        "FROM app.account_link_decisions WHERE decision_id = ?",
        ["dec00000001"],
    ).fetchone()
    assert row == ("reversed", "user", True)

    rev = next(
        r
        for r in _audit_rows_for(db, "dec00000001")
        if r[0] == "account_link_decision.reverse"
    )
    assert json.loads(rev[4])["status"] == "accepted"
    assert json.loads(rev[5])["status"] == "reversed"


def test_reverse_raises_for_missing_decision(db: Database) -> None:
    repo = AccountLinkDecisionsRepo(db)
    with pytest.raises(ValueError, match="not found"):
        repo.reverse("nope", reversed_by="user", actor="cli")


def test_reverse_raises_when_already_reversed(db: Database) -> None:
    """Re-reversing an already-reversed decision is rejected (audit integrity)."""
    repo = AccountLinkDecisionsRepo(db)
    _insert(repo, status="accepted")
    repo.reverse("dec00000001", reversed_by="user", actor="cli")
    with pytest.raises(ValueError, match="already reversed"):
        repo.reverse("dec00000001", reversed_by="user", actor="cli")


# -- list_pending --


def test_list_pending_returns_only_active_pending(db: Database) -> None:
    repo = AccountLinkDecisionsRepo(db)
    _insert(repo, decision_id="d_pending", status="pending")
    _insert(repo, decision_id="d_accepted", status="accepted")
    _insert(repo, decision_id="d_rejected", status="rejected")
    # Reverse a pending decision — it drops from the queue (reversed_at set, status=reversed)
    repo.reverse("d_pending", reversed_by="user", actor="cli")
    # A fresh pending decision should appear
    _insert(repo, decision_id="d_pending2", status="pending")

    result = repo.list_pending()
    ids = [r["decision_id"] for r in result]
    assert "d_pending2" in ids
    assert "d_pending" not in ids  # reversed
    assert "d_accepted" not in ids
    assert "d_rejected" not in ids


def test_list_pending_orders_by_provisional_then_decision_id(db: Database) -> None:
    repo = AccountLinkDecisionsRepo(db)
    _insert(
        repo,
        decision_id="dec_B1",
        provisional_account_id="acct_prov_B",
        status="pending",
    )
    _insert(
        repo,
        decision_id="dec_A2",
        provisional_account_id="acct_prov_A",
        status="pending",
    )
    _insert(
        repo,
        decision_id="dec_A1",
        provisional_account_id="acct_prov_A",
        status="pending",
    )

    result = repo.list_pending()
    ids = [r["decision_id"] for r in result]
    assert ids == ["dec_A1", "dec_A2", "dec_B1"]


def test_list_pending_decodes_match_signals(db: Database) -> None:
    """list_pending decodes JSON match_signals to nested objects (not doubly-encoded)."""
    repo = AccountLinkDecisionsRepo(db)
    _insert(repo, decision_id="dec_json", status="pending")

    result = repo.list_pending()
    assert len(result) == 1
    signals = result[0]["match_signals"]
    assert isinstance(signals, dict)
    assert signals["signal"] == "institution_last4"


# -- fetch_by_id --


def test_fetch_by_id_returns_decoded_row(db: Database) -> None:
    repo = AccountLinkDecisionsRepo(db)
    _insert(repo, decision_id="dec_fetch", status="pending")

    row = repo.fetch_by_id("dec_fetch")
    assert row is not None
    assert row["decision_id"] == "dec_fetch"
    assert row["provisional_account_id"] == "acct_prov_1"
    assert row["candidate_account_id"] == "acct_cand_1"
    # match_signals decodes to a nested object (not doubly-encoded).
    assert row["match_signals"]["signal"] == "institution_last4"


def test_fetch_by_id_returns_none_when_absent(db: Database) -> None:
    repo = AccountLinkDecisionsRepo(db)
    assert repo.fetch_by_id("nonexistent") is None


# -- history --


def test_history_includes_all_statuses(db: Database) -> None:
    """history() spans every status — unlike list_pending, which is pending-only."""
    repo = AccountLinkDecisionsRepo(db)
    _insert(repo, decision_id="h_pending", status="pending")
    _insert(repo, decision_id="h_accepted", status="accepted")
    _insert(repo, decision_id="h_rejected", status="rejected")

    ids = {r["decision_id"] for r in repo.history()}
    assert ids == {"h_pending", "h_accepted", "h_rejected"}


def test_history_respects_limit(db: Database) -> None:
    repo = AccountLinkDecisionsRepo(db)
    for i in range(3):
        _insert(repo, decision_id=f"h_{i}", status="pending")

    assert len(repo.history(limit=2)) == 2


def test_history_clamps_negative_limit(db: Database) -> None:
    """A negative limit must not reach DuckDB (LIMIT/OFFSET cannot be negative)."""
    repo = AccountLinkDecisionsRepo(db)
    _insert(repo, decision_id="d1", status="pending")

    # Must not raise duckdb.BinderException; clamps to an empty result.
    assert repo.history(limit=-1) == []


# -- missing-table resilience (CatalogException guard) --


def test_list_pending_returns_empty_when_table_absent(db: Database) -> None:
    """list_pending guards a missing table like count_pending/history do."""
    db.conn.execute("DROP TABLE app.account_link_decisions")
    repo = AccountLinkDecisionsRepo(db)

    assert repo.list_pending() == []


def test_fetch_by_id_returns_none_when_table_absent(db: Database) -> None:
    """fetch_by_id returns None (clean not-found) rather than raising on a fresh DB."""
    db.conn.execute("DROP TABLE app.account_link_decisions")
    repo = AccountLinkDecisionsRepo(db)

    assert repo.fetch_by_id("dec00000001") is None
