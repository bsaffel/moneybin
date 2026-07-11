"""Security link + decision repos: uniqueness guard, rebind, lifecycle."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.repositories.security_link_decisions_repo import (
    SecurityLinkDecisionsRepo,
)
from moneybin.repositories.security_links_repo import SecurityLinksRepo


def _bind(repo: SecurityLinksRepo, ref: str = "sec_plaid_1", sid: str = "cat000000001"):
    return repo.insert(
        security_id=sid,
        ref_kind="plaid_security_id",
        ref_value=ref,
        source_type="plaid",
        decided_by="auto",
        actor="system",
    )


def test_second_accepted_binding_for_same_ref_rejected(db: Database) -> None:
    repo = SecurityLinksRepo(db)
    _bind(repo)
    with pytest.raises(ValueError, match="accepted binding already exists"):
        _bind(repo, sid="cat000000002")


def test_same_security_may_hold_many_refs(db: Database) -> None:
    repo = SecurityLinksRepo(db)
    _bind(repo, ref="sec_plaid_1")
    _bind(repo, ref="sec_plaid_2")  # N:1 churn re-bind — allowed
    row = db.execute(
        "SELECT COUNT(*) FROM app.security_links WHERE security_id = 'cat000000001'"
    ).fetchone()
    assert row is not None and row[0] == 2


def test_rebind_repoints_and_audits(db: Database) -> None:
    repo = SecurityLinksRepo(db)
    event = _bind(repo)
    link_id = event.target_id
    assert link_id is not None
    repo.rebind(link_id=link_id, new_security_id="cat000000009", actor="user")
    row = db.execute(
        "SELECT security_id, ref_kind, ref_value, source_type, decided_by "
        "FROM app.security_links WHERE link_id = ?",
        [link_id],
    ).fetchone()
    # Only security_id changes — rebind must not omit-and-null the sibling
    # columns its targeted UPDATE doesn't list.
    assert row == ("cat000000009", "plaid_security_id", "sec_plaid_1", "plaid", "auto")
    audit = db.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'security_link.rebind'"
    ).fetchone()
    assert audit is not None and audit[0] == 1


def test_undo_reinsert_respects_uniqueness_guard(db: Database) -> None:
    """Undo-the-undo re-insert must not bypass the app-layer uniqueness guard.

    insert A -> undo (delete) -> insert B (same ref) -> undo-the-undo of A
    re-inserts A via BaseRepo._insert_row; without the restore-time guard in
    SecurityLinksRepo._insert_row, that leaves two accepted bindings for one
    provider ref. Mirrors AccountLinksRepo's precedent test.
    """
    repo = SecurityLinksRepo(db)
    ev_insert = _bind(repo, ref="sec_plaid_dup", sid="cat000000001")
    ev_undo = repo.undo_event(ev_insert, actor="system")  # deletes the binding
    assert ev_undo is not None
    _bind(repo, ref="sec_plaid_dup", sid="cat000000002")  # same ref — now allowed
    with pytest.raises(ValueError, match="accepted binding already exists"):
        repo.undo_event(ev_undo, actor="system")  # re-insert must hit the guard


def test_reverse_then_rebindable(db: Database) -> None:
    repo = SecurityLinksRepo(db)
    event = _bind(repo)
    link_id = event.target_id
    assert link_id is not None
    repo.reverse(link_id=link_id, reversed_by="user", actor="user")
    _bind(repo, sid="cat000000002")  # ref is free again after reversal
    with pytest.raises(ValueError):
        repo.reverse(link_id=link_id, reversed_by="user", actor="user")  # double


def test_decision_lifecycle_and_pending_count(db: Database) -> None:
    repo = SecurityLinkDecisionsRepo(db)
    assert repo.count_pending() == 0
    event = repo.insert(
        ref_kind="plaid_security_id",
        ref_value="sec_plaid_9",
        source_type="plaid",
        candidate_security_id="cat000000001",
        provider_ticker="VTI",
        provider_name="Vanguard Total Stock Market ETF",
        match_reason="fuzzy_name",
        actor="system",
    )
    decision_id = event.target_id
    assert decision_id is not None
    assert repo.count_pending() == 1
    fetched = repo.fetch_by_id(decision_id)
    assert fetched is not None
    assert fetched["status"] == "pending"
    repo.update_status(decision_id, status="rejected", decided_by="user", actor="user")
    assert repo.count_pending() == 0
    # update_status's targeted UPDATE (status/decided_by/decided_at only) must
    # not omit-and-null the sibling columns it doesn't list.
    after = repo.fetch_by_id(decision_id)
    assert after is not None
    assert after["provider_ticker"] == "VTI"
    assert after["provider_name"] == "Vanguard Total Stock Market ETF"
    assert after["candidate_security_id"] == "cat000000001"
    assert after["match_reason"] == "fuzzy_name"
    with pytest.raises(ValueError):  # only pending rows transition
        repo.update_status(
            decision_id, status="accepted", decided_by="user", actor="user"
        )
