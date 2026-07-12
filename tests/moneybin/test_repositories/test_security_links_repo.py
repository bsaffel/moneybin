"""Security link + decision repos: uniqueness guard, repoint, lifecycle.

The decisions-repo tests mirror ``test_account_link_decisions_repo.py`` —
``reverse()``, ``list_pending()``, and ``history()`` are brief-mandated public
methods that otherwise ship with zero coverage.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import duckdb
import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
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
    """UserError, not ValueError — the guard is reachable from `system audit undo`."""
    repo = SecurityLinksRepo(db)
    _bind(repo)
    with pytest.raises(UserError, match="accepted binding already exists") as exc:
        _bind(repo, sid="cat000000002")
    assert exc.value.code == error_codes.MUTATION_CONSTRAINT_VIOLATION


def test_same_security_may_hold_many_refs(db: Database) -> None:
    repo = SecurityLinksRepo(db)
    _bind(repo, ref="sec_plaid_1")
    _bind(repo, ref="sec_plaid_2")  # N:1 churn re-bind — allowed
    row = db.execute(
        "SELECT COUNT(*) FROM app.security_links WHERE security_id = 'cat000000001'"
    ).fetchone()
    assert row is not None and row[0] == 2


# -- repoint --


def test_repoint_moves_binding_to_new_security(db: Database) -> None:
    repo = SecurityLinksRepo(db)
    event = _bind(repo)
    link_id = event.target_id
    assert link_id is not None

    repo.repoint(
        link_id=link_id, new_security_id="cat000000009", decided_by="user", actor="cli"
    )

    accepted = db.execute(
        "SELECT security_id FROM app.security_links "
        "WHERE ref_value = 'sec_plaid_1' AND status = 'accepted'"
    ).fetchone()
    assert accepted == ("cat000000009",)


def test_repoint_leaves_exactly_one_accepted_plus_reversed_history(
    db: Database,
) -> None:
    """_guard_uniqueness holds after repoint(s).

    Exactly one accepted row per ref, with reversed rows preserved as
    append-only binding history — the table's documented invariant
    (app_security_links.sql header comment).
    """
    repo = SecurityLinksRepo(db)
    event = _bind(repo, ref="sec_plaid_1", sid="cat000000001")
    link_id = event.target_id
    assert link_id is not None

    repo.repoint(
        link_id=link_id, new_security_id="cat000000002", decided_by="user", actor="cli"
    )
    accepted = db.execute(
        "SELECT link_id, security_id FROM app.security_links "
        "WHERE ref_value = 'sec_plaid_1' AND status = 'accepted'"
    ).fetchall()
    assert len(accepted) == 1
    assert accepted[0][1] == "cat000000002"
    new_link_id = accepted[0][0]

    # Repoint the new accepted row again — the invariant must still hold with
    # two reversed rows in the ref's history.
    repo.repoint(
        link_id=new_link_id,
        new_security_id="cat000000003",
        decided_by="user",
        actor="cli",
    )
    counts = dict(
        db.execute(
            "SELECT status, COUNT(*) FROM app.security_links "
            "WHERE ref_value = 'sec_plaid_1' GROUP BY status"
        ).fetchall()
    )
    assert counts == {"accepted": 1, "reversed": 2}


def test_repoint_records_caller_decided_by_not_auto(db: Database) -> None:
    """The new accepted row's decided_by reflects who repointed it.

    A user decision, not the original binding's 'auto' provenance.
    """
    repo = SecurityLinksRepo(db)
    event = _bind(repo)  # decided_by="auto"
    link_id = event.target_id
    assert link_id is not None

    repo.repoint(
        link_id=link_id, new_security_id="cat000000009", decided_by="user", actor="user"
    )

    new_row = db.execute(
        "SELECT decided_by FROM app.security_links "
        "WHERE ref_value = 'sec_plaid_1' AND status = 'accepted'"
    ).fetchone()
    assert new_row == ("user",)


def test_repoint_preserves_original_decided_at_on_reversed_row(db: Database) -> None:
    """The reversed row's original decided_at must survive as binding history.

    repoint's targeted UPDATE only touches reversed_at/reversed_by/status.
    """
    repo = SecurityLinksRepo(db)
    event = _bind(repo)
    link_id = event.target_id
    assert link_id is not None
    original = db.execute(
        "SELECT decided_at FROM app.security_links WHERE link_id = ?", [link_id]
    ).fetchone()

    repo.repoint(
        link_id=link_id, new_security_id="cat000000009", decided_by="user", actor="user"
    )

    after = db.execute(
        "SELECT decided_at FROM app.security_links WHERE link_id = ?", [link_id]
    ).fetchone()
    assert after == original


def test_repoint_emits_repoint_audit_action(db: Database) -> None:
    repo = SecurityLinksRepo(db)
    event = _bind(repo)
    link_id = event.target_id
    assert link_id is not None

    repo.repoint(
        link_id=link_id, new_security_id="cat000000009", decided_by="user", actor="user"
    )

    row = db.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'security_link.repoint'"
    ).fetchone()
    assert row is not None and row[0] == 1


def test_repoint_raises_when_link_not_found(db: Database) -> None:
    repo = SecurityLinksRepo(db)
    with pytest.raises(ValueError, match="not found"):
        repo.repoint(
            link_id="nope",
            new_security_id="cat000000009",
            decided_by="user",
            actor="user",
        )


def test_repoint_raises_when_already_pointing_to_target(db: Database) -> None:
    repo = SecurityLinksRepo(db)
    event = _bind(repo, sid="cat000000001")
    link_id = event.target_id
    assert link_id is not None
    with pytest.raises(ValueError, match="already points to"):
        repo.repoint(
            link_id=link_id,
            new_security_id="cat000000001",
            decided_by="user",
            actor="user",
        )


def test_repoint_raises_when_link_not_accepted(db: Database) -> None:
    repo = SecurityLinksRepo(db)
    event = _bind(repo)
    link_id = event.target_id
    assert link_id is not None
    repo.reverse(link_id=link_id, reversed_by="user", actor="user")
    with pytest.raises(ValueError, match="need accepted"):
        repo.repoint(
            link_id=link_id,
            new_security_id="cat000000009",
            decided_by="user",
            actor="user",
        )


def test_undo_reinsert_respects_uniqueness_guard(db: Database) -> None:
    """Undo-the-undo re-insert must not bypass the app-layer uniqueness guard.

    insert A -> undo (delete) -> insert B (same ref) -> undo-the-undo of A
    re-inserts A via BaseRepo._insert_row; without the restore-time guard in
    SecurityLinksRepo._insert_row, that leaves two accepted bindings for one
    provider ref. Mirrors AccountLinksRepo's precedent test.

    This is the GENUINE conflict — a ref another live link has since claimed —
    that survives the audit-ordering fix in ``repoint``: the intermediate state
    a merge's own replay transits is not a conflict, but this is.
    """
    repo = SecurityLinksRepo(db)
    ev_insert = _bind(repo, ref="sec_plaid_dup", sid="cat000000001")
    ev_undo = repo.undo_event(ev_insert, actor="system")  # deletes the binding
    assert ev_undo is not None
    _bind(repo, ref="sec_plaid_dup", sid="cat000000002")  # same ref — now allowed
    with pytest.raises(UserError, match="accepted binding already exists"):
        repo.undo_event(ev_undo, actor="system")  # re-insert must hit the guard


def test_reverse_frees_ref_for_new_binding(db: Database) -> None:
    repo = SecurityLinksRepo(db)
    event = _bind(repo)
    link_id = event.target_id
    assert link_id is not None
    repo.reverse(link_id=link_id, reversed_by="user", actor="user")
    _bind(repo, sid="cat000000002")  # ref is free again after reversal
    with pytest.raises(ValueError):
        repo.reverse(link_id=link_id, reversed_by="user", actor="user")  # double


# ============================================================================
# SecurityLinkDecisionsRepo
# ============================================================================


def _propose(repo: SecurityLinkDecisionsRepo, **overrides: Any):
    kwargs: dict[str, Any] = {
        "ref_kind": "plaid_security_id",
        "ref_value": "sec_plaid_9",
        "source_type": "plaid",
        "candidate_security_id": "cat000000001",
        "provider_ticker": "VTI",
        "provider_name": "Vanguard Total Stock Market ETF",
        "confidence_score": 0.87,
        "match_signals": {"signal": "fuzzy_name", "score": 0.87},
        "match_reason": "fuzzy_name",
        "actor": "system",
    }
    kwargs.update(overrides)
    return repo.insert(**kwargs)


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
        confidence_score=0.87,
        match_signals={"signal": "fuzzy_name", "score": 0.87},
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
    # confidence_score/match_signals were populated at insert (not left NULL) —
    # this is the only coverage of the JSON round-trip surviving update_status.
    # confidence_score is DECIMAL(5,4) — DuckDB returns a Decimal, not a float.
    assert after["confidence_score"] == Decimal("0.8700")
    assert after["match_signals"] == {"signal": "fuzzy_name", "score": 0.87}
    with pytest.raises(ValueError):  # only pending rows transition
        repo.update_status(
            decision_id, status="accepted", decided_by="user", actor="user"
        )


def test_insert_writes_row_and_audit_row(db: Database) -> None:
    event = _propose(SecurityLinkDecisionsRepo(db), decision_id="dec_insert_audit")
    assert event.target_id == "dec_insert_audit"

    row = db.conn.execute(
        "SELECT status, decided_by, candidate_security_id, ref_value "
        "FROM app.security_link_decisions WHERE decision_id = ?",
        ["dec_insert_audit"],
    ).fetchone()
    assert row == ("pending", "auto", "cat000000001", "sec_plaid_9")

    audit = _audit_rows_for(db, "dec_insert_audit")
    assert len(audit) == 1
    action, schema, table, target_id, before, after, actor, _parent = audit[0]
    assert action == "security_link_decision.insert"
    assert (schema, table, target_id) == (
        "app",
        "security_link_decisions",
        "dec_insert_audit",
    )
    assert before is None
    after_json = json.loads(after)
    assert after_json["status"] == "pending"
    # match_signals decodes to a nested object in the audit payload.
    assert after_json["match_signals"]["signal"] == "fuzzy_name"
    assert actor == "system"


def test_insert_records_parent_audit_id(db: Database) -> None:
    repo = SecurityLinkDecisionsRepo(db)
    event = _propose(repo, decision_id="dec_parent", parent_audit_id="p1")
    assert _audit_rows_for(db, event.target_id or "")[0][7] == "p1"


@pytest.mark.parametrize("bad_value", ["bogus", "system"])
def test_insert_rejects_invalid_decided_by(db: Database, bad_value: str) -> None:
    """decided_by is constrained to this table's domain (auto/user) — not 'system'."""
    repo = SecurityLinkDecisionsRepo(db)
    with pytest.raises(duckdb.ConstraintException):
        _propose(repo, decided_by=bad_value)


def test_rejects_invalid_reversed_by(db: Database) -> None:
    """reversed_by is domain-constrained (auto/user) even though reverse() lands later."""
    repo = SecurityLinkDecisionsRepo(db)
    _propose(repo, decision_id="dec_bad_reversed_by")
    with pytest.raises(duckdb.ConstraintException):
        db.conn.execute(  # noqa: S608  # test input, not executing user SQL
            "UPDATE app.security_link_decisions SET reversed_by = 'bogus' "
            "WHERE decision_id = 'dec_bad_reversed_by'"
        )


# -- fetch_by_id --


def test_fetch_by_id_returns_decoded_row(db: Database) -> None:
    repo = SecurityLinkDecisionsRepo(db)
    _propose(repo, decision_id="dec_fetch")

    row = repo.fetch_by_id("dec_fetch")
    assert row is not None
    assert row["decision_id"] == "dec_fetch"
    assert row["candidate_security_id"] == "cat000000001"
    # match_signals decodes to a nested object (not doubly-encoded).
    assert row["match_signals"]["signal"] == "fuzzy_name"


def test_fetch_by_id_returns_none_when_absent(db: Database) -> None:
    repo = SecurityLinkDecisionsRepo(db)
    assert repo.fetch_by_id("nonexistent") is None


# -- reverse --


def test_reverse_sets_reversed_fields_and_captures_before(db: Database) -> None:
    repo = SecurityLinkDecisionsRepo(db)
    _propose(repo, decision_id="dec_reverse")
    repo.update_status("dec_reverse", status="accepted", decided_by="user", actor="cli")

    event = repo.reverse("dec_reverse", reversed_by="user", actor="cli")
    assert event.target_id == "dec_reverse"

    row = db.conn.execute(
        "SELECT status, reversed_by, reversed_at IS NOT NULL "
        "FROM app.security_link_decisions WHERE decision_id = ?",
        ["dec_reverse"],
    ).fetchone()
    assert row == ("reversed", "user", True)

    rev = next(
        r
        for r in _audit_rows_for(db, "dec_reverse")
        if r[0] == "security_link_decision.reverse"
    )
    assert json.loads(rev[4])["status"] == "accepted"
    assert json.loads(rev[5])["status"] == "reversed"
    assert rev[6] == "cli"


def test_reverse_raises_for_missing_decision(db: Database) -> None:
    repo = SecurityLinkDecisionsRepo(db)
    with pytest.raises(ValueError, match="not found"):
        repo.reverse("nope", reversed_by="user", actor="cli")


def test_reverse_raises_when_already_reversed(db: Database) -> None:
    """Re-reversing an already-reversed decision is rejected (audit integrity)."""
    repo = SecurityLinkDecisionsRepo(db)
    _propose(repo, decision_id="dec_double")
    repo.update_status("dec_double", status="accepted", decided_by="user", actor="cli")
    repo.reverse("dec_double", reversed_by="user", actor="cli")
    with pytest.raises(ValueError, match="accepted/rejected decisions can be reversed"):
        repo.reverse("dec_double", reversed_by="user", actor="cli")


def test_reverse_raises_when_pending(db: Database) -> None:
    """A pending decision has no accept/reject decision yet to undo.

    Reversing it would silently dequeue a review item with no decision ever
    recorded — the guarantee the security-link review queue exists to enforce.
    """
    repo = SecurityLinkDecisionsRepo(db)
    _propose(repo, decision_id="dec_pending")

    with pytest.raises(ValueError, match="accepted/rejected decisions can be reversed"):
        repo.reverse("dec_pending", reversed_by="user", actor="cli")

    assert repo.count_pending() == 1
    still_pending = repo.fetch_by_id("dec_pending")
    assert still_pending is not None
    assert still_pending["status"] == "pending"
    assert still_pending["reversed_at"] is None


# -- list_pending --


def test_list_pending_returns_only_active_pending(db: Database) -> None:
    repo = SecurityLinkDecisionsRepo(db)
    _propose(repo, decision_id="d_pending", ref_value="sec_a")
    _propose(repo, decision_id="d_accepted", ref_value="sec_b")
    repo.update_status("d_accepted", status="accepted", decided_by="user", actor="cli")
    _propose(repo, decision_id="d_rejected", ref_value="sec_c")
    repo.update_status("d_rejected", status="rejected", decided_by="user", actor="cli")
    # A decided (rejected) decision may be reversed — reopening it does not
    # resurrect it in list_pending (status becomes 'reversed', not 'pending').
    repo.reverse("d_rejected", reversed_by="user", actor="cli")

    result = repo.list_pending()
    ids = [r["decision_id"] for r in result]
    assert "d_pending" in ids
    assert "d_accepted" not in ids
    assert "d_rejected" not in ids


def test_list_pending_orders_by_ref_value_then_decision_id(db: Database) -> None:
    repo = SecurityLinkDecisionsRepo(db)
    _propose(repo, decision_id="dec_B1", ref_value="sec_B")
    _propose(repo, decision_id="dec_A2", ref_value="sec_A")
    _propose(repo, decision_id="dec_A1", ref_value="sec_A")

    result = repo.list_pending()
    ids = [r["decision_id"] for r in result]
    assert ids == ["dec_A1", "dec_A2", "dec_B1"]


def test_list_pending_decodes_match_signals(db: Database) -> None:
    """list_pending decodes JSON match_signals to nested objects (not doubly-encoded)."""
    repo = SecurityLinkDecisionsRepo(db)
    _propose(repo, decision_id="dec_json")

    result = repo.list_pending()
    assert len(result) == 1
    signals = result[0]["match_signals"]
    assert isinstance(signals, dict)
    assert signals["signal"] == "fuzzy_name"


# -- history --


def test_history_includes_all_statuses(db: Database) -> None:
    """history() spans every status — unlike list_pending, which is pending-only."""
    repo = SecurityLinkDecisionsRepo(db)
    _propose(repo, decision_id="h_pending", ref_value="sec_h1")
    _propose(repo, decision_id="h_accepted", ref_value="sec_h2")
    repo.update_status("h_accepted", status="accepted", decided_by="user", actor="cli")
    _propose(repo, decision_id="h_rejected", ref_value="sec_h3")
    repo.update_status("h_rejected", status="rejected", decided_by="user", actor="cli")

    ids = {r["decision_id"] for r in repo.history()}
    assert ids == {"h_pending", "h_accepted", "h_rejected"}


def test_history_respects_limit(db: Database) -> None:
    repo = SecurityLinkDecisionsRepo(db)
    for i in range(3):
        _propose(repo, decision_id=f"h_{i}", ref_value=f"sec_h_{i}")

    assert len(repo.history(limit=2)) == 2


def test_history_orders_newest_first(db: Database) -> None:
    """history() orders by decided_at DESC (newest decision first).

    insert stamps decided_at (NOT NULL), so distinct timestamps are set
    explicitly here to pin ordering deterministically rather than relying on
    same-tick inserts.
    """
    repo = SecurityLinkDecisionsRepo(db)
    _propose(repo, decision_id="older", ref_value="sec_older")
    _propose(repo, decision_id="newer", ref_value="sec_newer")
    db.conn.execute(
        "UPDATE app.security_link_decisions SET decided_at = ? WHERE decision_id = ?",
        ["2026-01-01 00:00:00", "older"],
    )
    db.conn.execute(
        "UPDATE app.security_link_decisions SET decided_at = ? WHERE decision_id = ?",
        ["2026-06-01 00:00:00", "newer"],
    )

    ids = [r["decision_id"] for r in repo.history()]
    assert ids == ["newer", "older"]


def test_history_clamps_negative_limit(db: Database) -> None:
    """A negative limit must not reach DuckDB (LIMIT/OFFSET cannot be negative)."""
    repo = SecurityLinkDecisionsRepo(db)
    _propose(repo, decision_id="d1")

    # Must not raise duckdb.BinderException; clamps to an empty result.
    assert repo.history(limit=-1) == []
