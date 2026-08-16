"""Tests for the transaction-local match-decision application boundary."""

from __future__ import annotations

import pytest

from moneybin.database import Database
from moneybin.matching.application import (
    MatchApplicationEffects,
    MatchDecisionApplication,
    MatchDecisionNotFoundError,
    MatchDecisionStateError,
    MatchStatusChange,
    record_committed_match_effects,
)
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo


def _seed_match(
    db: Database,
    *,
    match_id: str,
    status: str,
    match_type: str = "dedup",
) -> None:
    """Seed one audited decision with the minimum valid match shape."""
    MatchDecisionsRepo(db).insert(
        match_id=match_id,
        source_transaction_id_a=f"{match_id}-a",
        source_type_a="csv",
        source_origin_a="checking",
        source_transaction_id_b=f"{match_id}-b",
        source_type_b="ofx",
        source_origin_b="checking",
        account_id="account-1",
        confidence_score=0.9,
        match_signals={},
        match_type=match_type,
        match_tier="3" if match_type == "dedup" else None,
        match_status=status,
        decided_by="matcher",
        actor="test",
    )


def test_pending_accept_records_prior_and_effective_status(db: Database) -> None:
    """A pending acceptance records both the request and committed status."""
    _seed_match(db, match_id="pending-1", status="pending")
    db.begin()
    app = MatchDecisionApplication(db, decisions=MatchDecisionsRepo(db), actor="test")
    app.set_status("pending-1", status="accepted")
    effects = app.finalize()
    db.rollback()

    assert effects.changes == (
        MatchStatusChange(
            match_id="pending-1",
            requested_status="accepted",
            prior_status="pending",
            effective_status="accepted",
            changed=True,
        ),
    )
    assert effects.reconciliation_ran is True


def test_idempotent_status_records_no_change_or_reconciliation(db: Database) -> None:
    """Repeating an existing status makes no write or reconciliation pass."""
    _seed_match(db, match_id="accepted-1", status="accepted")
    db.begin()
    app = MatchDecisionApplication(db, decisions=MatchDecisionsRepo(db), actor="test")
    app.set_status("accepted-1", status="accepted")
    effects = app.finalize()
    db.rollback()

    assert effects.changes[0].changed is False
    assert effects.reconciliation_ran is False
    assert effects.standing_transfers_retired == 0


def test_finalize_distinguishes_immediate_from_standing_reversals(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A newly accepted transfer reversed in the pass is not standing retirement."""
    _seed_match(db, match_id="new-transfer", status="pending", match_type="transfer")
    repo = MatchDecisionsRepo(db)

    def _reverse_new_match(*args: object, **kwargs: object) -> int:
        repo.reverse(
            "new-transfer",
            reversed_by="system",
            actor="test",
            in_outer_txn=True,
        )
        return 2

    monkeypatch.setattr(
        "moneybin.matching.application.retire_transfers_invalidated_by_dedup",
        _reverse_new_match,
    )
    db.begin()
    app = MatchDecisionApplication(db, decisions=repo, actor="test")
    app.set_status("new-transfer", status="accepted")
    effects = app.finalize()
    db.rollback()

    assert effects.reconciliation_reversals == 2
    assert effects.immediate_reversals == 1
    assert effects.standing_transfers_retired == 1
    assert effects.effective_statuses == {"new-transfer": "reversed"}


def test_missing_id_carries_match_id(db: Database) -> None:
    """Unknown ids are distinguished from invalid decision transitions."""
    app = MatchDecisionApplication(db, decisions=MatchDecisionsRepo(db), actor="test")

    with pytest.raises(MatchDecisionNotFoundError) as exc:
        app.set_status("missing", status="accepted")

    assert exc.value.match_id == "missing"


def test_terminal_transition_carries_current_and_requested_statuses(
    db: Database,
) -> None:
    """A terminal decision cannot silently be changed to another terminal state."""
    _seed_match(db, match_id="accepted-2", status="accepted")
    app = MatchDecisionApplication(db, decisions=MatchDecisionsRepo(db), actor="test")

    with pytest.raises(MatchDecisionStateError) as exc:
        app.set_status("accepted-2", status="rejected")

    assert exc.value.match_id == "accepted-2"
    assert exc.value.current_status == "accepted"
    assert exc.value.requested_status == "rejected"


def test_duplicate_request_is_rejected_before_a_second_audit_write(
    db: Database,
) -> None:
    """One application cannot double-audit one requested decision."""
    _seed_match(db, match_id="pending-2", status="pending")
    db.begin()
    app = MatchDecisionApplication(db, decisions=MatchDecisionsRepo(db), actor="test")
    app.set_status("pending-2", status="accepted")

    with pytest.raises(ValueError, match="appears more than once"):
        app.set_status("pending-2", status="accepted")

    audit_count = db.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE target_id = ?",
        ["pending-2"],
    ).fetchone()
    db.rollback()
    assert audit_count == (2,)


def test_application_cannot_be_used_after_finalization(db: Database) -> None:
    """Finalization closes the transaction-local application boundary."""
    _seed_match(db, match_id="pending-3", status="pending")
    db.begin()
    app = MatchDecisionApplication(db, decisions=MatchDecisionsRepo(db), actor="test")
    app.set_status("pending-3", status="rejected")
    app.finalize()

    with pytest.raises(RuntimeError, match="finalized"):
        app.set_status("pending-3", status="rejected")
    with pytest.raises(RuntimeError, match="finalized"):
        app.finalize()
    db.rollback()


def test_accept_pending_records_repo_ids_and_reconciles_once(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Bulk mode preserves its repository-selected ids and runs one pass."""
    _seed_match(db, match_id="dedup-a", status="pending")
    _seed_match(db, match_id="dedup-b", status="pending")
    _seed_match(db, match_id="transfer-a", status="pending", match_type="transfer")
    reconciliation_calls = 0

    def _reconcile(*args: object, **kwargs: object) -> int:
        nonlocal reconciliation_calls
        reconciliation_calls += 1
        return 0

    monkeypatch.setattr(
        "moneybin.matching.application.retire_transfers_invalidated_by_dedup",
        _reconcile,
    )
    db.begin()
    app = MatchDecisionApplication(db, decisions=MatchDecisionsRepo(db), actor="test")
    app.accept_pending(match_type="dedup")
    effects = app.finalize()
    db.rollback()

    assert [change.match_id for change in effects.changes] == ["dedup-a", "dedup-b"]
    assert all(change.prior_status == "pending" for change in effects.changes)
    assert all(change.effective_status == "accepted" for change in effects.changes)
    assert reconciliation_calls == 1


def test_bulk_and_explicit_requests_cannot_overlap(db: Database) -> None:
    """Mixing modes would make the request set ambiguous, so it is rejected."""
    _seed_match(db, match_id="pending-4", status="pending")
    db.begin()
    app = MatchDecisionApplication(db, decisions=MatchDecisionsRepo(db), actor="test")
    app.set_status("pending-4", status="rejected")

    with pytest.raises(ValueError, match="explicit"):
        app.accept_pending()
    db.rollback()


def test_metric_failure_does_not_escape(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Committed metrics are best-effort and never expose backend messages."""
    effects = MatchApplicationEffects(changes=(), reconciliation_reversals=1)

    def _fail(_: int) -> None:
        raise RuntimeError("metric backend unavailable")

    monkeypatch.setattr(
        "moneybin.matching.application.record_dedup_retirements",
        _fail,
    )
    record_committed_match_effects(effects)

    assert "committed matching metric" in caplog.text
    assert "metric backend unavailable" not in caplog.text
