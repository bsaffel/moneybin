"""Unit tests for MatchingService accept/reject transition logic."""

from __future__ import annotations

import pytest

from moneybin import error_codes
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.matching.application import MatchApplicationEffects, MatchStatusChange
from moneybin.matching.persistence import (
    MatchStatus,
    get_match_decision,
)
from moneybin.repositories.match_decisions_repo import MatchDecisionsRepo
from moneybin.services.audit_service import AuditService
from moneybin.services.matching_service import MatchingService
from tests.moneybin.test_mcp.schema_assertions import (
    assert_recovery_actions_executable,
)


def _seed(
    db: Database,
    match_id: str,
    status: MatchStatus,
    *,
    match_type: str = "dedup",
) -> None:
    MatchDecisionsRepo(db).insert(
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
        match_type=match_type,
        account_id_b="acct2" if match_type == "transfer" else None,
        match_status=status,
        decided_by="matcher",
        actor="system",
    )


def _status_of(db: Database, match_id: str) -> str:
    row = get_match_decision(db, match_id)
    assert row is not None
    return row["match_status"]


def _retirement_count() -> float:
    """Read the committed retirement counter for the matching reconciliation."""
    from moneybin.metrics.registry import TRANSFER_RETIREMENTS_TOTAL

    counter = TRANSFER_RETIREMENTS_TOTAL.labels(cause="dedup_component")
    return counter._value.get()  # noqa: SLF001  # pyright: ignore[reportPrivateUsage]  # no public counter getter


def _seed_transfer_collision_fixture(db: Database) -> tuple[str, str, str]:
    """Seed two standing transfers and the pending dedup edge that collides them."""
    keep_id = "tx-keep"
    drop_id = "tx-drop"
    dedup_id = "dedup-edge"
    repo = MatchDecisionsRepo(db)
    for match_id, source_a, source_b, source_type_a, account_id_b, status in (
        (keep_id, "ofx-p", "ofx-x", "ofx", "brokerage", "accepted"),
        (drop_id, "csv-c", "ofx-y", "csv", "savings", "accepted"),
    ):
        repo.insert(
            match_id=match_id,
            source_transaction_id_a=source_a,
            source_type_a=source_type_a,
            source_origin_a="origin-a",
            source_transaction_id_b=source_b,
            source_type_b="ofx",
            source_origin_b="origin-b",
            account_id="checking",
            account_id_b=account_id_b,
            confidence_score=0.9,
            match_signals={},
            match_tier=None,
            match_type="transfer",
            match_status=status,
            decided_by="matcher",
            actor="test",
        )
    repo.insert(
        match_id=dedup_id,
        source_transaction_id_a="ofx-p",
        source_type_a="ofx",
        source_origin_a="origin-a",
        source_transaction_id_b="csv-c",
        source_type_b="csv",
        source_origin_b="origin-b",
        account_id="checking",
        confidence_score=0.9,
        match_signals={},
        match_tier="3",
        match_status="pending",
        decided_by="matcher",
        actor="test",
    )
    return keep_id, drop_id, dedup_id


def _audit_actions(db: Database, match_id: str) -> list[str]:
    """Return the audited mutations for one decision, newest first."""
    return [
        event.action
        for event in AuditService(db).list_events(
            target_table="match_decisions", target_id=match_id, limit=None
        )
    ]


def test_set_status_accepts_pending(db: Database) -> None:
    _seed(db, "m1", "pending")
    MatchingService(db).set_status("m1", status="accepted")
    assert _status_of(db, "m1") == "accepted"


def test_accepting_transfer_restates_fx_accounting(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(db, "fx-transfer", "pending", match_type="transfer")
    restated: list[Database] = []

    def record_restatement(target_db: Database, **_kwargs: object) -> None:
        restated.append(target_db)

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
        record_restatement,
    )

    MatchingService(db).set_status("fx-transfer", status="accepted")

    assert restated == [db]


def test_rejecting_transfer_does_not_restate_fx_accounting(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(db, "rejected-fx-transfer", "pending", match_type="transfer")
    restated: list[Database] = []

    def record_restatement(target_db: Database, **_kwargs: object) -> None:
        restated.append(target_db)

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
        record_restatement,
    )

    MatchingService(db).set_status("rejected-fx-transfer", status="rejected")

    assert restated == []


def test_reversing_transfer_restates_fx_accounting_as_committed_undo(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(db, "reversed-fx-transfer", "accepted", match_type="transfer")
    restated: list[tuple[Database, str]] = []

    def record_restatement(
        target_db: Database, *, committed_change: str = "setting"
    ) -> None:
        restated.append((target_db, committed_change))

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
        record_restatement,
    )

    MatchingService(db).undo("reversed-fx-transfer")

    assert restated == [(db, "undo")]


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


async def test_set_status_unknown_id_raises_not_found_with_recovery(
    db: Database,
) -> None:
    with pytest.raises(UserError) as exc:
        MatchingService(db).set_status("missing", status="accepted")
    err = exc.value
    assert err.code == error_codes.MUTATION_NOT_FOUND
    assert err.recovery_actions
    await assert_recovery_actions_executable(err.recovery_actions)
    assert (err.recovery_actions[0].tool, err.recovery_actions[0].arguments) == (
        "reviews",
        {"kind": "matches", "status": "pending"},
    )


async def test_reject_accepted_raises_constraint_with_undo_recovery(
    db: Database,
) -> None:
    _seed(db, "m4", "pending")
    insert_operation_id = (
        AuditService(db)
        .list_events(
            target_table="match_decisions",
            target_id="m4",
            limit=1,
        )[0]
        .operation_id
    )
    MatchingService(db).set_status("m4", status="accepted")
    acceptance_operation_id = (
        AuditService(db)
        .list_events(
            target_table="match_decisions",
            target_id="m4",
            limit=1,
        )[0]
        .operation_id
    )
    assert acceptance_operation_id != insert_operation_id

    with pytest.raises(UserError) as exc:
        MatchingService(db).set_status("m4", status="rejected")
    err = exc.value
    assert err.code == error_codes.MUTATION_CONSTRAINT_VIOLATION
    assert err.recovery_actions
    action = err.recovery_actions[0]
    await assert_recovery_actions_executable(err.recovery_actions)
    assert action.tool == "system_audit_undo"
    assert action.arguments == {"operation_id": acceptance_operation_id}


async def test_set_rejected_match_recovery_points_at_history(db: Database) -> None:
    # A rejected row isn't in the pending queue; recovery must point at history.
    _seed(db, "m4b", "rejected")
    with pytest.raises(UserError) as exc:
        MatchingService(db).set_status("m4b", status="accepted")
    err = exc.value
    assert err.recovery_actions
    await assert_recovery_actions_executable(err.recovery_actions)
    assert (err.recovery_actions[0].tool, err.recovery_actions[0].arguments) == (
        "reviews",
        {"kind": "matches", "status": "history"},
    )


async def test_set_reversed_match_recovery_points_at_history(db: Database) -> None:
    # The other terminal non-pending status: reversed routes to history too.
    _seed(db, "m4c", "reversed")
    with pytest.raises(UserError) as exc:
        MatchingService(db).set_status("m4c", status="accepted")
    err = exc.value
    assert err.recovery_actions
    await assert_recovery_actions_executable(err.recovery_actions)
    assert (err.recovery_actions[0].tool, err.recovery_actions[0].arguments) == (
        "reviews",
        {"kind": "matches", "status": "history"},
    )


def test_invalid_status_value_raises(db: Database) -> None:
    _seed(db, "m5", "pending")
    with pytest.raises(UserError) as exc:
        MatchingService(db).set_status("m5", status="bogus")
    assert exc.value.code == error_codes.MUTATION_INVALID_INPUT


def test_set_status_to_pending_is_invalid_input(db: Database) -> None:
    # "pending" is a real status but not user-settable (only accept/reject are);
    # an agent trying to "un-decide" a match hits the invalid-input guard.
    _seed(db, "m5b", "accepted")
    with pytest.raises(UserError) as exc:
        MatchingService(db).set_status("m5b", status="pending")
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
    outcome = MatchingService(db).accept_all_pending()
    assert outcome.accepted == 2
    # No transfer decisions in this fixture, so the reconciliation the bulk
    # accept now runs has nothing to invalidate and nothing of its own to undo.
    assert outcome.transfers_retired == 0
    assert outcome.reversed_by_reconciliation == 0
    assert _status_of(db, "q1") == "accepted"
    assert MatchingService(db).get_pending() == []


def test_accept_all_pending_restates_when_it_accepts_a_transfer(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(db, "bulk-fx-transfer", "pending", match_type="transfer")
    restated: list[Database] = []

    def record_restatement(target_db: Database, **_kwargs: object) -> None:
        restated.append(target_db)

    monkeypatch.setattr(
        "moneybin.services.fx_accounting_refresh.restate_fx_accounting",
        record_restatement,
    )

    MatchingService(db).accept_all_pending(match_type="transfer")

    assert restated == [db]


def test_set_status_application_effects_match_persisted_outcome(db: Database) -> None:
    """A decision, its audited reversals, and the metric agree after commit."""
    keep_id, drop_id, dedup_id = _seed_transfer_collision_fixture(db)
    before = _retirement_count()

    outcome = MatchingService(db).set_status(dedup_id, status="accepted", actor="cli")

    assert outcome.match_status == "accepted"
    assert outcome.transfers_retired == 1
    assert {
        keep_id: _status_of(db, keep_id),
        drop_id: _status_of(db, drop_id),
        dedup_id: _status_of(db, dedup_id),
    } == {
        keep_id: "accepted",
        drop_id: "reversed",
        dedup_id: "accepted",
    }
    assert _audit_actions(db, dedup_id) == [
        "match_decision.update_status",
        "match_decision.insert",
    ]
    assert _audit_actions(db, drop_id) == [
        "match_decision.reverse",
        "match_decision.insert",
    ]
    assert _retirement_count() - before == 1


def test_accept_all_application_effects_match_persisted_outcome(db: Database) -> None:
    """Bulk acceptance reports only standing reversals and audits all effects."""
    keep_id, drop_id, dedup_id = _seed_transfer_collision_fixture(db)
    before = _retirement_count()

    outcome = MatchingService(db).accept_all_pending(actor="cli")

    assert outcome.accepted == 1
    assert outcome.reversed_by_reconciliation == 0
    assert outcome.transfers_retired == 1
    assert {
        keep_id: _status_of(db, keep_id),
        drop_id: _status_of(db, drop_id),
        dedup_id: _status_of(db, dedup_id),
    } == {
        keep_id: "accepted",
        drop_id: "reversed",
        dedup_id: "accepted",
    }
    assert _audit_actions(db, dedup_id) == [
        "match_decision.update_status",
        "match_decision.insert",
    ]
    assert _audit_actions(db, drop_id) == [
        "match_decision.reverse",
        "match_decision.insert",
    ]
    assert _retirement_count() - before == 1


def test_set_status_does_not_record_metrics_when_commit_fails(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed decision commit rolls back rows and emits no durable metric."""
    keep_id, drop_id, dedup_id = _seed_transfer_collision_fixture(db)
    before = _retirement_count()

    def _fail_commit() -> None:
        raise RuntimeError("commit failed")

    monkeypatch.setattr(db, "commit", _fail_commit)

    with pytest.raises(RuntimeError, match="commit failed"):
        MatchingService(db).set_status(dedup_id, status="accepted", actor="cli")

    assert {
        keep_id: _status_of(db, keep_id),
        drop_id: _status_of(db, drop_id),
        dedup_id: _status_of(db, dedup_id),
    } == {
        keep_id: "accepted",
        drop_id: "accepted",
        dedup_id: "pending",
    }
    assert _audit_actions(db, dedup_id) == ["match_decision.insert"]
    assert _audit_actions(db, drop_id) == ["match_decision.insert"]
    assert _retirement_count() == before


def test_accept_all_does_not_record_metrics_when_commit_fails(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed bulk commit rolls back rows and emits no durable metric."""
    keep_id, drop_id, dedup_id = _seed_transfer_collision_fixture(db)
    before = _retirement_count()

    def _fail_commit() -> None:
        raise RuntimeError("commit failed")

    monkeypatch.setattr(db, "commit", _fail_commit)

    with pytest.raises(RuntimeError, match="commit failed"):
        MatchingService(db).accept_all_pending(actor="cli")

    assert {
        keep_id: _status_of(db, keep_id),
        drop_id: _status_of(db, drop_id),
        dedup_id: _status_of(db, dedup_id),
    } == {
        keep_id: "accepted",
        drop_id: "accepted",
        dedup_id: "pending",
    }
    assert _audit_actions(db, dedup_id) == ["match_decision.insert"]
    assert _audit_actions(db, drop_id) == ["match_decision.insert"]
    assert _retirement_count() == before


def test_set_status_returns_the_application_effects_outcome(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The returned decision outcome is the finalized boundary effect."""
    _seed(db, "seam-single", "pending")
    effects = MatchApplicationEffects(
        changes=(
            MatchStatusChange(
                match_id="seam-single",
                requested_status="accepted",
                prior_status="pending",
                effective_status="reversed",
                changed=True,
            ),
        ),
        reconciliation_reversals=6,
    )

    class _Application:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def set_status(self, *_args: object, **_kwargs: object) -> None:
            pass

        def finalize(self) -> MatchApplicationEffects:
            return effects

    monkeypatch.setattr(
        "moneybin.services.matching_service.MatchDecisionApplication",
        _Application,
        raising=False,
    )

    outcome = MatchingService(db).set_status("seam-single", status="accepted")

    assert outcome.match_status == "reversed"
    assert outcome.transfers_retired == 5


def test_accept_all_returns_the_application_effects_outcome(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The bulk outcome is derived from the finalized boundary effect."""
    effects = MatchApplicationEffects(
        changes=(
            MatchStatusChange("one", "accepted", "pending", "accepted", True),
            MatchStatusChange("two", "accepted", "pending", "accepted", True),
            MatchStatusChange("three", "accepted", "pending", "reversed", True),
        ),
        reconciliation_reversals=5,
    )

    class _Application:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def accept_pending(self, *_args: object, **_kwargs: object) -> None:
            pass

        def finalize(self) -> MatchApplicationEffects:
            return effects

    monkeypatch.setattr(
        "moneybin.services.matching_service.MatchDecisionApplication",
        _Application,
        raising=False,
    )

    outcome = MatchingService(db).accept_all_pending()

    assert outcome.accepted == 2
    assert outcome.reversed_by_reconciliation == 1
    assert outcome.transfers_retired == 4


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


def _seed_dedup(
    db: Database,
    match_id: str,
    status: MatchStatus,
    *,
    stid_a: str,
    stype_a: str,
    stid_b: str,
    stype_b: str,
    account_id: str = "acct1",
) -> None:
    """Seed a dedup decision with explicit node IDs for component-key tests."""
    MatchDecisionsRepo(db).insert(
        match_id=match_id,
        source_transaction_id_a=stid_a,
        source_type_a=stype_a,
        source_origin_a="origin_a",
        source_transaction_id_b=stid_b,
        source_type_b=stype_b,
        source_origin_b="origin_b",
        account_id=account_id,
        confidence_score=0.9,
        match_signals={},
        match_tier="3",
        match_status=status,
        decided_by="matcher",
        match_type="dedup",
        actor="system",
    )


def test_count_pending_dedup_groups_respects_match_type_filter(db: Database) -> None:
    """The group count must honour the caller's match_type, not hardcode dedup.

    transactions_matches_pending(match_type="transfer") returns transfer rows;
    reporting the full dedup-queue group count alongside them is a
    self-contradictory payload. A transfer-scoped call has zero dedup groups.
    """
    # 3-copy dedup cluster on acct1: T1-T2-T3 (two pending edges, one component)
    _seed_dedup(
        db, "g_ab", "pending", stid_a="t1", stype_a="csv", stid_b="t2", stype_b="ofx"
    )
    _seed_dedup(
        db, "g_bc", "pending", stid_a="t2", stype_a="ofx", stid_b="t3", stype_b="tiller"
    )
    svc = MatchingService(db)
    # Unfiltered and dedup-scoped both see the one component.
    assert svc.count_pending_dedup_groups() == 1
    assert svc.count_pending_dedup_groups(match_type="dedup") == 1
    # Transfer-scoped: no dedup groups are in scope.
    assert svc.count_pending_dedup_groups(match_type="transfer") == 0


def test_get_pending_warns_when_component_node_absent(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The defensive comp_keys fallback must surface a log warning, not split silently.

    If a pending dedup row's side-A node is missing from the component map, the
    row falls back to its own match_id (its own cluster). That "should not
    happen" — emit a warning so it's observable if it ever does.
    """
    import logging

    _seed_dedup(
        db, "w_ab", "pending", stid_a="t1", stype_a="csv", stid_b="t2", stype_b="ofx"
    )
    svc = MatchingService(db)
    # Force the fallback: pretend the component map is empty.
    empty_map: dict[tuple[str, str, str], str] = {}
    monkeypatch.setattr(svc, "_compute_component_keys", lambda: empty_map)
    with caplog.at_level(logging.WARNING):
        pending = svc.get_pending(match_type="dedup")
    # Falls back to match_id and logs a warning naming the row.
    assert pending[0]["component_key"] == "w_ab"
    assert any(
        "w_ab" in r.message and r.levelno == logging.WARNING for r in caplog.records
    )


def test_get_pending_assigns_component_key(db: Database) -> None:
    """Pending dedup rows that share a component get the same component_key.

    Component: T1(csv,t1) — T2(ofx,t2) — T3(tiller,t3) on acct1.
    Edge m_ab: T1↔T2 (pending)
    Edge m_bc: T2↔T3 (pending)
    Both pending edges share a component; their component_key must match.
    Unrelated edge m_zz (different account) is its own component.

    component_key = account_id prefixed onto
    MIN(source_type||'|'||source_transaction_id) over the component's members —
    same account-prefixed rule as the prep fold's group_id.
    On acct1: members are ("csv","t1"), ("ofx","t2"), ("tiller","t3").
    Packed: "csv|t1", "ofx|t2", "tiller|t3" → MIN = "csv|t1" → "acct1|csv|t1".
    """
    # 3-copy dedup cluster on acct1: T1-T2-T3
    _seed_dedup(
        db,
        "m_ab",
        "pending",
        stid_a="t1",
        stype_a="csv",
        stid_b="t2",
        stype_b="ofx",
    )
    _seed_dedup(
        db,
        "m_bc",
        "pending",
        stid_a="t2",
        stype_a="ofx",
        stid_b="t3",
        stype_b="tiller",
    )
    # Unrelated pending edge on a different account
    _seed_dedup(
        db,
        "m_zz",
        "pending",
        stid_a="x1",
        stype_a="csv",
        stid_b="x2",
        stype_b="ofx",
        account_id="acct2",
    )

    pending = MatchingService(db).get_pending(match_type="dedup")
    keys = {p["match_id"]: p["component_key"] for p in pending}

    # m_ab and m_bc share a 3-copy component; their keys must be identical
    assert keys["m_ab"] == keys["m_bc"], (
        f"Expected m_ab and m_bc to share component_key, got {keys}"
    )
    # m_zz is on a different account — its own component
    assert keys["m_zz"] != keys["m_ab"], (
        f"Expected m_zz to have a different component_key from m_ab, got {keys}"
    )
    # Sanity: the shared key should be the MIN packed node key in the component.
    # Members of the acct1 component: "csv|t1", "ofx|t2", "tiller|t3";
    # account-prefixed → "acct1|csv|t1".
    assert keys["m_ab"] == "acct1|csv|t1"
