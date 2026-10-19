"""Source coverage must retain evidence missing from financial projections."""

from __future__ import annotations

import importlib
import importlib.util
import json
from collections.abc import Collection
from datetime import timedelta
from typing import Any, Literal, cast
from uuid import UUID

import pytest

from moneybin.connectors.sync_models import (
    PullResult,
    ScopeOutcome,
    SimpleFINHistorySnapshot,
    SimpleFINSource,
    SimpleFINSourceResult,
    SyncAckV2Response,
    SyncJobDetails,
    SyncJobSource,
    UTCWindow,
)
from moneybin.database import Database
from moneybin.repositories.sync_job_state_repo import SyncJobStateRepo
from tests.moneybin.db_helpers import create_core_tables
from tests.moneybin.test_extractors.test_simplefin_extractor import BASE, load, source


def _assess(db: Database, account_ids: Collection[str] | None = None) -> Any:
    name = "moneybin.services.sync_completeness"
    assert importlib.util.find_spec(name) is not None, (
        "Local source completeness must assess Raw scopes and durable receipts"
    )
    return (
        importlib
        .import_module(name)
        .SyncCompletenessService(db)
        .assess(account_ids=account_ids)
    )


def _kinds(result: Any) -> set[str]:
    return {reason.kind for reason in result.reasons}


def _bind(
    db: Database,
    native_id: str = "acct",
    canonical_id: str = "canonical",
    *,
    account_type: str | None = "depository",
    include: bool = True,
) -> None:
    """Coverage consumes these accepted bindings; resolver behavior is separate."""
    create_core_tables(db)
    db.execute(
        """INSERT INTO app.account_links
            (link_id, account_id, ref_kind, ref_value, source_type, source_origin,
             status, decided_by, decided_at)
            SELECT DISTINCT ?, ?, 'source_native', account_id, 'simplefin',
                source_origin, 'accepted', 'user', CURRENT_TIMESTAMP
            FROM raw.simplefin_accounts WHERE account_id = ?""",
        [f"link-{canonical_id}", canonical_id, native_id],
    )
    db.execute(
        """INSERT INTO app.account_settings
            (account_id, account_type, account_subtype, include_in_net_worth)
            VALUES (?, ?, 'checking', ?)""",
        [canonical_id, account_type, include],
    )
    db.execute(
        """INSERT INTO core.dim_accounts
            (account_id, account_type, account_subtype, currency_code,
             source_type, display_name, include_in_net_worth)
            VALUES (?, ?, 'checking', 'USD', 'simplefin', 'Synthetic', ?)""",
        [canonical_id, account_type, include],
    )


def _history(day: int, *, pending: bool = False) -> SimpleFINHistorySnapshot:
    return SimpleFINHistorySnapshot.model_validate({
        "observed_at": BASE + timedelta(days=day, hours=1),
        "replay_required": False,
        "discovery_history_target": None,
        "accounts": [
            {
                "account_id": "acct",
                "native_scope": {
                    "kind": "simplefin_v2_connection",
                    "conn_id": "native-connection",
                },
                "acknowledged_intervals": [
                    {
                        "start": "2026-09-01T00:00:00Z",
                        "end": "2026-09-20T00:00:00Z",
                    }
                ],
                "history_target": "2026-07-01" if pending else None,
            }
        ],
    })


def _confirmed_job(
    db: Database,
    data: SimpleFINSource,
    *,
    job: int,
    day: int,
    confirmed_history: SimpleFINHistorySnapshot,
) -> None:
    """Persist the same pre-ack/confirmed-ack separation as sync ingestion."""
    load(db, data, job=job, day=day)
    native = SimpleFINSourceResult.model_validate(data.model_dump(exclude={"batches"}))
    confirmed = native.model_copy(update={"history": confirmed_history})
    SyncJobStateRepo(db).ensure(
        str(UUID(int=job)),
        "synthetic-profile",
        SyncJobDetails(
            sources=[
                SyncJobSource(
                    source=native, batch_ids=[batch.batch_id for batch in data.batches]
                )
            ],
            result=PullResult(
                job_id=str(UUID(int=job)),
                ack_state="confirmed",
                setup_state="complete",
                refresh_state="complete",
                transactions_loaded=0,
                accounts_loaded=0,
                balances_loaded=0,
                transactions_removed=0,
                institutions=[],
                source_results=[native],
            ),
            acknowledgement=SyncAckV2Response(
                job_id=UUID(int=job),
                contract_version=2,
                status="acked",
                acked_at=BASE + timedelta(days=day, hours=2),
                results=[confirmed],
            ),
        ),
    )


@pytest.mark.parametrize("delivery", ["missing", "wrong_window"])
def test_missing_delivery_cannot_clear_prior_validation_hold(
    db: Database, delivery: str
) -> None:
    first = source(account_changes={"transactions": {}})
    history = _history(1)
    first.outcomes = [
        ScopeOutcome(
            account_id="acct",
            native_scope=history.accounts[0].native_scope,
            window=first.batches[0].window,
            status="complete",
            error_code=None,
        )
    ]
    _confirmed_job(db, first, job=1, day=1, confirmed_history=history)
    _bind(db)
    assert "invalid_observation" in _kinds(_assess(db))

    missing = first.model_copy(deep=True)
    missing.batches[0].batch_id = UUID(int=2)
    if delivery == "missing":
        missing.batches[0].payload["accounts"] = []
    else:
        missing.batches[0].payload = source().batches[0].payload
        missing.batches[0].window = UTCWindow.model_validate({
            "start": "2026-08-01T00:00:00Z",
            "end": "2026-09-01T00:00:00Z",
        })
        missing.outcomes.append(
            first.outcomes[0].model_copy(
                update={"window": missing.batches[0].window}, deep=True
            )
        )
    _confirmed_job(db, missing, job=2, day=2, confirmed_history=_history(2))

    assert "invalid_observation" in _kinds(_assess(db))

    repaired = source(batch=3, day=3)
    repaired.outcomes = first.outcomes
    _confirmed_job(db, repaired, job=3, day=3, confirmed_history=_history(3))
    assert _assess(db).complete


@pytest.mark.parametrize("window", ["covering", "narrower", "disjoint"])
@pytest.mark.parametrize("generation", [1, 2])
def test_missing_delivery_requires_covering_repair(
    db: Database, window: str, generation: int
) -> None:
    missing = source()
    history = _history(1)
    missing.outcomes = [
        ScopeOutcome(
            account_id="acct",
            native_scope=history.accounts[0].native_scope,
            window=missing.batches[0].window,
            status="complete",
            error_code=None,
        )
    ]
    missing.batches[0].payload["accounts"] = []
    _confirmed_job(db, missing, job=1, day=1, confirmed_history=history)

    latest = source(batch=2, day=2)
    latest.credential_generation = generation
    if window != "covering":
        latest.batches[0].window = UTCWindow.model_validate({
            "start": "2026-09-10T00:00:00Z"
            if window == "narrower"
            else "2026-08-01T00:00:00Z",
            "end": "2026-09-20T00:00:00Z"
            if window == "narrower"
            else "2026-09-01T00:00:00Z",
        })
    latest.outcomes = [
        missing.outcomes[0].model_copy(update={"window": latest.batches[0].window})
    ]
    _confirmed_job(db, latest, job=2, day=2, confirmed_history=_history(2))
    _bind(db)

    assert ("retrieval_partial" in _kinds(_assess(db))) is (window != "covering")

    repaired = source(batch=3, day=3)
    repaired.credential_generation = generation
    repaired.outcomes = missing.outcomes
    _confirmed_job(db, repaired, job=3, day=3, confirmed_history=_history(3))
    assert _assess(db).complete


def test_unprojected_account_retains_unresolved_scope(db: Database) -> None:
    load(db, source())
    result = _assess(db)
    assert result.complete is False
    reason = next(r for r in result.reasons if r.kind == "unresolved_identity")
    assert reason.affected_account_ids == ()
    assert reason.affected_account_count == 0
    assert reason.unresolved_scope_count == 1


@pytest.mark.parametrize(
    ("changes", "account_type", "kind"),
    [
        ({}, None, "unresolved_account_type"),
        ({}, "investment", "unsupported_account_type"),
        (
            {"currency": "https://synthetic.invalid/currency"},
            "depository",
            "unsupported_currency",
        ),
        ({"balance": "100.001"}, "depository", "unsupported_precision"),
    ],
)
def test_held_monetary_scope_remains_incomplete(
    db: Database, changes: dict[str, str], account_type: str | None, kind: str
) -> None:
    load(db, source(account_changes=changes))
    _bind(db, account_type=account_type)
    result = _assess(db)
    assert result.complete is False
    reason = next(r for r in result.reasons if r.kind == kind)
    assert reason.affected_account_ids == ()
    assert reason.affected_account_count == 1


@pytest.mark.parametrize("status", ["failed", "deferred"])
def test_zero_batch_source_failure_survives_without_account(
    db: Database, status: Literal["failed", "deferred"]
) -> None:
    data = source()
    window = data.batches[0].window
    data.batches = []
    data.outcomes = [
        ScopeOutcome(
            account_id=None,
            native_scope=None,
            window=window,
            status=status,
            error_code="private-native-id https://credential.invalid/token",
        )
    ]
    load(db, data, status=status)
    result = _assess(db)
    assert result.complete is False
    reason = next(r for r in result.reasons if r.kind == f"retrieval_{status}")
    assert reason.unresolved_scope_count == 1
    assert "private-native" not in repr(result)
    assert "credential.invalid" not in repr(result)


def test_later_arriving_older_success_does_not_erase_failed_request(
    db: Database,
) -> None:
    failed = source(batch=2, day=2)
    failed.batches = []
    load(db, failed, job=2, day=2, status="failed")
    load(db, source(), job=1, day=1)
    assert "retrieval_failed" in _kinds(_assess(db))


def test_confirmed_history_supersedes_pre_ack_without_rewriting_raw(
    db: Database,
) -> None:
    data = source()
    data.history = _history(1, pending=True)
    _confirmed_job(db, data, job=1, day=1, confirmed_history=_history(2))
    _bind(db)
    result = _assess(db)
    assert not {"history_pending", "history_unknown"} & _kinds(result)
    row = db.execute(
        "SELECT source_metadata FROM raw.simplefin_sync_receipts"
    ).fetchone()
    assert row is not None
    assert (
        json.loads(row[0])["history"]["accounts"][0]["history_target"] == "2026-07-01"
    )


def test_history_observation_order_is_independent_of_arrival(db: Database) -> None:
    newer = source(batch=2, day=2)
    newer.history = _history(2, pending=True)
    _confirmed_job(db, newer, job=2, day=2, confirmed_history=_history(3))
    older = source()
    older.history = _history(1, pending=True)
    _confirmed_job(db, older, job=1, day=1, confirmed_history=_history(1, pending=True))
    _bind(db)
    assert "history_pending" not in _kinds(_assess(db))


@pytest.mark.parametrize(
    ("end", "complete"),
    [("2020-02-01", False), ("2026-09-19", False), ("2026-09-20", True)],
)
def test_report_requires_acknowledged_forward_coverage(
    db: Database, end: str, complete: bool
) -> None:
    data = source()
    data.history = _history(1, pending=True)
    data.outcomes = [
        ScopeOutcome(
            account_id="acct",
            native_scope=data.history.accounts[0].native_scope,
            window=data.batches[0].window,
            status="complete",
            error_code=None,
        )
    ]
    confirmed = _history(2)
    confirmed.accounts[0].acknowledged_intervals = [
        UTCWindow.model_validate({
            "start": "2020-01-01T00:00:00Z",
            "end": f"{end}T00:00:00Z",
        })
    ]
    _confirmed_job(db, data, job=1, day=1, confirmed_history=confirmed)
    _bind(db)

    result = _assess(db)

    assert result.complete is complete
    assert ("history_pending" in _kinds(result)) is (not complete)
    row = db.execute(
        "SELECT source_metadata FROM raw.simplefin_sync_receipts"
    ).fetchone()
    assert row is not None
    assert json.loads(row[0])["history"]["accounts"][0]["history_target"] == (
        "2026-07-01"
    )


def test_new_credential_generation_does_not_inherit_old_history(
    db: Database,
) -> None:
    old = source()
    _confirmed_job(db, old, job=1, day=1, confirmed_history=_history(1))
    replacement = source(batch=2, day=2)
    replacement.credential_generation = 2
    load(db, replacement, job=2, day=2)
    _bind(db)
    assert "history_unknown" in _kinds(_assess(db))


@pytest.mark.parametrize("generation", [1, 2])
@pytest.mark.parametrize("older_coverage", [False, True])
@pytest.mark.parametrize("latest_coverage", ["omitted", "other-native", "complete"])
def test_report_requires_latest_retrieval_for_each_history_scope(
    db: Database, generation: int, older_coverage: bool, latest_coverage: str
) -> None:
    old = source()
    old.history = _history(1)
    if older_coverage:
        old.outcomes = [
            ScopeOutcome(
                account_id="acct",
                native_scope=old.history.accounts[0].native_scope,
                window=old.batches[0].window,
                status="complete",
                error_code=None,
            )
        ]
    load(db, old)
    _bind(db)
    latest = source(batch=2, day=2)
    latest.credential_generation = generation
    confirmed = _history(2)
    if latest_coverage != "omitted":
        native_scope = confirmed.accounts[0].native_scope
        if latest_coverage == "other-native":
            native_scope = native_scope.model_copy(update={"conn_id": "other-native"})
        latest.outcomes = [
            ScopeOutcome(
                account_id="acct",
                native_scope=native_scope,
                window=latest.batches[0].window,
                status="complete",
                error_code=None,
            )
        ]
    _confirmed_job(db, latest, job=2, day=2, confirmed_history=confirmed)

    result = _assess(db)

    assert result.complete is (latest_coverage == "complete")
    if latest_coverage == "complete":
        assert not result.reasons
    else:
        assert _kinds(result) == {"evidence_unavailable", "retrieval_partial"}
        missing = next(
            reason for reason in result.reasons if reason.kind == "retrieval_partial"
        )
        assert missing.affected_account_ids == ()
        assert missing.affected_account_count == 1
        assert missing.unresolved_scope_count == (
            1 if latest_coverage == "other-native" else 0
        )
        reason = next(
            reason for reason in result.reasons if reason.kind == "evidence_unavailable"
        )
        assert reason.affected_account_ids == ()
        assert reason.affected_account_count == 1
        assert reason.unresolved_scope_count == 0


@pytest.mark.parametrize("policy", ["included", "excluded", "filtered", "unbound"])
def test_omitted_history_scope_respects_report_account_policy(
    db: Database, policy: str
) -> None:
    old = source(account_changes={"id": "omitted"})
    load(db, old)
    if policy != "unbound":
        _bind(db, "omitted", "omitted-canonical", include=policy != "excluded")
    latest = source(batch=2, day=2)
    history = _history(2)
    history.accounts.append(
        history.accounts[0].model_copy(update={"account_id": "omitted"})
    )
    latest.outcomes = [
        ScopeOutcome(
            account_id="acct",
            native_scope=history.accounts[0].native_scope,
            window=latest.batches[0].window,
            status="complete",
            error_code=None,
        )
    ]
    _confirmed_job(db, latest, job=2, day=2, confirmed_history=history)
    _bind(db)

    result = _assess(db, {"canonical"} if policy in {"filtered", "unbound"} else None)

    assert result.complete is (policy in {"excluded", "filtered"})
    if policy in {"included", "unbound"}:
        reason = next(r for r in result.reasons if r.kind == "evidence_unavailable")
        assert reason.affected_account_ids == ()
        assert reason.affected_account_count == (1 if policy == "included" else 0)
        assert reason.unresolved_scope_count == (1 if policy == "unbound" else 0)
    else:
        assert not result.reasons


def test_explicit_account_filter_omits_unrelated_resolved_holds(
    db: Database,
) -> None:
    load(db, source())
    _bind(db)
    load(
        db,
        source(batch=2, account_changes={"id": "other", "balance": "1.001"}),
        job=2,
    )
    _bind(db, "other", "unrelated")
    assert "unsupported_precision" in _kinds(_assess(db))
    assert "unsupported_precision" not in _kinds(_assess(db, {"canonical"}))


def test_explicit_exclusion_does_not_demand_account_classification(
    db: Database,
) -> None:
    load(db, source())
    _bind(db, account_type=None, include=False)
    assert "unresolved_account_type" not in _kinds(_assess(db))


def test_other_source_partial_job_does_not_taint_explicit_complete_source(
    db: Database,
) -> None:
    data = source()
    data.history = _history(2)
    data.outcomes = [
        ScopeOutcome(
            account_id="acct",
            native_scope=data.history.accounts[0].native_scope,
            window=data.batches[0].window,
            status="complete",
            error_code=None,
        )
    ]
    load(db, data, status="partial")
    _bind(db)

    result = _assess(db)

    assert result.complete is True
    assert not result.reasons


@pytest.mark.parametrize("filtered", [True, False])
def test_scoped_failure_respects_account_filter_and_explicit_exclusion(
    db: Database, filtered: bool
) -> None:
    data = source()
    data.batches.extend(source(batch=2, account_changes={"id": "excluded"}).batches)
    data.history = _history(2)
    data.history.accounts.append(
        data.history.accounts[0].model_copy(update={"account_id": "excluded"})
    )
    scoped_statuses: tuple[tuple[str, Literal["complete", "failed"]], ...] = (
        ("acct", "complete"),
        ("excluded", "failed"),
    )
    data.outcomes = [
        ScopeOutcome(
            account_id=native,
            native_scope=data.history.accounts[0].native_scope,
            window=data.batches[0].window,
            status=status,
            error_code=None,
        )
        for native, status in scoped_statuses
    ]
    load(db, data, status="partial")
    _bind(db)
    _bind(db, "excluded", "excluded-canonical", include=filtered)

    result = _assess(db, {"canonical"} if filtered else None)

    assert result.complete is True
    assert not result.reasons


@pytest.mark.parametrize("has_other_account", [False, True])
def test_missing_account_history_does_not_prove_observed_coverage(
    db: Database,
    has_other_account: bool,
) -> None:
    data = source()
    history = _history(2)
    history.accounts = (
        [history.accounts[0].model_copy(update={"account_id": "another-account"})]
        if has_other_account
        else []
    )
    _confirmed_job(db, data, job=1, day=1, confirmed_history=history)
    _bind(db)

    result = _assess(db)

    assert result.complete is False
    assert "history_unknown" in _kinds(result)


@pytest.mark.parametrize("selected", [None, {"canonical"}, {"other-source"}])
def test_linked_duplicate_coverage_requires_review(
    db: Database, selected: set[str] | None
) -> None:
    from moneybin.repositories.account_link_decisions_repo import (
        AccountLinkDecisionsRepo,
    )

    _confirmed_job(db, source(), job=1, day=1, confirmed_history=_history(2))
    _bind(db)
    db.execute("""INSERT INTO core.dim_accounts
        (account_id, account_type, account_subtype, currency_code, source_type)
        VALUES ('other-source', 'depository', 'checking', 'USD', 'ofx')""")
    AccountLinkDecisionsRepo(db).insert(
        decision_id="synthetic-link-review",
        provisional_account_id="canonical",
        candidate_account_id="other-source",
        confidence_score=0.9,
        match_signals={},
        decided_by="auto",
        actor="system",
    )

    result = _assess(db, selected)

    assert result.complete is False
    reason = next(
        reason for reason in result.reasons if reason.kind == "suspected_overlap"
    )
    assert reason.affected_account_ids == ()
    assert reason.affected_account_count == len(
        selected or {"canonical", "other-source"}
    )


def test_rejected_duplicate_proposal_is_not_an_account_exclusion(db: Database) -> None:
    from moneybin.repositories.account_link_decisions_repo import (
        AccountLinkDecisionsRepo,
    )

    load(db, source(account_changes={"balance": "100.001"}))
    _bind(db)
    AccountLinkDecisionsRepo(db).insert(
        decision_id="synthetic-link-rejected",
        provisional_account_id="canonical",
        candidate_account_id="other-source",
        confidence_score=0.9,
        match_signals={},
        decided_by="user",
        actor="user",
        status="rejected",
    )

    result = _assess(db)

    assert "unsupported_precision" in _kinds(result)
    assert "suspected_overlap" not in _kinds(result)


def _skipped_child_source(
    kind: str, *, batch: int = 2, day: int = 2
) -> SimpleFINSource:
    data = source(batch=batch, day=day)
    data.history = _history(day)
    payload = cast(dict[str, Any], data.batches[0].payload)
    accounts = cast(list[dict[str, Any]], payload["accounts"])
    account = accounts[0]
    transactions = cast(list[dict[str, Any]], account["transactions"])
    if kind == "invalid_transactions":
        account["transactions"] = "private-native malformed"
    elif kind == "missing_transaction_id":
        transactions[0].pop("id")
    elif kind == "duplicate_transaction_id":
        transactions.append(dict(transactions[0]))
    else:
        assert kind == "duplicate_account"
        accounts.append(dict(account))
    return data


@pytest.mark.parametrize(
    "kind",
    [
        "invalid_transactions",
        "missing_transaction_id",
        "duplicate_transaction_id",
        "duplicate_account",
    ],
)
def test_scoped_skipped_children_remain_incomplete_with_known_history(
    db: Database, kind: str
) -> None:
    load(db, source())
    _bind(db)
    load(db, _skipped_child_source(kind), job=2, day=2)
    assert db.execute(
        "SELECT count(*) FROM raw.simplefin_transactions WHERE batch_id = ?",
        [str(UUID(int=2))],
    ).fetchone() == (0,)

    result = _assess(db)

    assert result.complete is False
    reason = next(
        reason for reason in result.reasons if reason.kind == "invalid_observation"
    )
    assert reason.affected_account_ids == ()
    assert reason.affected_account_count == 1
    assert "private-native" not in repr(result)


@pytest.mark.parametrize("newer_kind", ["other_account", "different_window"])
def test_unrelated_newer_receipt_does_not_clear_scoped_local_hold(
    db: Database, newer_kind: str
) -> None:
    load(db, source())
    _bind(db)
    load(db, _skipped_child_source("invalid_transactions"), job=2, day=2)
    newer = source(batch=3, day=3)
    newer.history = _history(3)
    if newer_kind == "other_account":
        newer = source(batch=3, day=3, account_changes={"id": "other"})
        newer.history = _history(3)
    else:
        assert newer.batches[0].window is not None
        newer.batches[0].window = newer.batches[0].window.model_copy(
            update={
                "start": newer.batches[0].window.end,
                "end": newer.batches[0].window.end + timedelta(days=1),
            }
        )
    assert newer.history is not None
    newer.outcomes = [
        ScopeOutcome(
            account_id="other" if newer_kind == "other_account" else "acct",
            native_scope=newer.history.accounts[0].native_scope,
            window=newer.batches[0].window,
            status="complete",
            error_code=None,
        )
    ]
    load(db, newer, job=3, day=3)

    assert "invalid_observation" in _kinds(_assess(db, {"canonical"}))


@pytest.mark.parametrize("filtered", [True, False])
def test_scoped_local_hold_respects_filter_and_exclusion(
    db: Database, filtered: bool
) -> None:
    load(db, source())
    _bind(db, include=filtered)
    load(db, _skipped_child_source("invalid_transactions"), job=2, day=2)

    result = _assess(db, {"other-account"} if filtered else None)

    assert "invalid_observation" not in _kinds(result)


def test_newer_complete_covering_scope_resolves_local_hold(db: Database) -> None:
    load(db, source())
    _bind(db)
    load(db, _skipped_child_source("invalid_transactions"), job=2, day=2)
    assert "invalid_observation" in _kinds(_assess(db))
    repaired = source(batch=3, day=3)
    repaired.history = _history(3)
    repaired.outcomes = [
        ScopeOutcome(
            account_id="acct",
            native_scope=repaired.history.accounts[0].native_scope,
            window=repaired.batches[0].window,
            status="complete",
            error_code=None,
        )
    ]
    load(db, repaired, job=3, day=3)

    assert "invalid_observation" not in _kinds(_assess(db))


@pytest.mark.parametrize("generation", [1, 2], ids=["same-credentials", "replacement"])
@pytest.mark.parametrize(
    "later_scope", ["different-window", "covering-account", "covering-general"]
)
def test_unidentified_account_hold_survives_later_success(
    db: Database, generation: int, later_scope: str
) -> None:
    initial = source()
    initial.history = _history(1)
    initial.outcomes = [
        ScopeOutcome(
            account_id="acct",
            native_scope=initial.history.accounts[0].native_scope,
            window=initial.batches[0].window,
            status="complete",
            error_code=None,
        )
    ]
    load(db, initial)
    _bind(db)
    assert _assess(db).complete is True

    malformed = source(batch=2, day=2, account_changes={"id": None})
    malformed.history = _history(2)
    load(db, malformed, job=2, day=2)
    assert "invalid_observation" in _kinds(_assess(db))

    later = source(batch=3, day=3)
    later.credential_generation = generation
    later.history = _history(3)
    window = later.batches[0].window
    assert window is not None
    if later_scope == "different-window":
        window = window.model_copy(
            update={"start": window.end, "end": window.end + timedelta(days=1)}
        )
        later.batches[0].window = window
        account_history = later.history.accounts[0]
        account_history.acknowledged_intervals = [
            account_history.acknowledged_intervals[0].model_copy(
                update={"end": window.end}
            )
        ]
    general = later_scope == "covering-general"
    later.outcomes = [
        ScopeOutcome(
            account_id=None if general else "acct",
            native_scope=None if general else later.history.accounts[0].native_scope,
            window=window,
            status="complete",
            error_code=None,
        )
    ]
    load(db, later, job=3, day=3)

    result = _assess(db)

    assert result.complete is False
    assert _kinds(result) == (
        {"invalid_observation", "evidence_unavailable", "retrieval_partial"}
        if general
        else {"invalid_observation"}
    )
    reason = next(r for r in result.reasons if r.kind == "invalid_observation")
    assert reason.affected_account_ids == ()
    assert reason.affected_account_count == 0
    assert reason.unresolved_scope_count == 1


def test_accepted_legacy_native_target_is_not_disclosed_as_canonical(
    db: Database,
) -> None:
    legacy_target = "999999123456"
    load(db, source(account_changes={"balance": "100.001"}))
    _bind(db, canonical_id=legacy_target)

    result = _assess(db)

    assert result.complete is False
    reason = next(
        reason for reason in result.reasons if reason.kind == "unsupported_precision"
    )
    assert reason.affected_account_ids == ()
    assert reason.affected_account_count == 1
    assert legacy_target not in repr(result)


def test_replacement_generation_retains_local_hold_until_covering_fetch(
    db: Database,
) -> None:
    load(db, source())
    _bind(db)
    load(db, _skipped_child_source("invalid_transactions"), job=2, day=2)
    assert "invalid_observation" in _kinds(_assess(db))

    replacement = source(batch=3, day=3)
    replacement.credential_generation = 2
    replacement.history = _history(3)
    window = replacement.batches[0].window
    assert window is not None
    replacement.batches[0].window = window.model_copy(
        update={"start": window.end, "end": window.end + timedelta(days=1)}
    )
    replacement.outcomes = [
        ScopeOutcome(
            account_id="acct",
            native_scope=replacement.history.accounts[0].native_scope,
            window=replacement.batches[0].window,
            status="complete",
            error_code=None,
        )
    ]
    load(db, replacement, job=3, day=3)
    assert db.execute(
        "SELECT count(DISTINCT source_origin) FROM raw.simplefin_accounts"
    ).fetchone() == (1,)

    assert "invalid_observation" in _kinds(_assess(db, {"canonical"}))

    repaired = source(batch=4, day=4)
    repaired.credential_generation = 2
    repaired.history = _history(4)
    repaired.outcomes = [
        ScopeOutcome(
            account_id="acct",
            native_scope=repaired.history.accounts[0].native_scope,
            window=repaired.batches[0].window,
            status="complete",
            error_code=None,
        )
    ]
    load(db, repaired, job=4, day=4)

    assert "invalid_observation" not in _kinds(_assess(db, {"canonical"}))


@pytest.mark.parametrize(
    "recovery", ["covering", "short-window", "partial", "older", "skipped-child"]
)
def test_general_failure_requires_newer_covering_success(
    db: Database, recovery: str
) -> None:
    """A newer full response resolves transport failure, without erasing local holds."""
    load(db, source())
    _bind(db)
    failed = source(batch=2, day=2)
    failed.outcomes = [
        ScopeOutcome(
            account_id=None,
            native_scope=None,
            window=failed.batches[0].window,
            status="failed",
            error_code="upstream_unavailable",
        )
    ]
    failed.batches = []
    load(db, failed, job=2, day=2, status="failed")
    recovered = source(batch=3, day=3)
    recovered.history = _history(3)
    window = recovered.batches[0].window
    assert window is not None
    if recovery == "short-window":
        window = window.model_copy(update={"start": window.start + timedelta(days=1)})
        recovered.batches[0].window = window
    recovered.outcomes = [
        ScopeOutcome(
            account_id="acct",
            native_scope=recovered.history.accounts[0].native_scope,
            window=window,
            status="partial" if recovery == "partial" else "complete",
            error_code="provider_error" if recovery == "partial" else None,
        )
    ]
    if recovery == "skipped-child":
        accounts = cast(list[dict[str, Any]], recovered.batches[0].payload["accounts"])
        accounts[0]["transactions"] = "malformed child"
    _confirmed_job(
        db,
        recovered,
        job=3,
        day=1 if recovery == "older" else 3,
        confirmed_history=_history(3),
    )
    kinds = _kinds(_assess(db))
    if recovery in {"covering", "skipped-child"}:
        assert "retrieval_failed" not in kinds
    else:
        assert "retrieval_failed" in kinds
    if recovery == "skipped-child":
        assert "invalid_observation" in kinds
