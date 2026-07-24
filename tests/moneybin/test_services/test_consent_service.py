"""Tests for ConsentService — backend resolution, validation, status."""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin.config import clear_settings_cache, set_current_profile
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.consent import ConsentMode
from moneybin.privacy.log import read_privacy_events
from moneybin.repositories.consent_repo import ConsentRepo
from moneybin.services.consent_service import ConsentService


def test_service_grant_resolves_default_backend(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MONEYBIN_AI__DEFAULT_BACKEND", "anthropic")
    clear_settings_cache()
    set_current_profile("test")
    svc = ConsentService(db)
    result = svc.grant_consent(
        feature_category="mcp-data-sharing",
        backend=None,  # falls back to default_backend
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli.privacy_grant",
    )
    assert result.created is True
    assert result.grant.backend == "anthropic"


def test_service_grant_idempotent_returns_not_created(db: Database) -> None:
    svc = ConsentService(db)
    first = svc.grant_consent(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli",
    )
    second = svc.grant_consent(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli",
    )
    assert first.created is True
    assert second.created is False
    assert second.grant.grant_id == first.grant.grant_id


def test_service_grant_requires_backend_when_no_default(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MONEYBIN_AI__DEFAULT_BACKEND", raising=False)
    clear_settings_cache()
    set_current_profile("test")
    svc = ConsentService(db)
    with pytest.raises(UserError):
        svc.grant_consent(
            feature_category="mcp-data-sharing",
            backend=None,
            consent_mode=ConsentMode.PERSISTENT,
            actor="cli.privacy_grant",
        )


def test_service_grant_rejects_empty_backend(db: Database) -> None:
    """An explicit empty/whitespace backend is invalid input, not a default request."""
    svc = ConsentService(db)
    for bad in ("", "   "):
        with pytest.raises(UserError):
            svc.grant_consent(
                feature_category="mcp-data-sharing",
                backend=bad,
                consent_mode=ConsentMode.PERSISTENT,
                actor="cli",
            )


def test_service_grant_strips_backend_whitespace(db: Database) -> None:
    """A padded backend is normalized so a later canonical-form revoke matches."""
    svc = ConsentService(db)
    result = svc.grant_consent(
        feature_category="mcp-data-sharing",
        backend="  anthropic  ",
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli",
    )
    assert result.grant.backend == "anthropic"
    # Revoke with the canonical form must find the grant stored from padded input.
    revoke = svc.revoke_consent(
        feature_category="mcp-data-sharing", backend="anthropic", actor="cli"
    )
    assert revoke.count == 1


def test_service_rejects_unknown_category(db: Database) -> None:
    svc = ConsentService(db)
    with pytest.raises(UserError):
        svc.grant_consent(
            feature_category="not-a-real-category",
            backend="anthropic",
            consent_mode=ConsentMode.PERSISTENT,
            actor="cli.privacy_grant",
        )


def test_service_status_lists_active(db: Database) -> None:
    svc = ConsentService(db)
    svc.grant_consent(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli",
    )
    status = svc.status()
    assert status.consent_policy == "standard"
    assert len(status.active_grants) == 1
    assert status.active_grants[0].feature_category == "mcp-data-sharing"


def test_service_revoke_then_status_empty(db: Database) -> None:
    svc = ConsentService(db)
    svc.grant_consent(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli",
    )
    result = svc.revoke_consent(
        feature_category="mcp-data-sharing", backend="anthropic", actor="cli"
    )
    assert result.count == 1
    assert result.backend == "anthropic"
    assert svc.status().active_grants == []


def test_service_revoke_reports_resolved_default_backend(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """revoke_consent(backend=None) reports the resolved backend, not a sentinel."""
    monkeypatch.setenv("MONEYBIN_AI__DEFAULT_BACKEND", "anthropic")
    clear_settings_cache()
    set_current_profile("test")
    svc = ConsentService(db)
    svc.grant_consent(
        feature_category="mcp-data-sharing",
        backend=None,
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli",
    )
    result = svc.revoke_consent(
        feature_category="mcp-data-sharing", backend=None, actor="cli"
    )
    assert result.count == 1
    assert result.backend == "anthropic"


def test_service_revoke_all(db: Database) -> None:
    svc = ConsentService(db)
    svc.grant_consent(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli",
    )
    svc.grant_consent(
        feature_category="ml-categorization",
        backend="openai",
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli",
    )
    assert svc.revoke_all(actor="cli") == 2
    assert svc.status().active_grants == []
    # Idempotent: a second revoke_all over an empty ledger is a no-op.
    assert svc.revoke_all(actor="cli") == 0


def test_service_revoke_all_emits_per_grant_privacy_events(
    db: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """revoke_all writes one privacy.log event per grant, not a single wildcard.

    The bulk-revoke must stay reconstructable from `privacy log` alone — a
    `*`/`*` event would lose which (category, backend) pairs were revoked.
    """
    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir", lambda: tmp_path
    )
    svc = ConsentService(db)
    svc.grant_consent(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli",
    )
    svc.grant_consent(
        feature_category="ml-categorization",
        backend="openai",
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli",
    )
    assert svc.revoke_all(actor="cli") == 2
    revokes = read_privacy_events({"action": "consent.revoke"}, max_rows=10)
    pairs = {(e["feature_category"], e["backend"]) for e in revokes}
    assert pairs == {("mcp-data-sharing", "anthropic"), ("ml-categorization", "openai")}
    assert "*" not in {e["feature_category"] for e in revokes}


def test_service_grant_one_time_mode_persists_until_revoked(db: Database) -> None:
    """one-time grants are recorded and persist — enforcement is deferred.

    Pins the current record-only semantics: selecting one-time does NOT
    auto-expire the grant. A future enforcement gate must change this test
    deliberately, not silently.
    """
    svc = ConsentService(db)
    result = svc.grant_consent(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.ONE_TIME,
        actor="cli",
    )
    assert result.grant.consent_mode is ConsentMode.ONE_TIME
    # Still active across repeated reads — no auto-revocation on access.
    assert svc.status().active_grants[0].consent_mode is ConsentMode.ONE_TIME
    assert len(svc.status().active_grants) == 1


def test_consent_target_batch_rolls_back_late_failure(
    db: Database,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A late revoke failure rolls back rows, audits, and privacy side effects."""
    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir",
        lambda: tmp_path,
    )
    svc = ConsentService(db)
    for category in ("mcp-data-sharing", "matching-overview"):
        svc.grant_consent(
            feature_category=category,
            backend="anthropic",
            consent_mode=ConsentMode.PERSISTENT,
            actor="test",
        )
    plan = svc.plan_targets(
        ["mcp-data-sharing", "matching-overview"],
        state="revoked",
        backend="anthropic",
    )
    original = ConsentRepo.revoke
    calls = 0

    def fail_second(
        repo: ConsentRepo,
        *,
        feature_category: str,
        backend: str,
        actor: str,
        parent_audit_id: str | None = None,
        in_outer_txn: bool = False,
    ) -> int:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("late consent failure")
        return original(
            repo,
            feature_category=feature_category,
            backend=backend,
            actor=actor,
            parent_audit_id=parent_audit_id,
            in_outer_txn=in_outer_txn,
        )

    monkeypatch.setattr(ConsentRepo, "revoke", fail_second)
    with pytest.raises(RuntimeError, match="late consent failure"):
        svc.apply_targets(
            plan,
            actor="mcp.privacy_consent_set",
            operation_id="op_task6_consent",
        )

    assert {grant.feature_category for grant in svc.status().active_grants} == {
        "mcp-data-sharing",
        "matching-overview",
    }
    assert db.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'consent.revoke'"
    ).fetchone() == (0,)
    assert read_privacy_events({"action": "consent.revoke"}, max_rows=10) == []
