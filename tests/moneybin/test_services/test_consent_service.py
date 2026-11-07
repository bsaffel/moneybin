"""Tests for ConsentService — backend resolution, validation, status."""

from __future__ import annotations

from pathlib import Path

import pytest

from moneybin.config import clear_settings_cache, set_current_profile
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.consent import ConsentMode
from moneybin.privacy.disclosure import (
    host_from_client_info,
    host_from_environment,
    terms_version_for,
)
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


@pytest.mark.parametrize("drift", ["added", "removed"])
def test_service_revoke_all_refuses_a_stale_selected_grant_set(
    db: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    """A confirmation snapshot never widens or mutates after it goes stale."""
    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir", lambda: tmp_path
    )
    svc = ConsentService(db)
    first = svc.grant_consent(
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        actor="test",
    ).grant
    expected_ids = {first.grant_id}
    if drift == "added":
        svc.grant_consent(
            feature_category="ml-categorization",
            backend="anthropic",
            consent_mode=ConsentMode.PERSISTENT,
            actor="test",
        )
    else:
        svc.revoke_consent(
            feature_category="mcp-data-sharing", backend="anthropic", actor="test"
        )

    audit_before_row = db.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'consent.revoke'"
    ).fetchone()
    assert audit_before_row is not None
    audit_before = audit_before_row[0]
    events_before = read_privacy_events({"action": "consent.revoke"}, max_rows=20)
    with pytest.raises(UserError, match="Consent selection changed") as exc:
        svc.revoke_all(actor="cli", expected_grant_ids=expected_ids)

    assert exc.value.code == "mutation_confirmation_mismatch"
    assert {grant.grant_id for grant in svc.status().active_grants} != expected_ids
    audit_after_row = db.execute(
        "SELECT COUNT(*) FROM app.audit_log WHERE action = 'consent.revoke'"
    ).fetchone()
    assert audit_after_row is not None
    assert audit_after_row[0] == audit_before
    assert (
        read_privacy_events({"action": "consent.revoke"}, max_rows=20) == events_before
    )


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


# --- ai_host recipients (MB-59) ---


_HOST_CATEGORY = "smart-import-parsing"


def _host(name: str = "claude-ai", transport: str = "stdio"):  # test helper
    return host_from_client_info(name, "1", transport=transport)  # type: ignore[arg-type]  # test transport literal


def test_host_grant_does_not_collide_with_backend_grant(db: Database) -> None:
    svc = ConsentService(db)
    svc.grant_consent(
        feature_category=_HOST_CATEGORY,
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli",
    )
    grant, created = svc.grant_host_consent(
        host=_host(), approved_via="cli_prompt", actor="cli"
    )
    assert created is True
    assert grant.recipient_kind == "ai_host"
    assert grant.backend == "local:claude-ai"
    assert grant.approved_via == "cli_prompt"
    assert grant.terms_version == terms_version_for(_host())
    assert svc.active_host_grant(host=_host()) is not None


def test_legacy_backend_grant_does_not_satisfy_host_gate(db: Database) -> None:
    svc = ConsentService(db)
    svc.grant_consent(
        feature_category=_HOST_CATEGORY,
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli",
    )
    assert svc.active_host_grant(host=_host()) is None


def test_terms_version_bump_deactivates_host_grant(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    svc = ConsentService(db)
    svc.grant_host_consent(host=_host(), approved_via="cli_prompt", actor="cli")

    def _bumped(_host: object) -> str:
        return "bumped"

    monkeypatch.setattr("moneybin.services.consent_service.terms_version_for", _bumped)
    assert svc.active_host_grant(host=_host()) is None


def test_active_host_grant_requires_matching_transport(db: Database) -> None:
    svc = ConsentService(db)
    svc.grant_host_consent(host=_host(), approved_via="cli_prompt", actor="cli")
    assert svc.active_host_grant(host=_host(transport="http")) is None


def test_unidentified_host_cannot_be_granted(db: Database) -> None:
    svc = ConsentService(db)
    with pytest.raises(ValueError):
        svc.grant_host_consent(
            host=host_from_environment({}), approved_via="cli_prompt", actor="cli"
        )
    assert svc.active_host_grant(host=host_from_environment({})) is None


def test_revoke_host_consent(db: Database) -> None:
    svc = ConsentService(db)
    svc.grant_host_consent(host=_host(), approved_via="cli_prompt", actor="cli")
    result = svc.revoke_host_consent(recipient_key="local:claude-ai", actor="cli")
    assert result.count == 1 and result.backend == "local:claude-ai"
    assert svc.active_host_grant(host=_host()) is None
    assert (
        svc.revoke_host_consent(recipient_key="local:claude-ai", actor="cli").count == 0
    )


def test_backend_name_shaped_like_recipient_key_is_refused(db: Database) -> None:
    with pytest.raises(UserError) as exc:
        ConsentService(db).grant_consent(
            feature_category=_HOST_CATEGORY,
            backend="local:claude-ai",
            consent_mode=ConsentMode.PERSISTENT,
            actor="mcp",
        )
    assert exc.value.code == "privacy_consent_recipient_unsupported"


def test_host_grant_stores_redaction_policy_version(db: Database) -> None:
    from moneybin.privacy.disclosure import REDACTION_POLICY_VERSION

    grant, _ = ConsentService(db).grant_host_consent(
        host=_host(), approved_via="cli_prompt", actor="cli"
    )
    assert grant.redaction_policy_version == REDACTION_POLICY_VERSION


def test_redaction_policy_bump_deactivates_and_regrant_replaces(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    svc = ConsentService(db)
    svc.grant_host_consent(host=_host(), approved_via="cli_prompt", actor="cli")
    monkeypatch.setattr(
        "moneybin.services.consent_service.REDACTION_POLICY_VERSION", "99"
    )
    assert svc.active_host_grant(host=_host()) is None
    regrant, created = svc.grant_host_consent(
        host=_host(), approved_via="cli_prompt", actor="cli"
    )
    assert created is True and regrant.redaction_policy_version == "99"
    assert svc.active_host_grant(host=_host()) is not None
    rows = db.execute(
        "SELECT COUNT(*) FILTER (WHERE revoked_at IS NULL), COUNT(*) "
        "FROM app.ai_consent_grants WHERE recipient_kind = 'ai_host'"
    ).fetchone()
    assert rows == (1, 2)


def test_terms_bump_regrant_creates_new_active_row_and_revokes_old(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    svc = ConsentService(db)
    first, _ = svc.grant_host_consent(
        host=_host(), approved_via="cli_prompt", actor="cli"
    )

    def _bumped(_host: object) -> str:
        return "bumped"

    monkeypatch.setattr("moneybin.services.consent_service.terms_version_for", _bumped)
    second, created = svc.grant_host_consent(
        host=_host(), approved_via="cli_prompt", actor="cli"
    )
    assert created is True and second.grant_id != first.grant_id
    assert svc.active_host_grant(host=_host()) is not None
    old = db.execute(
        "SELECT revoked_at IS NOT NULL FROM app.ai_consent_grants WHERE grant_id = ?",
        [first.grant_id],
    ).fetchone()
    assert old == (True,)


def test_regrant_same_terms_is_not_created(db: Database) -> None:
    svc = ConsentService(db)
    svc.grant_host_consent(host=_host(), approved_via="cli_prompt", actor="cli")
    _, created = svc.grant_host_consent(
        host=_host(), approved_via="cli_prompt", actor="cli"
    )
    assert created is False


def test_host_grant_records_the_given_prompt(db: Database) -> None:
    ConsentService(db).grant_host_consent(
        host=_host(),
        approved_via="cli_prompt",
        actor="cli",
        grant_prompt="exact text shown",
    )
    row = db.execute(
        "SELECT grant_prompt FROM app.ai_consent_grants WHERE recipient_kind='ai_host'"
    ).fetchone()
    assert row == ("exact text shown",)


def test_stale_terms_regrant_logs_consent_revoke(
    db: Database, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Privacy log shows the stale grant's revoke before the replacing grant."""
    monkeypatch.setattr(
        "moneybin.privacy.log._resolve_privacy_log_dir", lambda: tmp_path
    )
    svc = ConsentService(db)
    svc.grant_host_consent(host=_host(), approved_via="cli_prompt", actor="cli")

    def _bumped(_host: object) -> str:
        return "bumped"

    monkeypatch.setattr("moneybin.services.consent_service.terms_version_for", _bumped)
    svc.grant_host_consent(host=_host(), approved_via="cli_prompt", actor="cli")

    revokes = read_privacy_events({"action": "consent.revoke"}, max_rows=10)
    assert [(e["feature_category"], e["backend"]) for e in revokes] == [
        (_HOST_CATEGORY, "local:claude-ai")
    ]
