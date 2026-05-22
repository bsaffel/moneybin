"""Tests for ConsentService — backend resolution, validation, status."""

from __future__ import annotations

import pytest

from moneybin.config import clear_settings_cache, set_current_profile
from moneybin.database import Database
from moneybin.errors import UserError
from moneybin.privacy.consent import ConsentMode
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
