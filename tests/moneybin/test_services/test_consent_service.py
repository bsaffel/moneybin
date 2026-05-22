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
    grant = svc.grant_consent(
        feature_category="mcp-data-sharing",
        backend=None,  # falls back to default_backend
        consent_mode=ConsentMode.PERSISTENT,
        actor="cli.privacy_grant",
    )
    assert grant.backend == "anthropic"


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
    assert status.consent_mode == "standard"
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
    assert (
        svc.revoke_consent(
            feature_category="mcp-data-sharing", backend="anthropic", actor="cli"
        )
        == 1
    )
    assert svc.status().active_grants == []
