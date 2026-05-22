"""Unit tests for moneybin.privacy.consent primitives."""

from datetime import datetime

from moneybin.privacy.consent import ConsentMode, GrantInfo


def test_consent_mode_values():
    assert ConsentMode.PERSISTENT == "persistent"
    assert ConsentMode.ONE_TIME == "one-time"
    assert {m.value for m in ConsentMode} == {"persistent", "one-time"}


def test_grant_info_is_frozen():
    info = GrantInfo(
        grant_id="abc123",
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode=ConsentMode.PERSISTENT,
        granted_at=datetime(2026, 5, 22, 12, 0, 0),
        revoked_at=None,
    )
    assert info.feature_category == "mcp-data-sharing"
    import dataclasses

    import pytest

    with pytest.raises(dataclasses.FrozenInstanceError):
        info.backend = "openai"  # type: ignore[misc]


def test_build_consent_event_shape():
    from moneybin.privacy.log import build_consent_event

    event = build_consent_event(
        actor="cli.privacy_grant",
        action="consent.grant",
        feature_category="mcp-data-sharing",
        backend="anthropic",
        consent_mode="persistent",
    )
    assert event["action"] == "consent.grant"
    assert event["feature_category"] == "mcp-data-sharing"
    assert event["backend"] == "anthropic"
    assert event["consent_mode"] == "persistent"
    assert "ts" in event
    assert event["actor"] == "cli.privacy_grant"
