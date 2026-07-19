"""Tests for persisted, profile-scoped nonblocking sync authentication."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from pydantic import SecretStr

from moneybin.connectors.sync_auth import SyncAuthService
from moneybin.connectors.sync_models import (
    DeviceAuthorizationChallenge,
    LoginPollResult,
)
from moneybin.secrets import SecretNotFoundError


class _MemorySecrets:
    """Minimal SecretStore-compatible profile-scoped test double."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get_key(self, name: str) -> str:
        try:
            return self.values[name]
        except KeyError:
            raise SecretNotFoundError(name) from None

    def set_key(self, name: str, value: str) -> None:
        self.values[name] = value

    def delete_key(self, name: str) -> None:
        try:
            del self.values[name]
        except KeyError:
            raise SecretNotFoundError(name) from None


def _challenge() -> DeviceAuthorizationChallenge:
    return DeviceAuthorizationChallenge(
        device_code=SecretStr("secret-device-code"),
        user_code="ABCD-EFGH",
        verification_uri="https://auth.example/activate",
        verification_uri_complete="https://auth.example/activate?code=ABCD-EFGH",
        expires_in=900,
        interval=5,
    )


def test_begin_persists_secret_session_and_returns_only_safe_fields() -> None:
    client = MagicMock()
    client.begin_login.return_value = _challenge()
    secrets = _MemorySecrets()
    service = SyncAuthService(
        client=client,
        secrets=secrets,  # type: ignore[arg-type]
        now=lambda: datetime(2026, 7, 19, tzinfo=UTC),
    )

    result = service.begin()

    assert result.status == "pending"
    assert result.user_code == "ABCD-EFGH"
    assert result.verification_url is not None
    assert result.verification_url.endswith("ABCD-EFGH")
    assert result.auth_session_id.startswith("syncauth_")
    assert "secret-device-code" not in repr(result)
    assert any("secret-device-code" in value for value in secrets.values.values())


def test_status_completion_stores_terminal_state_and_is_idempotent() -> None:
    client = MagicMock()
    client.begin_login.return_value = _challenge()
    client.poll_login.side_effect = [
        LoginPollResult(status="pending"),
        LoginPollResult(status="authenticated"),
    ]
    secrets = _MemorySecrets()
    service = SyncAuthService(
        client=client,
        secrets=secrets,  # type: ignore[arg-type]
        now=lambda: datetime(2026, 7, 19, tzinfo=UTC),
    )
    auth_session_id = service.begin().auth_session_id

    pending = service.status(auth_session_id)
    authenticated = service.status(auth_session_id)
    replay = service.status(auth_session_id)

    assert pending.status == "pending"
    assert authenticated.status == "authenticated"
    assert authenticated.replayed is False
    assert replay.status == "authenticated"
    assert replay.replayed is True
    assert client.poll_login.call_count == 2
    assert "secret-device-code" not in " ".join(secrets.values.values())


def test_expired_session_never_calls_provider() -> None:
    client = MagicMock()
    client.begin_login.return_value = _challenge()
    secrets = _MemorySecrets()
    times = iter([
        datetime(2026, 7, 19, tzinfo=UTC),
        datetime(2026, 7, 19, 0, 16, tzinfo=UTC),
    ])
    service = SyncAuthService(
        client=client,
        secrets=secrets,  # type: ignore[arg-type]
        now=lambda: next(times),
    )
    auth_session_id = service.begin().auth_session_id

    result = service.status(auth_session_id)

    assert result.status == "expired"
    assert result.replayed is False
    client.poll_login.assert_not_called()


def test_logout_clears_tokens_and_every_pending_auth_session() -> None:
    client = MagicMock()
    client.begin_login.return_value = _challenge()
    secrets = _MemorySecrets()
    service = SyncAuthService(
        client=client,
        secrets=secrets,  # type: ignore[arg-type]
        now=lambda: datetime(2026, 7, 19, tzinfo=UTC),
    )
    service.begin()
    service.begin()

    result = service.logout()

    assert result.status == "logged_out"
    assert result.cleared_auth_sessions == 2
    client.logout.assert_called_once_with()
    assert secrets.values == {}
