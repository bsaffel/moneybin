"""Tests for persisted, profile-scoped nonblocking sync authentication."""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import keyring.errors
import pytest
from pydantic import SecretStr

from moneybin.connectors.sync_auth import SyncAuthService
from moneybin.connectors.sync_models import (
    DeviceAuthorizationChallenge,
    LoginPollResult,
)
from moneybin.secrets import SecretStore


class _PatchedKeyring:
    """Thread-safe keyring backend state; SecretStore itself remains real."""

    def __init__(self) -> None:
        self.values: dict[tuple[str, str], str] = {}
        self.lock = threading.Lock()
        self.fail_next_write = False

    def get_password(self, service: str, name: str) -> str | None:
        with self.lock:
            return self.values.get((service, name))

    def set_password(self, service: str, name: str, value: str) -> None:
        with self.lock:
            if self.fail_next_write:
                self.fail_next_write = False
                raise keyring.errors.NoKeyringError
            self.values[(service, name)] = value

    def delete_password(self, service: str, name: str) -> None:
        with self.lock:
            try:
                del self.values[(service, name)]
            except KeyError:
                raise keyring.errors.PasswordDeleteError from None


@pytest.fixture
def patched_keyring(monkeypatch: pytest.MonkeyPatch) -> _PatchedKeyring:
    backend = _PatchedKeyring()
    monkeypatch.setattr("keyring.get_password", backend.get_password)
    monkeypatch.setattr("keyring.set_password", backend.set_password)
    monkeypatch.setattr("keyring.delete_password", backend.delete_password)
    return backend


def _service(
    *,
    client: object,
    store: SecretStore,
    lock_path: Path,
) -> SyncAuthService:
    return SyncAuthService(
        client=client,  # type: ignore[arg-type]
        secrets=store,
        lock_path=lock_path,
        now=lambda: datetime(2026, 7, 19, tzinfo=UTC),
    )


def _challenge(*, interval: float = 0.0) -> DeviceAuthorizationChallenge:
    return DeviceAuthorizationChallenge(
        device_code=SecretStr("secret-device-code"),
        user_code="ABCD-EFGH",
        verification_uri="https://auth.example/activate",
        verification_uri_complete="https://auth.example/activate?code=ABCD-EFGH",
        expires_in=900,
        interval=interval,
    )


def _stored_session(
    auth_session_id: str,
    *,
    status: str,
    expiration: datetime,
    device_code: str | None,
    next_poll_at: datetime | None = None,
) -> dict[str, object]:
    """Build one persisted auth session for lifecycle-retention tests."""
    return {
        "auth_session_id": auth_session_id,
        "status": status,
        "user_code": "ABCD-EFGH",
        "verification_url": "https://auth.example/activate",
        "expiration": expiration.isoformat(),
        "device_code": device_code,
        "poll_interval_seconds": 5.0,
        "next_poll_at": next_poll_at.isoformat() if next_poll_at is not None else None,
        "error_code": None,
    }


def test_begin_persists_one_atomic_collection_and_returns_only_safe_fields(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.begin_login.return_value = _challenge()
    service = _service(
        client=client,
        store=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
    )

    result = service.begin()

    assert result.status == "pending"
    assert result.user_code == "ABCD-EFGH"
    assert result.verification_url is not None
    assert result.verification_url.endswith("ABCD-EFGH")
    assert result.auth_session_id.startswith("syncauth_")
    assert "secret-device-code" not in repr(result)
    assert set(patched_keyring.values) == {("moneybin-alice", "SYNC__AUTH_SESSIONS")}
    collection = json.loads(next(iter(patched_keyring.values.values())))
    assert list(collection) == [result.auth_session_id]
    assert collection[result.auth_session_id]["device_code"] == "secret-device-code"


def test_begin_expires_abandoned_sessions_and_scrubs_terminal_device_codes(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 19, tzinfo=UTC)
    active_id = "syncauth_active"
    expired_id = "syncauth_expired"
    terminal_id = "syncauth_terminal"
    patched_keyring.values[("moneybin-alice", "SYNC__AUTH_SESSIONS")] = json.dumps({
        active_id: _stored_session(
            active_id,
            status="pending",
            expiration=now + timedelta(minutes=10),
            device_code="active-device-code",
        ),
        expired_id: _stored_session(
            expired_id,
            status="pending",
            expiration=now - timedelta(minutes=1),
            device_code="expired-device-code",
        ),
        terminal_id: _stored_session(
            terminal_id,
            status="authenticated",
            expiration=now - timedelta(minutes=2),
            device_code="legacy-terminal-device-code",
        ),
    })
    client = MagicMock()
    client.begin_login.return_value = _challenge()
    service = _service(
        client=client,
        store=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
    )

    created = service.begin()

    collection = json.loads(next(iter(patched_keyring.values.values())))
    assert set(collection) == {
        active_id,
        expired_id,
        terminal_id,
        created.auth_session_id,
    }
    assert collection[active_id]["device_code"] == "active-device-code"
    assert collection[expired_id]["status"] == "expired"
    assert collection[expired_id]["error_code"] == "device_code_expired"
    assert collection[expired_id]["device_code"] is None
    assert collection[terminal_id]["device_code"] is None


def test_begin_retains_only_sixteen_newest_terminal_sessions(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 19, tzinfo=UTC)
    active_id = "syncauth_active"
    sessions = {
        f"syncauth_terminal_{index:02d}": _stored_session(
            f"syncauth_terminal_{index:02d}",
            status="authenticated",
            expiration=now + timedelta(minutes=index),
            device_code="legacy-terminal-device-code",
        )
        for index in range(20)
    }
    sessions[active_id] = _stored_session(
        active_id,
        status="pending",
        expiration=now + timedelta(minutes=30),
        device_code="active-device-code",
    )
    patched_keyring.values[("moneybin-alice", "SYNC__AUTH_SESSIONS")] = json.dumps(
        sessions
    )
    client = MagicMock()
    client.begin_login.return_value = _challenge()
    service = _service(
        client=client,
        store=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
    )

    created = service.begin()

    collection = json.loads(next(iter(patched_keyring.values.values())))
    terminal_ids = {
        session_id
        for session_id, session in collection.items()
        if session["status"] != "pending"
    }
    assert terminal_ids == {f"syncauth_terminal_{index:02d}" for index in range(4, 20)}
    assert active_id in collection
    assert created.auth_session_id in collection
    assert collection[active_id]["device_code"] == "active-device-code"
    assert all(
        collection[session_id]["device_code"] is None for session_id in terminal_ids
    )


def test_begin_retains_new_session_and_only_fifteen_other_pending_sessions(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 19, tzinfo=UTC)
    sessions = {
        f"syncauth_pending_{index:02d}": _stored_session(
            f"syncauth_pending_{index:02d}",
            status="pending",
            expiration=now + timedelta(minutes=index + 1),
            device_code=f"pending-device-code-{index:02d}",
        )
        for index in range(20)
    }
    patched_keyring.values[("moneybin-alice", "SYNC__AUTH_SESSIONS")] = json.dumps(
        sessions
    )
    client = MagicMock()
    client.begin_login.return_value = _challenge()
    service = _service(
        client=client,
        store=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
    )

    created = service.begin()

    collection = json.loads(next(iter(patched_keyring.values.values())))
    assert set(collection) == {
        created.auth_session_id,
        *(f"syncauth_pending_{index:02d}" for index in range(5, 20)),
    }
    assert collection[created.auth_session_id]["device_code"] == "secret-device-code"


def test_status_retains_addressed_session_when_bounding_pending_collection(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 19, tzinfo=UTC)
    target_id = "syncauth_pending_00"
    sessions = {
        f"syncauth_pending_{index:02d}": _stored_session(
            f"syncauth_pending_{index:02d}",
            status="pending",
            expiration=now + timedelta(minutes=index + 1),
            device_code=f"pending-device-code-{index:02d}",
            next_poll_at=now + timedelta(seconds=5),
        )
        for index in range(20)
    }
    patched_keyring.values[("moneybin-alice", "SYNC__AUTH_SESSIONS")] = json.dumps(
        sessions
    )
    client = MagicMock()
    service = _service(
        client=client,
        store=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
    )

    result = service.status(target_id)

    assert result.status == "pending"
    client.poll_login.assert_not_called()
    collection = json.loads(next(iter(patched_keyring.values.values())))
    assert set(collection) == {
        target_id,
        *(f"syncauth_pending_{index:02d}" for index in range(5, 20)),
    }


def test_status_prunes_unrelated_expired_session_before_throttled_return(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 19, tzinfo=UTC)
    active_id = "syncauth_active"
    expired_id = "syncauth_expired"
    patched_keyring.values[("moneybin-alice", "SYNC__AUTH_SESSIONS")] = json.dumps({
        active_id: _stored_session(
            active_id,
            status="pending",
            expiration=now + timedelta(minutes=10),
            device_code="active-device-code",
            next_poll_at=now + timedelta(seconds=5),
        ),
        expired_id: _stored_session(
            expired_id,
            status="pending",
            expiration=now - timedelta(minutes=1),
            device_code="expired-device-code",
        ),
    })
    client = MagicMock()
    service = _service(
        client=client,
        store=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
    )

    result = service.status(active_id)

    assert result.status == "pending"
    client.poll_login.assert_not_called()
    collection = json.loads(next(iter(patched_keyring.values.values())))
    assert collection[expired_id]["status"] == "expired"
    assert collection[expired_id]["device_code"] is None


def test_status_completion_stores_terminal_state_and_is_idempotent(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.begin_login.return_value = _challenge()
    client.poll_login.side_effect = [
        LoginPollResult(status="pending"),
        LoginPollResult(status="authenticated"),
    ]
    service = _service(
        client=client,
        store=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
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
    assert "secret-device-code" not in " ".join(patched_keyring.values.values())


def test_status_upgrades_stored_session_without_poll_schedule_fields(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    """Sessions persisted before polling backoff shipped remain resumable."""
    auth_session_id = "syncauth_legacy"
    patched_keyring.values[("moneybin-alice", "SYNC__AUTH_SESSIONS")] = json.dumps({
        auth_session_id: {
            "auth_session_id": auth_session_id,
            "status": "pending",
            "user_code": "ABCD-EFGH",
            "verification_url": "https://auth.example/activate",
            "expiration": "2026-07-19T00:15:00+00:00",
            "device_code": "secret-device-code",
            "error_code": None,
        }
    })
    client = MagicMock()
    client.poll_login.return_value = LoginPollResult(status="pending")
    service = _service(
        client=client,
        store=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
    )

    result = service.status(auth_session_id)

    assert result.status == "pending"
    client.poll_login.assert_called_once_with("secret-device-code")
    stored = json.loads(next(iter(patched_keyring.values.values())))[auth_session_id]
    assert stored["poll_interval_seconds"] == 5.0
    assert stored["next_poll_at"] == "2026-07-19T00:00:05+00:00"


def test_slow_down_persists_and_enforces_increased_poll_interval(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    now = [datetime(2026, 7, 19, tzinfo=UTC)]
    client = MagicMock()
    client.begin_login.return_value = _challenge(interval=5)
    client.poll_login.side_effect = [
        LoginPollResult(status="slow_down"),
        LoginPollResult(status="pending"),
    ]
    service = SyncAuthService(
        client=client,
        secrets=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
        now=lambda: now[0],
    )
    auth_session_id = service.begin().auth_session_id

    assert service.status(auth_session_id).status == "pending"
    client.poll_login.assert_not_called()

    now[0] += timedelta(seconds=5)
    assert service.status(auth_session_id).status == "pending"
    assert client.poll_login.call_count == 1
    collection = json.loads(next(iter(patched_keyring.values.values())))
    stored = collection[auth_session_id]
    assert stored["poll_interval_seconds"] == 10.0
    assert stored["next_poll_at"] == "2026-07-19T00:00:15+00:00"

    now[0] += timedelta(seconds=5)
    assert service.status(auth_session_id).status == "pending"
    assert client.poll_login.call_count == 1

    now[0] += timedelta(seconds=5)
    assert service.status(auth_session_id).status == "pending"
    assert client.poll_login.call_count == 2


def test_expired_session_never_calls_provider(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.begin_login.return_value = _challenge()
    times = iter([
        datetime(2026, 7, 19, tzinfo=UTC),
        datetime(2026, 7, 19, 0, 16, tzinfo=UTC),
    ])
    service = SyncAuthService(
        client=client,
        secrets=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
        now=lambda: next(times),
    )
    auth_session_id = service.begin().auth_session_id

    result = service.status(auth_session_id)

    assert result.status == "expired"
    assert result.replayed is False
    client.poll_login.assert_not_called()


def test_logout_clears_tokens_and_every_pending_auth_session(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.begin_login.return_value = _challenge()
    service = _service(
        client=client,
        store=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
    )
    service.begin()
    service.begin()

    result = service.logout()

    assert result.status == "logged_out"
    assert result.cleared_auth_sessions == 2
    client.logout.assert_called_once_with()
    assert patched_keyring.values == {}

    replay = service.logout()
    assert replay.cleared_auth_sessions == 0
    assert client.logout.call_count == 2


def test_failed_atomic_write_preserves_prior_collection(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.begin_login.return_value = _challenge()
    service = _service(
        client=client,
        store=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
    )
    first = service.begin()
    before = dict(patched_keyring.values)
    patched_keyring.fail_next_write = True

    with pytest.raises(Exception, match="No OS keyring backend"):
        service.begin()

    assert patched_keyring.values == before
    assert first.auth_session_id in json.loads(next(iter(before.values())))


def test_concurrent_begin_preserves_every_session(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    client = MagicMock()
    client.begin_login.return_value = _challenge()
    lock_path = tmp_path / "alice.sync-auth"

    def begin() -> str:
        return (
            _service(
                client=client,
                store=SecretStore(profile="alice"),
                lock_path=lock_path,
            )
            .begin()
            .auth_session_id
        )

    def begin_one(_: int) -> str:
        return begin()

    with ThreadPoolExecutor(max_workers=4) as executor:
        session_ids = list(executor.map(begin_one, range(8)))

    collection = json.loads(next(iter(patched_keyring.values.values())))
    assert set(collection) == set(session_ids)
    assert len(collection) == 8


def test_concurrent_status_polls_once_and_replays_terminal_state(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    entered_poll = threading.Event()
    release_poll = threading.Event()
    client = MagicMock()
    client.begin_login.return_value = _challenge()

    def poll(_: str) -> LoginPollResult:
        entered_poll.set()
        assert release_poll.wait(timeout=2)
        return LoginPollResult(status="authenticated")

    client.poll_login.side_effect = poll
    lock_path = tmp_path / "alice.sync-auth"
    first_service = _service(
        client=client,
        store=SecretStore(profile="alice"),
        lock_path=lock_path,
    )
    second_service = _service(
        client=client,
        store=SecretStore(profile="alice"),
        lock_path=lock_path,
    )
    auth_session_id = first_service.begin().auth_session_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(first_service.status, auth_session_id)
        assert entered_poll.wait(timeout=2)
        second = executor.submit(second_service.status, auth_session_id)
        release_poll.set()
        outcomes = [first.result(timeout=2), second.result(timeout=2)]

    assert client.poll_login.call_count == 1
    assert [outcome.status for outcome in outcomes] == [
        "authenticated",
        "authenticated",
    ]
    assert sorted(outcome.replayed for outcome in outcomes) == [False, True]


def test_concurrent_logout_clears_tokens_after_inflight_status(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    entered_poll = threading.Event()
    release_poll = threading.Event()

    class _ConcurrentClient:
        def __init__(self) -> None:
            self.authenticated = False

        def begin_login(self) -> DeviceAuthorizationChallenge:
            return _challenge()

        def poll_login(self, _: str) -> LoginPollResult:
            entered_poll.set()
            assert release_poll.wait(timeout=2)
            self.authenticated = True
            return LoginPollResult(status="authenticated")

        def logout(self) -> None:
            self.authenticated = False

    client = _ConcurrentClient()
    service = _service(
        client=client,
        store=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
    )
    auth_session_id = service.begin().auth_session_id

    with ThreadPoolExecutor(max_workers=2) as executor:
        status = executor.submit(service.status, auth_session_id)
        assert entered_poll.wait(timeout=2)
        logout = executor.submit(service.logout)
        release_poll.set()
        assert status.result(timeout=2).status == "authenticated"
        assert logout.result(timeout=2).status == "logged_out"

    assert client.authenticated is False
    assert patched_keyring.values == {}


def test_profile_namespaces_do_not_share_sessions(
    patched_keyring: _PatchedKeyring,
    tmp_path: Path,
) -> None:
    alice_client = MagicMock()
    bob_client = MagicMock()
    alice_client.begin_login.return_value = _challenge()
    bob_client.begin_login.return_value = _challenge()
    alice = _service(
        client=alice_client,
        store=SecretStore(profile="alice"),
        lock_path=tmp_path / "alice.sync-auth",
    )
    bob = _service(
        client=bob_client,
        store=SecretStore(profile="bob"),
        lock_path=tmp_path / "bob.sync-auth",
    )
    alice.begin()
    bob_session = bob.begin()

    alice.logout()

    assert set(patched_keyring.values) == {("moneybin-bob", "SYNC__AUTH_SESSIONS")}
    assert bob.status(bob_session.auth_session_id).status == "pending"
