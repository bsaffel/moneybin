"""Persisted profile-scoped orchestration for nonblocking sync authentication."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Callable, Generator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, cast

from moneybin.connectors.sync_client import SyncClient
from moneybin.connectors.sync_errors import SyncAPIError, SyncAuthError
from moneybin.errors import UserError
from moneybin.secrets import SecretNotFoundError, SecretStore

_AUTH_SESSIONS_KEY = "SYNC__AUTH_SESSIONS"
_LOCK_FILE_MODE = 0o600

AuthStatus = Literal[
    "pending",
    "authenticated",
    "denied",
    "expired",
    "provider_error",
]


@dataclass(frozen=True, slots=True)
class SyncAuthResult:
    """Safe public result of beginning or polling sync authentication."""

    auth_session_id: str
    status: AuthStatus
    user_code: str | None
    verification_url: str | None
    expiration: str
    replayed: bool = False
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class SyncLogoutResult:
    """Outcome of clearing scoped sync credentials and pending auth sessions."""

    status: Literal["logged_out"]
    cleared_auth_sessions: int


@dataclass(frozen=True, slots=True)
class _StoredAuthSession:
    """Secret-store representation; device_code never crosses the service boundary."""

    auth_session_id: str
    status: AuthStatus
    user_code: str | None
    verification_url: str | None
    expiration: str
    device_code: str | None
    poll_interval_seconds: float = 5.0
    next_poll_at: str | None = None
    error_code: str | None = None

    def safe_result(self, *, replayed: bool = False) -> SyncAuthResult:
        return SyncAuthResult(
            auth_session_id=self.auth_session_id,
            status=self.status,
            user_code=self.user_code,
            verification_url=self.verification_url,
            expiration=self.expiration,
            replayed=replayed,
            error_code=self.error_code,
        )


class SyncAuthService:
    """Coordinate nonblocking device auth with profile-scoped secret persistence."""

    def __init__(
        self,
        *,
        client: SyncClient,
        secrets: SecretStore | None = None,
        lock_path: Path | None = None,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        """Bind one sync client to its profile-scoped secret store."""
        self._client = client
        self._secrets = secrets or SecretStore()
        if lock_path is None:
            from moneybin.config import get_settings

            lock_path = get_settings().profile_dir / ".sync-auth.lock"
        self._lock_path = lock_path
        self._now = now or (lambda: datetime.now(UTC))

    def begin(self) -> SyncAuthResult:
        """Start a device flow and persist only its secret half in keychain storage."""
        challenge = self._client.begin_login()
        auth_session_id = f"syncauth_{uuid.uuid4().hex}"
        now = self._now()
        expiration = now + timedelta(seconds=challenge.expires_in)
        session = _StoredAuthSession(
            auth_session_id=auth_session_id,
            status="pending",
            user_code=challenge.user_code,
            verification_url=cast(
                str,
                challenge.verification_uri_complete or challenge.verification_uri,
            ),
            expiration=expiration.isoformat(),
            device_code=challenge.device_code.get_secret_value(),
            poll_interval_seconds=challenge.interval,
            next_poll_at=(now + timedelta(seconds=challenge.interval)).isoformat(),
        )
        with self._acquire_lock():
            sessions = self._load_collection()
            sessions[auth_session_id] = session
            self._save_collection(sessions)
        return session.safe_result()

    def status(self, auth_session_id: str) -> SyncAuthResult:
        """Poll once, returning stable terminal state on subsequent calls."""
        with self._acquire_lock():
            sessions = self._load_collection()
            session = self._get_session(sessions, auth_session_id)
            now = self._now()
            if session.status != "pending":
                return session.safe_result(replayed=True)
            if now >= datetime.fromisoformat(session.expiration):
                expired = replace(
                    session,
                    status="expired",
                    device_code=None,
                    error_code="device_code_expired",
                )
                sessions[auth_session_id] = expired
                self._save_collection(sessions)
                return expired.safe_result()
            if session.next_poll_at is not None and now < datetime.fromisoformat(
                session.next_poll_at
            ):
                return session.safe_result()
            if session.device_code is None:
                raise UserError(
                    "Authentication session cannot be resumed.",
                    code="SYNC_AUTH_SESSION_INVALID",
                )
            try:
                polled = self._client.poll_login(session.device_code)
            except SyncAuthError as exc:
                status: Literal["denied", "expired"] = (
                    "denied" if "denied" in str(exc).lower() else "expired"
                )
                terminal = replace(
                    session,
                    status=status,
                    device_code=None,
                    error_code=(
                        "authorization_denied"
                        if status == "denied"
                        else "device_code_expired"
                    ),
                )
                sessions[auth_session_id] = terminal
                self._save_collection(sessions)
                return terminal.safe_result()
            except SyncAPIError:
                pending = self._schedule_next_poll(session, now=now)
                sessions[auth_session_id] = pending
                self._save_collection(sessions)
                return replace(
                    pending.safe_result(),
                    status="provider_error",
                    error_code="sync_provider_error",
                )
            if polled.status != "authenticated":
                interval = session.poll_interval_seconds
                if polled.status == "slow_down":
                    interval += 5.0
                pending = self._schedule_next_poll(
                    session,
                    now=now,
                    interval=interval,
                )
                sessions[auth_session_id] = pending
                self._save_collection(sessions)
                return pending.safe_result()
            authenticated = replace(
                session,
                status="authenticated",
                device_code=None,
                error_code=None,
            )
            sessions[auth_session_id] = authenticated
            self._save_collection(sessions)
            return authenticated.safe_result()

    @staticmethod
    def _schedule_next_poll(
        session: _StoredAuthSession,
        *,
        now: datetime,
        interval: float | None = None,
    ) -> _StoredAuthSession:
        """Persist the RFC 8628 minimum delay before another provider poll."""
        poll_interval = session.poll_interval_seconds if interval is None else interval
        return replace(
            session,
            poll_interval_seconds=poll_interval,
            next_poll_at=(now + timedelta(seconds=poll_interval)).isoformat(),
        )

    def logout(self) -> SyncLogoutResult:
        """Clear scoped broker tokens and every persisted device-auth session."""
        with self._acquire_lock():
            sessions = self._load_collection()
            self._client.logout()
            self._delete_if_present(_AUTH_SESSIONS_KEY)
        return SyncLogoutResult(
            status="logged_out",
            cleared_auth_sessions=len(sessions),
        )

    def _get_session(
        self,
        sessions: dict[str, _StoredAuthSession],
        auth_session_id: str,
    ) -> _StoredAuthSession:
        if not auth_session_id.startswith("syncauth_"):
            raise UserError(
                "Unknown authentication session.",
                code="SYNC_AUTH_SESSION_NOT_FOUND",
            )
        try:
            return sessions[auth_session_id]
        except KeyError:
            raise UserError(
                "Authentication session was not found or was already cleared.",
                code="SYNC_AUTH_SESSION_NOT_FOUND",
            ) from None

    def _load_collection(self) -> dict[str, _StoredAuthSession]:
        try:
            raw = self._secrets.get_key(_AUTH_SESSIONS_KEY)
        except SecretNotFoundError:
            return {}
        payload = cast(dict[str, dict[str, object]], json.loads(raw))
        return {
            auth_session_id: _StoredAuthSession(**session)  # type: ignore[arg-type]
            for auth_session_id, session in payload.items()
        }

    def _save_collection(
        self,
        sessions: dict[str, _StoredAuthSession],
    ) -> None:
        payload = {
            auth_session_id: asdict(session)
            for auth_session_id, session in sessions.items()
        }
        self._secrets.set_key(
            _AUTH_SESSIONS_KEY,
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
        )

    def _delete_if_present(self, key: str) -> None:
        try:
            self._secrets.delete_key(key)
        except SecretNotFoundError:
            pass

    @contextmanager
    def _acquire_lock(self) -> Generator[None, None, None]:
        """Serialize profile-scoped collection updates across processes."""
        import fcntl

        self._lock_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(
            self._lock_path,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            _LOCK_FILE_MODE,
        )
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
