"""Persisted profile-scoped orchestration for nonblocking sync authentication."""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Literal, cast

from moneybin.connectors.sync_client import SyncClient
from moneybin.connectors.sync_errors import SyncAPIError, SyncAuthError
from moneybin.errors import UserError
from moneybin.secrets import SecretNotFoundError, SecretStore

_AUTH_SESSION_INDEX_KEY = "SYNC__AUTH_SESSION_INDEX"
_AUTH_SESSION_KEY_PREFIX = "SYNC__AUTH_SESSION__"

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
        now: Callable[[], datetime] | None = None,
    ) -> None:
        """Bind one sync client to its profile-scoped secret store."""
        self._client = client
        self._secrets = secrets or SecretStore()
        self._now = now or (lambda: datetime.now(UTC))

    def begin(self) -> SyncAuthResult:
        """Start a device flow and persist only its secret half in keychain storage."""
        challenge = self._client.begin_login()
        auth_session_id = f"syncauth_{uuid.uuid4().hex}"
        expiration = self._now() + timedelta(seconds=challenge.expires_in)
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
        )
        self._save(session)
        self._add_to_index(auth_session_id)
        return session.safe_result()

    def status(self, auth_session_id: str) -> SyncAuthResult:
        """Poll once, returning stable terminal state on subsequent calls."""
        session = self._load(auth_session_id)
        if session.status != "pending":
            return session.safe_result(replayed=True)
        if self._now() >= datetime.fromisoformat(session.expiration):
            expired = replace(
                session,
                status="expired",
                device_code=None,
                error_code="device_code_expired",
            )
            self._save(expired)
            return expired.safe_result()
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
            self._save(terminal)
            return terminal.safe_result()
        except SyncAPIError:
            return replace(
                session.safe_result(),
                status="provider_error",
                error_code="sync_provider_error",
            )
        if polled.status != "authenticated":
            return session.safe_result()
        authenticated = replace(
            session,
            status="authenticated",
            device_code=None,
            error_code=None,
        )
        self._save(authenticated)
        return authenticated.safe_result()

    def logout(self) -> SyncLogoutResult:
        """Clear scoped broker tokens and every persisted device-auth session."""
        self._client.logout()
        session_ids = self._load_index()
        for auth_session_id in session_ids:
            self._delete_if_present(self._session_key(auth_session_id))
        self._delete_if_present(_AUTH_SESSION_INDEX_KEY)
        return SyncLogoutResult(
            status="logged_out",
            cleared_auth_sessions=len(session_ids),
        )

    def _load(self, auth_session_id: str) -> _StoredAuthSession:
        if not auth_session_id.startswith("syncauth_"):
            raise UserError(
                "Unknown authentication session.",
                code="SYNC_AUTH_SESSION_NOT_FOUND",
            )
        try:
            raw = self._secrets.get_key(self._session_key(auth_session_id))
        except SecretNotFoundError:
            raise UserError(
                "Authentication session was not found or was already cleared.",
                code="SYNC_AUTH_SESSION_NOT_FOUND",
            ) from None
        payload = json.loads(raw)
        return _StoredAuthSession(**payload)

    def _save(self, session: _StoredAuthSession) -> None:
        self._secrets.set_key(
            self._session_key(session.auth_session_id),
            json.dumps(asdict(session), sort_keys=True, separators=(",", ":")),
        )

    def _load_index(self) -> list[str]:
        try:
            raw = self._secrets.get_key(_AUTH_SESSION_INDEX_KEY)
        except SecretNotFoundError:
            return []
        payload = json.loads(raw)
        return [str(value) for value in payload]

    def _add_to_index(self, auth_session_id: str) -> None:
        session_ids = self._load_index()
        if auth_session_id not in session_ids:
            session_ids.append(auth_session_id)
        self._secrets.set_key(
            _AUTH_SESSION_INDEX_KEY,
            json.dumps(session_ids, sort_keys=True, separators=(",", ":")),
        )

    def _delete_if_present(self, key: str) -> None:
        try:
            self._secrets.delete_key(key)
        except SecretNotFoundError:
            pass

    @staticmethod
    def _session_key(auth_session_id: str) -> str:
        return f"{_AUTH_SESSION_KEY_PREFIX}{auth_session_id}"
