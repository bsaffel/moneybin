"""HTTP client for moneybin-sync.

Pure transport — no business logic, no database access. Methods correspond
1:1 to server endpoints. Service-layer orchestration lives in SyncService.

Token storage uses SecretStore and requires a writable secure OS keychain.
Exact-profile legacy plaintext files are imported once after verified storage;
new credentials are never written to plaintext files.

Timeouts:
- _DEFAULT_TIMEOUT (15s) for most endpoints
- _LONG_TIMEOUT (120s) for POST /sync/trigger
- _LINK_POLL_DEADLINE (300s) for the browser link flow (decoupled from the HTTP
  timeouts: completing a bank's OAuth + MFA can take minutes)
Per design — no per-endpoint configuration knobs unless evidence demands them.
"""

from __future__ import annotations

import json
import logging
import sys
import time
import webbrowser
from pathlib import Path
from typing import cast

import httpx

from moneybin.connectors.sync_errors import (
    SyncAPIError,
    SyncAuthError,
    SyncLinkError,
    SyncTimeoutError,
)
from moneybin.connectors.sync_models import (
    AuthToken,
    ConnectedInstitution,
    DeviceAuthorizationChallenge,
    LinkInitiateResponse,
    LinkStatusResponse,
    LoginPollResult,
    SyncAckResponse,
    SyncDataResponse,
    SyncTriggerResponse,
)
from moneybin.metrics.registry import SYNC_AUTH_REFRESH_OUTCOMES
from moneybin.secrets import (
    SecretNotFoundError,
    SecretStorageUnavailableError,
    SecretStore,
    SecretUnavailableError,
)

logger = logging.getLogger(__name__)

_KEYRING_JWT_KEY = "jwt"
_KEYRING_REFRESH_KEY = "refresh_token"
_PENDING_KEY = "pending"

_DEFAULT_TIMEOUT = httpx.Timeout(15.0, connect=10.0)
_LONG_TIMEOUT = httpx.Timeout(120.0, connect=10.0)
_LINK_POLL_INTERVAL = 3.0
# How long the CLI waits for the user to finish the browser link flow before
# giving up. Decoupled from the HTTP timeouts above: completing a real bank's
# OAuth + MFA can legitimately take minutes, far longer than any single request.
_LINK_POLL_DEADLINE = 300.0


class SyncClient:
    """HTTP client using profile-scoped SecretStore credentials."""

    _sleep = staticmethod(time.sleep)

    def __init__(
        self,
        server_url: str,
        profile_id: str | None = None,
        *,
        secret_store: SecretStore | None = None,
    ) -> None:
        """Bind transport to a broker identity and its secure credential store."""
        self._server_url = server_url.rstrip("/")
        self._profile_id = profile_id
        self._store = secret_store or SecretStore.for_sync(profile_id)
        self._client = httpx.Client(base_url=self._server_url, timeout=_DEFAULT_TIMEOUT)

    def _optional_key(self, name: str) -> str | None:
        try:
            return self._store.get_key(name)
        except SecretUnavailableError:
            raise
        except SecretNotFoundError:
            return None

    def _store_tokens(self, *, access_token: str, refresh_token: str) -> None:
        """Publish a verified pair; interrupted writes remain unavailable."""
        if not access_token.strip() or not refresh_token.strip():
            raise SecretStorageUnavailableError("Sync credential pair is incomplete.")
        # Separate keychain writes are not atomic. Readers must refuse a pair
        # until both new values have been verified and the marker is removed.
        self._store.set_key(_PENDING_KEY, "1")
        if self._optional_key(_PENDING_KEY) != "1":
            raise SecretStorageUnavailableError(
                "Sync credential write could not start safely."
            )
        try:
            self._store.set_key(_KEYRING_JWT_KEY, access_token)
            self._store.set_key(_KEYRING_REFRESH_KEY, refresh_token)
            if (
                self._optional_key(_KEYRING_JWT_KEY) != access_token
                or self._optional_key(_KEYRING_REFRESH_KEY) != refresh_token
            ):
                raise SecretStorageUnavailableError(
                    "Sync credential verification failed."
                )
            self._store.delete_key(_PENDING_KEY)
            if self._optional_key(_PENDING_KEY) is not None:
                raise SecretStorageUnavailableError(
                    "Sync credential publication failed."
                )
        except (SecretNotFoundError, SecretStorageUnavailableError):
            # Leave the pending marker if any part of cleanup fails. A fresh
            # client then refuses a mixed pair, even when both slots exist.
            try:
                self._delete_pair(self._store)
            except (SecretNotFoundError, SecretStorageUnavailableError):
                pass
            raise SecretStorageUnavailableError(
                "Sync credentials could not be safely stored. Unlock the OS "
                "keychain and run `moneybin sync login` again."
            ) from None

    @staticmethod
    def _delete_pair(store: SecretStore) -> None:
        for name in (_KEYRING_JWT_KEY, _KEYRING_REFRESH_KEY, _PENDING_KEY):
            try:
                store.delete_key(name)
            except SecretUnavailableError:
                raise
            except SecretNotFoundError:
                pass
            try:
                store.get_key(name)
            except SecretUnavailableError:
                raise
            except SecretNotFoundError:
                continue
            raise SecretStorageUnavailableError(
                "Sync credential removal could not be verified."
            )

    def _read_pair(self) -> tuple[str, str] | None:
        if self._optional_key(_PENDING_KEY) is not None:
            raise SecretStorageUnavailableError(
                "Sync credential storage was interrupted. Run `moneybin sync login` again."
            )
        access = self._optional_key(_KEYRING_JWT_KEY)
        refresh = self._optional_key(_KEYRING_REFRESH_KEY)
        if access is not None or refresh is not None:
            if not access or not refresh or not access.strip() or not refresh.strip():
                raise SecretStorageUnavailableError(
                    "Sync credential pair is incomplete. Run `moneybin sync login` again."
                )
            # A previous import may have stored both values but failed to remove
            # its plaintext source. Retry cleanup only when that exact pair matches.
            if self._profile_id is not None:
                path = self._legacy_token_path(self._profile_id)
                try:
                    payload = json.loads(path.read_text())
                except (OSError, UnicodeError, ValueError):
                    payload = None
                if isinstance(payload, dict) and (
                    cast(dict[str, object], payload).get("jwt") == access
                    and cast(dict[str, object], payload).get("refresh_token") == refresh
                ):
                    self._remove_legacy_file(path)
            return access, refresh
        if self._profile_id is None:
            return None
        path = self._legacy_token_path(self._profile_id)
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text())
            if not isinstance(payload, dict):
                raise ValueError
            values = cast(dict[str, object], payload)
            access = values.get("jwt")
            refresh = values.get("refresh_token")
            if (
                not isinstance(access, str)
                or not access.strip()
                or not isinstance(refresh, str)
                or not refresh.strip()
            ):
                raise ValueError
        except (OSError, UnicodeError, ValueError):
            raise SecretStorageUnavailableError(
                "Legacy sync credential file is unreadable or invalid; it was preserved. "
                "Run `moneybin sync login` again."
            ) from None
        self._store_tokens(access_token=access, refresh_token=refresh)
        self._remove_legacy_file(path)
        return access, refresh

    @staticmethod
    def _remove_legacy_file(path: Path) -> None:
        try:
            path.unlink()
        except OSError:
            raise SecretStorageUnavailableError(
                "Sync credentials were stored, but the legacy plaintext file "
                "could not be removed. Restore file permissions and retry."
            ) from None

    def _read_token(self) -> str | None:
        pair = self._read_pair()
        return pair[0] if pair else None

    def _read_refresh_token(self) -> str | None:
        pair = self._read_pair()
        return pair[1] if pair else None

    @staticmethod
    def _legacy_token_path(profile_id: str | None) -> Path:
        name = f".sync_token-{profile_id}" if profile_id else ".sync_token"
        return Path.home() / ".moneybin" / name

    def logout(self) -> None:
        """Remove local credentials; this does not revoke the broker session."""
        self._clear_tokens()

    @classmethod
    def clear_tokens_for_profile(cls, profile_id: str) -> None:
        """Remove only the named broker identity's credentials and legacy file."""
        cls._delete_pair(SecretStore.for_sync(profile_id))
        cls._legacy_token_path(profile_id).unlink(missing_ok=True)

    def _clear_tokens(self) -> None:
        self._delete_pair(self._store)
        self._legacy_token_path(self._profile_id).unlink(missing_ok=True)

    # ------------------------------ Login ------------------------------

    def begin_login(self) -> DeviceAuthorizationChallenge:
        """Begin RFC 8628 device authorization without polling or opening a browser."""
        try:
            code_resp = self._client.post("/auth/device/code")
        except httpx.RequestError as e:
            raise SyncAPIError(
                f"sync server unreachable at {self._server_url}: {e}"
            ) from e
        code_resp.raise_for_status()
        return DeviceAuthorizationChallenge.model_validate(code_resp.json())

    def poll_login(self, device_code: str) -> LoginPollResult:
        """Poll device authorization once and persist tokens only on completion."""
        token_body: dict[str, str] = {"device_code": device_code}
        if self._profile_id:
            token_body["profile_id"] = self._profile_id
        try:
            poll = self._client.post("/auth/device/token", json=token_body)
        except httpx.RequestError as e:
            raise SyncAPIError(
                f"sync server unreachable at {self._server_url}: {e}"
            ) from e
        if poll.status_code == 200:
            token = AuthToken.model_validate(poll.json())
            self._store_tokens(
                access_token=token.access_token,
                refresh_token=token.refresh_token,
            )
            return LoginPollResult(status="authenticated")
        if poll.status_code == 202:
            status = poll.json().get("status")
            if status in {"pending", "slow_down"}:
                return LoginPollResult(status=status)
            raise SyncAPIError(f"unexpected 202 status: {status}")
        if poll.status_code == 403:
            raise SyncAuthError("user denied device authorization")
        if poll.status_code == 400:
            raise SyncAuthError("device code expired or invalid; restart login")
        raise SyncAPIError(
            f"unexpected status {poll.status_code} from /auth/device/token"
        )

    def login(self, *, open_browser: bool = True) -> None:
        """Run the blocking CLI wrapper over the nonblocking device flow."""
        challenge = self.begin_login()
        uri = challenge.verification_uri_complete or challenge.verification_uri
        if uri is None:  # pragma: no cover — validated by the response model
            raise SyncAPIError("device authorization response omitted verification URI")
        print(f"To sign in, visit: {uri}", file=sys.stderr)  # noqa: T201
        print(f"Code: {challenge.user_code}", file=sys.stderr)  # noqa: T201
        if open_browser:
            try:
                webbrowser.open(uri)
            except webbrowser.Error:
                pass  # fall through; URL already printed

        interval = challenge.interval
        device_code = challenge.device_code.get_secret_value()
        while True:
            self._sleep(interval)
            result = self.poll_login(device_code)
            if result.status == "authenticated":
                return
            if result.status == "slow_down":
                interval += 5.0  # RFC 8628 §3.5

    # ------------------------------ Authed transport ------------------------------

    def _authed_request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, object] | None = None,
        params: dict[str, str] | None = None,
        timeout: httpx.Timeout = _DEFAULT_TIMEOUT,
    ) -> httpx.Response:
        token = self._read_token()
        if token is None:
            raise SyncAuthError("not authenticated — run `moneybin sync login`")
        headers = {"Authorization": f"Bearer {token}"}
        try:
            resp = self._client.request(
                method,
                path,
                json=json_body,
                params=params,
                headers=headers,
                timeout=timeout,
            )
            if resp.status_code == 401:
                self._refresh()  # raises SyncAuthError on failure
                token = self._read_token()
                headers["Authorization"] = f"Bearer {token}"
                resp = self._client.request(
                    method,
                    path,
                    json=json_body,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                )
                if resp.status_code == 401:
                    # Refresh succeeded but the retry still 401'd — token store
                    # drift, server-side revocation, or the refresh issued a
                    # token the resource server rejects. Treat as auth (run
                    # sync login), not generic API.
                    self._clear_tokens()
                    SYNC_AUTH_REFRESH_OUTCOMES.labels(outcome="second_401").inc()
                    raise SyncAuthError(
                        "session expired after refresh — run `moneybin sync login`"
                    )
        except httpx.RequestError as e:
            # Connection refused, DNS failure, timeout, etc. Wrap so
            # classify_user_error can surface a clean CLI/MCP message instead
            # of a raw httpx traceback.
            raise SyncAPIError(
                f"sync server unreachable at {self._server_url}: {e}"
            ) from e
        if resp.status_code >= 400:
            raise SyncAPIError(
                f"{method} {path} returned {resp.status_code}: {resp.text[:200]}"
            )
        return resp

    def _refresh(self) -> None:
        """Exchange refresh token for a new access token (rotating refresh tokens)."""
        refresh = self._read_refresh_token()
        if refresh is None:
            self._clear_tokens()
            raise SyncAuthError("no refresh token stored — run `moneybin sync login`")
        try:
            resp = self._client.post("/auth/refresh", json={"refresh_token": refresh})
        except httpx.RequestError as e:
            raise SyncAPIError(
                f"sync server unreachable at {self._server_url}: {e}"
            ) from e
        if resp.status_code != 200:
            self._clear_tokens()
            SYNC_AUTH_REFRESH_OUTCOMES.labels(outcome="failed").inc()
            raise SyncAuthError("session expired — run `moneybin sync login`")
        token = AuthToken.model_validate(resp.json())
        self._store_tokens(
            access_token=token.access_token,
            refresh_token=token.refresh_token,
        )
        SYNC_AUTH_REFRESH_OUTCOMES.labels(outcome="success").inc()

    # ------------------------------ Institutions ------------------------------

    def list_institutions(self) -> list[ConnectedInstitution]:
        """Return all connected institutions for the authenticated user."""
        resp = self._authed_request("GET", "/institutions")
        return [ConnectedInstitution.model_validate(item) for item in resp.json()]

    def disconnect(self, connection_id: str) -> None:
        """Remove a connected institution by its connection ID."""
        self._authed_request("DELETE", f"/institutions/{connection_id}")

    # ------------------------------ Link flow ------------------------------

    def initiate_link(
        self,
        *,
        provider: str = "plaid",
        provider_item_id: str | None = None,
        return_to: str | None = None,
    ) -> LinkInitiateResponse:
        """Start a Plaid Link session; returns session_id and hosted link_url."""
        body: dict[str, object] = {"provider": provider}
        if provider_item_id:
            body["provider_item_id"] = provider_item_id
        if return_to:
            body["return_to"] = return_to
        resp = self._authed_request("POST", "/sync/link/initiate", json_body=body)
        return LinkInitiateResponse.model_validate(resp.json())

    def get_link_status(self, session_id: str) -> LinkStatusResponse:
        """Single-shot GET /sync/link/status — returns whatever state the server holds.

        Used by CLI `sync link-status` and MCP `sync_link_status`; both are
        event-driven (caller decides when to check) rather than blocking. Use
        `poll_link_status` instead when the caller needs to block until a
        terminal state.
        """
        resp = self._authed_request(
            "GET",
            "/sync/link/status",
            params={"session_id": session_id},
        )
        return LinkStatusResponse.model_validate(resp.json())

    def poll_link_status(self, session_id: str) -> LinkStatusResponse:
        """Poll GET /sync/link/status until status reaches a terminal state.

        Terminal: 'linked' (returns) or 'failed' (raises SyncLinkError).
        Times out after _LINK_POLL_DEADLINE seconds → SyncTimeoutError.
        """
        deadline = time.time() + _LINK_POLL_DEADLINE
        while time.time() < deadline:
            self._sleep(_LINK_POLL_INTERVAL)
            resp = self._authed_request(
                "GET",
                "/sync/link/status",
                params={"session_id": session_id},
            )
            status = LinkStatusResponse.model_validate(resp.json())
            if status.status == "linked":
                return status
            if status.status == "failed":
                raise SyncLinkError(status.error or "link session failed")
            # status == "pending" → continue
        raise SyncTimeoutError(
            "link flow timed out — user may have abandoned the browser"
        )

    # ------------------------------ Sync trigger and data ------------------------------

    def trigger_sync(
        self,
        *,
        provider_item_id: str | None = None,
        reset_cursor: bool = False,
    ) -> SyncTriggerResponse:
        """POST /sync/trigger — synchronous. Blocks until sync completes server-side.

        Uses _LONG_TIMEOUT since multi-institution syncs can take 30-90s.
        """
        body: dict[str, object] = {}
        if provider_item_id:
            body["provider_item_id"] = provider_item_id
        if reset_cursor:
            body["reset_cursor"] = True
        resp = self._authed_request(
            "POST",
            "/sync/trigger",
            json_body=body,
            timeout=_LONG_TIMEOUT,
        )
        return SyncTriggerResponse.model_validate(resp.json())

    def get_data(self, job_id: str) -> SyncDataResponse:
        """GET /sync/data — re-readable until ack or the broker's hold TTL expires.

        Reading does not advance cursors or drop the window; the broker holds the
        sync's data until POST /sync/ack confirms a durable load (or the TTL lapses).
        """
        resp = self._authed_request(
            "GET",
            "/sync/data",
            params={"job_id": job_id},
        )
        return SyncDataResponse.model_validate(resp.json())

    def ack(self, job_id: str) -> SyncAckResponse:
        """POST /sync/ack — confirm durable load so the broker advances cursors.

        Idempotent: a re-ack of an already-acked job is a no-op 200. Until the
        client acks, the broker holds the sync's advanced cursors and re-serves
        the data, so a crash before ack yields a loss-free re-pull.
        """
        resp = self._authed_request("POST", "/sync/ack", json_body={"job_id": job_id})
        return SyncAckResponse.model_validate(resp.json())
