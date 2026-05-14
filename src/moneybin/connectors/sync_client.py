"""HTTP client for moneybin-server.

Pure transport — no business logic, no database access. Methods correspond
1:1 to server endpoints. Service-layer orchestration lives in SyncService.

Token storage:
- Primary: OS keyring via `keyring` library
- Fallback: ~/.moneybin/.sync_token (0600), JSON {"jwt": ..., "refresh_token": ...}
The fallback handles environments without an OS keychain (headless Linux without
Secret Service, some Docker setups).

Timeouts:
- _DEFAULT_TIMEOUT (15s) for most endpoints
- _LONG_TIMEOUT (120s) for POST /sync/trigger and connect polling deadline
Per design — no per-endpoint configuration knobs unless evidence demands them.
"""

from __future__ import annotations

import json
import logging
import os
import stat
import sys
import time
import webbrowser
from pathlib import Path

import httpx
import keyring
from keyring.errors import KeyringError

from moneybin.connectors.sync_errors import (
    SyncAPIError,
    SyncAuthError,
    SyncConnectError,
    SyncTimeoutError,
)
from moneybin.connectors.sync_models import (
    AuthToken,
    ConnectedInstitution,
    ConnectInitiateResponse,
    ConnectStatusResponse,
    SyncDataResponse,
    SyncTriggerResponse,
)
from moneybin.metrics.registry import SYNC_AUTH_REFRESH_OUTCOMES

logger = logging.getLogger(__name__)

_KEYRING_SERVICE = "moneybin-sync"
_KEYRING_JWT_KEY = "jwt"
_KEYRING_REFRESH_KEY = "refresh_token"

_DEFAULT_TIMEOUT = httpx.Timeout(15.0, connect=10.0)
_LONG_TIMEOUT = httpx.Timeout(120.0, connect=10.0)
_CONNECT_POLL_INTERVAL = 3.0


class SyncClient:
    """HTTP client wrapping moneybin-server endpoints.

    Construction:
        SyncClient(server_url, token_path=None)
    For tests, pass an explicit `token_path` to use a tmp file (bypasses keyring).
    """

    # Test hook — overridable for fast tests (e.g. client._sleep = list.append)
    _sleep = staticmethod(time.sleep)

    def __init__(self, server_url: str, token_path: Path | None = None) -> None:
        """Set up the HTTP client and optional test-only token path override."""
        self._server_url = server_url.rstrip("/")
        self._token_path = token_path  # if set, bypass keyring entirely (tests)
        self._client = httpx.Client(base_url=self._server_url, timeout=_DEFAULT_TIMEOUT)

    # ------------------------------ Token storage ------------------------------

    def _store_tokens(self, *, access_token: str, refresh_token: str) -> None:
        if self._token_path is not None:
            self._write_token_file(access_token, refresh_token)
            return
        try:
            keyring.set_password(_KEYRING_SERVICE, _KEYRING_JWT_KEY, access_token)
            keyring.set_password(_KEYRING_SERVICE, _KEYRING_REFRESH_KEY, refresh_token)
        except KeyringError as e:
            logger.warning(f"Keyring unavailable ({e}); falling back to file storage.")
            self._write_token_file(access_token, refresh_token)

    def _read_token(self) -> str | None:
        if self._token_path is not None:
            return self._read_token_file().get("jwt")
        try:
            return keyring.get_password(_KEYRING_SERVICE, _KEYRING_JWT_KEY)
        except KeyringError:
            return self._read_token_file().get("jwt")

    def _read_refresh_token(self) -> str | None:
        if self._token_path is not None:
            return self._read_token_file().get("refresh_token")
        try:
            return keyring.get_password(_KEYRING_SERVICE, _KEYRING_REFRESH_KEY)
        except KeyringError:
            return self._read_token_file().get("refresh_token")

    def logout(self) -> None:
        """Remove stored tokens from keychain (or fallback file)."""
        self._clear_tokens()

    def _clear_tokens(self) -> None:
        if self._token_path is not None:
            if self._token_path.exists():
                self._token_path.unlink()
            return
        try:
            keyring.delete_password(_KEYRING_SERVICE, _KEYRING_JWT_KEY)
        except KeyringError:
            pass
        try:
            keyring.delete_password(_KEYRING_SERVICE, _KEYRING_REFRESH_KEY)
        except KeyringError:
            pass
        fallback = Path.home() / ".moneybin" / ".sync_token"
        if fallback.exists():
            fallback.unlink()

    def _write_token_file(
        self,
        access_token: str,
        refresh_token: str,
    ) -> None:
        path = self._effective_token_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        # Tighten parent dir to 0o700 — mkdir respects umask, which on many
        # systems leaves the directory traversable by group/other.
        os.chmod(path.parent, stat.S_IRWXU)  # 0o700
        payload = json.dumps({"jwt": access_token, "refresh_token": refresh_token})
        # Atomic create with 0o600 permissions — never world-readable, even for
        # the brief window between create and chmod. The JWT + refresh token
        # gate every connected bank's data; the cost of getting this right is
        # small relative to the blast radius.
        flags = os.O_CREAT | os.O_WRONLY | os.O_TRUNC
        fd = os.open(path, flags, stat.S_IRUSR | stat.S_IWUSR)  # 0o600
        with os.fdopen(fd, "w") as f:
            f.write(payload)

    def _read_token_file(self) -> dict[str, str]:
        path = self._effective_token_path()
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError:
            return {}

    def _effective_token_path(self) -> Path:
        if self._token_path is not None:
            return self._token_path
        return Path.home() / ".moneybin" / ".sync_token"

    # ------------------------------ Login ------------------------------

    def login(self, *, open_browser: bool = True) -> None:
        """Device Authorization Flow (RFC 8628).

        Displays user_code + verification URL on stderr, optionally opens the
        browser, then polls for the access token. Stores token + refresh token.
        """
        try:
            code_resp = self._client.post("/auth/device/code")
        except httpx.RequestError as e:
            raise SyncAPIError(
                f"sync server unreachable at {self._server_url}: {e}"
            ) from e
        code_resp.raise_for_status()
        code_data = code_resp.json()
        user_code = code_data["user_code"]
        uri = code_data["verification_uri_complete"]
        interval = float(code_data.get("interval", 5))
        device_code = code_data["device_code"]

        print(f"To sign in, visit: {uri}", file=sys.stderr)  # noqa: T201
        print(f"Code: {user_code}", file=sys.stderr)  # noqa: T201
        if open_browser:
            try:
                webbrowser.open(uri)
            except webbrowser.Error:
                pass  # fall through; URL already printed

        while True:
            self._sleep(interval)
            try:
                poll = self._client.post(
                    "/auth/device/token", json={"device_code": device_code}
                )
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
                return
            if poll.status_code == 202:
                status = poll.json().get("status")
                if status == "slow_down":
                    interval += 5.0  # RFC 8628 §3.5
                    continue
                if status == "pending":
                    continue
                raise SyncAPIError(f"unexpected 202 status: {status}")
            if poll.status_code == 403:
                raise SyncAuthError("user denied device authorization")
            if poll.status_code == 400:
                raise SyncAuthError("device code expired or invalid; restart login")
            raise SyncAPIError(
                f"unexpected status {poll.status_code} from /auth/device/token"
            )

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

    # ------------------------------ Connect flow ------------------------------

    def initiate_connect(
        self,
        *,
        provider: str = "plaid",
        provider_item_id: str | None = None,
        return_to: str | None = None,
    ) -> ConnectInitiateResponse:
        """Start a Plaid Link session; returns session_id and hosted link_url."""
        body: dict[str, object] = {"provider": provider}
        if provider_item_id:
            body["provider_item_id"] = provider_item_id
        if return_to:
            body["return_to"] = return_to
        resp = self._authed_request("POST", "/sync/connect/initiate", json_body=body)
        return ConnectInitiateResponse.model_validate(resp.json())

    def get_connect_status(self, session_id: str) -> ConnectStatusResponse:
        """Single-shot GET /sync/connect/status — returns whatever state the server holds.

        Used by CLI `sync connect-status` and MCP `sync_connect_status`; both are
        event-driven (caller decides when to check) rather than blocking. Use
        `poll_connect_status` instead when the caller needs to block until a
        terminal state.
        """
        resp = self._authed_request(
            "GET",
            "/sync/connect/status",
            params={"session_id": session_id},
        )
        return ConnectStatusResponse.model_validate(resp.json())

    def poll_connect_status(self, session_id: str) -> ConnectStatusResponse:
        """Poll GET /sync/connect/status until status reaches a terminal state.

        Terminal: 'connected' (returns) or 'failed' (raises SyncConnectError).
        Times out after _LONG_TIMEOUT seconds → SyncTimeoutError.
        """
        read_timeout: float = _LONG_TIMEOUT.read or 120.0
        deadline = time.time() + read_timeout
        while time.time() < deadline:
            self._sleep(_CONNECT_POLL_INTERVAL)
            resp = self._authed_request(
                "GET",
                "/sync/connect/status",
                params={"session_id": session_id},
            )
            status = ConnectStatusResponse.model_validate(resp.json())
            if status.status == "connected":
                return status
            if status.status == "failed":
                raise SyncConnectError(status.error or "connect session failed")
            # status == "pending" → continue
        raise SyncTimeoutError(
            "connect flow timed out — user may have abandoned the browser"
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
        """GET /sync/data — one-shot read; server deletes from TTL store after."""
        resp = self._authed_request(
            "GET",
            "/sync/data",
            params={"job_id": job_id},
        )
        return SyncDataResponse.model_validate(resp.json())
