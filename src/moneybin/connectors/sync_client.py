"""HTTP client for moneybin-sync.

Pure transport — no business logic, no database access. Methods correspond
1:1 to server endpoints. Service-layer orchestration lives in SyncService.

Token storage:
- Primary: OS keyring via `keyring` library
- Fallback: ~/.moneybin/.sync_token (0600), JSON {"jwt": ..., "refresh_token": ...}
The fallback handles environments without an OS keychain (headless Linux without
Secret Service, some Docker setups).

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
import os
import stat
import sys
import time
import webbrowser
from datetime import date
from pathlib import Path
from typing import Any, TypeVar, cast
from uuid import UUID

import httpx
import keyring
from keyring.errors import KeyringError
from pydantic import BaseModel, ValidationError

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
    SyncAccountScope,
    SyncAckV2Response,
    SyncDisconnectResponse,
    SyncEnvelopeV2,
    SyncStatusV2Response,
    SyncTriggerV2Response,
)
from moneybin.metrics.registry import SYNC_AUTH_REFRESH_OUTCOMES

ModelT = TypeVar("ModelT", bound=BaseModel)

logger = logging.getLogger(__name__)

_KEYRING_SERVICE = "moneybin-sync"
_KEYRING_JWT_KEY = "jwt"
_KEYRING_REFRESH_KEY = "refresh_token"

_DEFAULT_TIMEOUT = httpx.Timeout(15.0, connect=10.0)
_LONG_TIMEOUT = httpx.Timeout(120.0, connect=10.0)
_LINK_POLL_INTERVAL = 3.0
# How long the CLI waits for the user to finish the browser link flow before
# giving up. Decoupled from the HTTP timeouts above: completing a real bank's
# OAuth + MFA can legitimately take minutes, far longer than any single request.
_LINK_POLL_DEADLINE = 300.0


class SyncClient:
    """HTTP client wrapping moneybin-sync endpoints.

    Construction:
        SyncClient(server_url, token_path=None)
    For tests, pass an explicit `token_path` to use a tmp file (bypasses keyring).
    """

    # Test hook — overridable for fast tests (e.g. client._sleep = list.append)
    _sleep = staticmethod(time.sleep)

    def __init__(
        self,
        server_url: str,
        token_path: Path | None = None,
        profile_id: str | None = None,
    ) -> None:
        """Set up the HTTP client and optional test-only token path override."""
        self._server_url = server_url.rstrip("/")
        self._token_path = token_path  # if set, bypass keyring entirely (tests)
        self._profile_id = profile_id
        # Namespace token storage by profile so two profiles never share a slot —
        # each profile's token encodes a distinct broker subject. Absent a
        # profile_id, fall back to the legacy unscoped keys.
        self._jwt_key = (
            f"{profile_id}:{_KEYRING_JWT_KEY}" if profile_id else _KEYRING_JWT_KEY
        )
        self._refresh_key = (
            f"{profile_id}:{_KEYRING_REFRESH_KEY}"
            if profile_id
            else _KEYRING_REFRESH_KEY
        )
        self._client = httpx.Client(base_url=self._server_url, timeout=_DEFAULT_TIMEOUT)

    # ------------------------------ Token storage ------------------------------

    def _store_tokens(self, *, access_token: str, refresh_token: str) -> None:
        if self._token_path is not None:
            self._write_token_file(access_token, refresh_token)
            return
        try:
            keyring.set_password(_KEYRING_SERVICE, self._jwt_key, access_token)
            keyring.set_password(_KEYRING_SERVICE, self._refresh_key, refresh_token)
        except KeyringError as e:
            logger.warning(f"Keyring unavailable ({e}); falling back to file storage.")
            self._write_token_file(access_token, refresh_token)

    def _read_token(self) -> str | None:
        if self._token_path is not None:
            return self._read_token_file().get("jwt")
        try:
            return keyring.get_password(_KEYRING_SERVICE, self._jwt_key)
        except KeyringError:
            return self._read_token_file().get("jwt")

    def _read_refresh_token(self) -> str | None:
        if self._token_path is not None:
            return self._read_token_file().get("refresh_token")
        try:
            return keyring.get_password(_KEYRING_SERVICE, self._refresh_key)
        except KeyringError:
            return self._read_token_file().get("refresh_token")

    def logout(self) -> None:
        """Remove stored tokens from keychain (or fallback file)."""
        self._clear_tokens()

    @classmethod
    def clear_tokens_for_profile(cls, profile_id: str) -> None:
        """Delete a single profile's scoped broker tokens (keyring + fallback file).

        For profile deletion: removes only that profile's scoped slots — never the
        legacy unscoped slots (which may belong to another or legacy identity) or
        another profile's slots. Best-effort; missing entries are ignored.
        """
        for key in (
            f"{profile_id}:{_KEYRING_JWT_KEY}",
            f"{profile_id}:{_KEYRING_REFRESH_KEY}",
        ):
            try:
                keyring.delete_password(_KEYRING_SERVICE, key)
            except KeyringError:
                pass
        fallback = Path.home() / ".moneybin" / f".sync_token-{profile_id}"
        if fallback.exists():
            fallback.unlink()

    def _clear_tokens(self) -> None:
        if self._token_path is not None:
            if self._token_path.exists():
                self._token_path.unlink()
            return
        # Clear this profile's scoped slots AND the legacy unscoped slots, so a
        # user upgrading from the pre-per-profile version doesn't leave an
        # orphaned token behind on logout. The set dedups when no profile is set
        # (scoped keys == legacy keys).
        for key in {
            self._jwt_key,
            self._refresh_key,
            _KEYRING_JWT_KEY,
            _KEYRING_REFRESH_KEY,
        }:
            try:
                keyring.delete_password(_KEYRING_SERVICE, key)
            except KeyringError:
                pass
        for path in {
            self._effective_token_path(),
            Path.home() / ".moneybin" / ".sync_token",
        }:
            if path.exists():
                path.unlink()

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
        name = f".sync_token-{self._profile_id}" if self._profile_id else ".sync_token"
        return Path.home() / ".moneybin" / name

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
        except httpx.RequestError:
            # Connection refused, DNS failure, timeout, etc. Wrap so
            # classify_user_error can surface a clean CLI/MCP message instead
            # of a raw httpx traceback.
            raise SyncAPIError(
                "Sync broker could not be reached; retry this operation"
            ) from None
        if resp.status_code >= 400:
            raise SyncAPIError(
                f"Sync broker request failed (HTTP {resp.status_code})",
                status_code=resp.status_code,
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
        resp = self._authed_request(
            "GET",
            "/institutions",
            params={"contract_version": "2", "consumer_id": self.consumer_id},
        )
        try:
            return [ConnectedInstitution.model_validate(item) for item in resp.json()]
        except (ValidationError, ValueError):
            raise SyncAPIError("Invalid connection listing from broker") from None

    def disconnect(
        self,
        connection_id: str,
        *,
        credential_generation: int,
        accounts: list[SyncAccountScope],
    ) -> SyncDisconnectResponse:
        """Remove precisely the confirmed generation and native account identity set."""
        try:
            response = self._authed_request(
                "DELETE",
                f"/connections/{connection_id}",
                params={"consumer_id": self.consumer_id},
                json_body={
                    "credential_generation": credential_generation,
                    "accounts": [
                        account.model_dump(mode="json", by_alias=True)
                        for account in accounts
                    ],
                },
            )
        except SyncAPIError as error:
            if error.status_code == 409:
                raise SyncAPIError(
                    "Disconnect refused; inspect current sync status and reconfirm this grant",
                    status_code=409,
                ) from None
            raise
        try:
            result = SyncDisconnectResponse.model_validate(response.json())
            if (
                result.connection_id != UUID(connection_id)
                or result.credential_generation != credential_generation
                or {account.identity for account in result.accounts}
                != {account.identity for account in accounts}
            ):
                raise ValueError("Unexpected removed grant")
            return result
        except (ValidationError, ValueError):
            raise SyncAPIError("Invalid disconnect response from broker") from None

    # ------------------------------ Link flow ------------------------------

    def initiate_link(
        self,
        *,
        provider: str = "plaid",
        provider_item_id: str | None = None,
        connection_id: str | None = None,
        return_to: str | None = None,
    ) -> LinkInitiateResponse:
        """Start a Plaid Link session; returns session_id and hosted link_url."""
        body: dict[str, object] = {"provider": provider}
        if provider == "simplefin":
            body["consumer_id"] = self.consumer_id
            if connection_id is not None:
                body["connection_id"] = connection_id
        if provider_item_id:
            body["provider_item_id"] = provider_item_id
        if return_to:
            body["return_to"] = return_to
        resp = self._authed_request("POST", "/sync/link/initiate", json_body=body)
        try:
            return LinkInitiateResponse.model_validate(resp.json())
        except (ValidationError, ValueError):
            raise SyncAPIError("Invalid link initiation response from broker") from None

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
        try:
            status = LinkStatusResponse.model_validate(resp.json())
        except (ValidationError, ValueError):
            raise SyncAPIError("Invalid link status from broker") from None
        if status.error not in {
            None,
            "SESSION_EXPIRED",
            "EXCHANGE_FAILED",
            "claim_rejected",
            "claim_outcome_unknown",
            "payment_required",
            "rate_limited",
            "authorization_revoked",
        }:
            status.error = (
                "Link failed; inspect connection status before starting a new link"
            )
        return status

    def poll_link_status(self, session_id: str) -> LinkStatusResponse:
        """Poll GET /sync/link/status until status reaches a terminal state.

        Terminal: 'linked' (returns) or 'failed' (raises SyncLinkError).
        Times out after _LINK_POLL_DEADLINE seconds → SyncTimeoutError.
        """
        deadline = time.time() + _LINK_POLL_DEADLINE
        while time.time() < deadline:
            self._sleep(_LINK_POLL_INTERVAL)
            status = self.get_link_status(session_id)
            if status.status in {"linked", "claim_outcome_unknown"}:
                return status
            if status.status == "failed":
                raise SyncLinkError(status.error or "link session failed")
            # status == "pending" → continue
        raise SyncTimeoutError(
            "link flow timed out — user may have abandoned the browser"
        )

    # ------------------------------ Sync trigger and data ------------------------------

    @property
    def consumer_id(self) -> str:
        """Use the profile's persistent identity without exposing it as a user selector."""
        from moneybin.config import get_settings
        from moneybin.utils.user_config import get_or_create_profile_id

        return self._profile_id or get_or_create_profile_id(get_settings().profile_dir)

    def _v2_response(
        self,
        response: httpx.Response,
        model: type[ModelT],
        *,
        job_id: str | None = None,
    ) -> ModelT:
        try:
            body = response.json()
            if not isinstance(body, dict):
                raise SyncAPIError(
                    "Sync broker upgrade required for contract version 2"
                )
            body = cast(dict[str, Any], body)
            if body.get("contract_version") != 2:
                raise SyncAPIError(
                    "Sync broker upgrade required for contract version 2"
                )
            result = model.model_validate(body)
            if job_id is not None and body.get("job_id") != str(UUID(job_id)):
                raise SyncAPIError("Sync broker returned mismatched job evidence")
            return result
        except (ValidationError, ValueError):
            raise SyncAPIError("Sync broker returned invalid job evidence") from None

    def trigger_sync(
        self,
        *,
        provider_item_id: str | None = None,
        reset_cursor: bool = False,
        provider: str | None = None,
        connection_id: str | None = None,
        history_start: date | None = None,
    ) -> SyncTriggerV2Response:
        """Request a bounded v2 job under the internal consumer identity."""
        body: dict[str, object] = {
            "contract_version": 2,
            "consumer_id": self.consumer_id,
        }
        if provider_item_id is not None:
            body["provider_item_id"] = provider_item_id
        if provider is not None:
            body["provider"] = provider
        if connection_id is not None:
            body["connection_id"] = connection_id
        if history_start is not None:
            body["history_start"] = history_start.isoformat()
        if reset_cursor:
            body["force"] = True
        return self._v2_response(
            self._authed_request(
                "POST",
                "/sync/trigger",
                json_body=body,
                timeout=_LONG_TIMEOUT,
            ),
            SyncTriggerV2Response,
        )

    def get_data(self, job_id: str) -> SyncEnvelopeV2:
        """Read a held v2 envelope without advancing any checkpoint."""
        return self._v2_response(
            self._authed_request(
                "GET",
                "/sync/data",
                params={"job_id": job_id, "consumer_id": self.consumer_id},
            ),
            SyncEnvelopeV2,
            job_id=job_id,
        )

    def get_status(self, job_id: str) -> SyncStatusV2Response:
        """Read durable authenticated metadata, including after held payload expiry."""
        return self._v2_response(
            self._authed_request(
                "GET",
                "/sync/status",
                params={"job_id": job_id, "consumer_id": self.consumer_id},
            ),
            SyncStatusV2Response,
            job_id=job_id,
        )

    def ack(self, job_id: str) -> SyncAckV2Response:
        """Acknowledge a durable job and receive its stored post-ack evidence."""
        return self._v2_response(
            self._authed_request(
                "POST",
                "/sync/ack",
                json_body={"job_id": job_id, "consumer_id": self.consumer_id},
            ),
            SyncAckV2Response,
            job_id=job_id,
        )
