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
from pathlib import Path

import httpx
import keyring
from keyring.errors import KeyringError

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
            self._write_token_file(access_token, refresh_token, fallback_path=True)

    def _read_token(self) -> str | None:
        if self._token_path is not None:
            return self._read_token_file().get("jwt")
        try:
            return keyring.get_password(_KEYRING_SERVICE, _KEYRING_JWT_KEY)
        except KeyringError:
            return self._read_token_file(fallback_path=True).get("jwt")

    def _read_refresh_token(self) -> str | None:
        if self._token_path is not None:
            return self._read_token_file().get("refresh_token")
        try:
            return keyring.get_password(_KEYRING_SERVICE, _KEYRING_REFRESH_KEY)
        except KeyringError:
            return self._read_token_file(fallback_path=True).get("refresh_token")

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
        *,
        fallback_path: bool = False,
    ) -> None:
        path = self._effective_token_path(fallback_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps({"jwt": access_token, "refresh_token": refresh_token})
        path.write_text(payload)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)  # 0600

    def _read_token_file(self, *, fallback_path: bool = False) -> dict[str, str]:
        path = self._effective_token_path(fallback_path)
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError:
            return {}

    def _effective_token_path(self, fallback_path: bool) -> Path:  # noqa: ARG002
        if self._token_path is not None:
            return self._token_path
        return Path.home() / ".moneybin" / ".sync_token"
