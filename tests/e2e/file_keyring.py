"""File-backed keyring for E2E tests that span multiple subprocesses.

The default ``MemoryKeyring`` lives in process memory, so each ``run_cli``
invocation gets a fresh empty store. That works when a single env var
provides the encryption key as fallback, but it can't exercise keychain
isolation across subprocesses.

``FileKeyring`` writes credentials to ``$MONEYBIN_KEYRING_FILE`` (a JSON
file). Set the env var to a per-test path so isolation is preserved while
keys persist across subprocess boundaries.

Usage in tests::

    env = {
        "PYTHON_KEYRING_BACKEND": "tests.e2e.file_keyring.FileKeyring",
        "MONEYBIN_KEYRING_FILE": str(tmp_path / "keyring.json"),
        ...
    }
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from keyring.backend import KeyringBackend


class FileKeyring(KeyringBackend):
    """JSON-file-backed keyring scoped by ``MONEYBIN_KEYRING_FILE``."""

    priority = 1  # type: ignore[assignment]  # keyring uses class-level priority

    def _path(self) -> Path:
        path = os.environ.get("MONEYBIN_KEYRING_FILE")
        if not path:
            raise RuntimeError(
                "FileKeyring requires MONEYBIN_KEYRING_FILE env var to be set"
            )
        return Path(path)

    def _load(self) -> dict[str, str]:
        path = self._path()
        if not path.exists():
            return {}
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}

    def _save(self, store: dict[str, str]) -> None:
        path = self._path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(store, f)

    @staticmethod
    def _key(service: str, username: str) -> str:
        return f"{service}\x00{username}"

    def set_password(self, service: str, username: str, password: str) -> None:
        store = self._load()
        store[self._key(service, username)] = password
        self._save(store)

    def get_password(self, service: str, username: str) -> str | None:
        return self._load().get(self._key(service, username))

    def delete_password(self, service: str, username: str) -> None:
        store = self._load()
        try:
            del store[self._key(service, username)]
        except KeyError:
            from keyring.errors import PasswordDeleteError

            raise PasswordDeleteError(f"No password for {service}/{username}") from None
        self._save(store)
