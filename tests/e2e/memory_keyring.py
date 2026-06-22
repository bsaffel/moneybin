"""In-memory keyring backend for E2E subprocess tests.

Unlike the null backend (which silently drops writes), this backend
stores credentials in a module-level dict so set_password/get_password
round-trips work within the same process.  Each E2E subprocess gets its
own fresh dict, providing test isolation for free.

Usage: set PYTHON_KEYRING_BACKEND=tests.e2e.memory_keyring.MemoryKeyring
"""

from keyring.backend import KeyringBackend


class MemoryKeyring(KeyringBackend):
    """Dict-backed keyring that persists for the lifetime of the process."""

    priority = 1  # type: ignore[assignment]  # keyring uses class-level priority

    _store: dict[tuple[str, str], str] = {}

    def set_password(self, service: str, username: str, password: str) -> None:
        MemoryKeyring._store[(service, username)] = password

    def get_password(self, service: str, username: str) -> str | None:
        return MemoryKeyring._store.get((service, username))

    def delete_password(self, service: str, username: str) -> None:
        try:
            del MemoryKeyring._store[(service, username)]
        except KeyError:
            from keyring.errors import PasswordDeleteError

            raise PasswordDeleteError(f"No password for {service}/{username}") from None
