"""Centralized secret management for MoneyBin.

SecretStore is the sole module that imports ``keyring``. All other modules
access secrets through this interface — the Database class for encryption
keys, CLI commands for key lifecycle, MoneyBinSettings for sensitive config.

SecretStore does NOT cache, derive, rotate, or orchestrate secret lifecycle.
Passphrase derivation (Argon2id) and rotation sequencing live in the CLI
commands that call set_key() / delete_key().
"""

import logging
import os

import keyring
from keyring.errors import (
    KeyringError,
    NoKeyringError,
    PasswordDeleteError,
)

logger = logging.getLogger(__name__)

_SERVICE_PREFIX = "moneybin"
_ENV_PREFIX = "MONEYBIN_"

# Google Sheets connector OAuth secret keys.
GSHEET_REFRESH_TOKEN_KEY = "gsheet:refresh_token"  # noqa: S105  # keyring lookup name, not a secret value
GSHEET_ACCESS_TOKEN_KEY = "gsheet:access_token"  # noqa: S105  # keyring lookup name, not a secret value
GSHEET_ACCESS_TOKEN_EXPIRES_KEY = "gsheet:access_token_expires_at"  # noqa: S105  # keyring lookup name, not a secret value
GSHEET_WRITE_REFRESH_TOKEN_KEY = "gsheet:write_refresh_token"  # noqa: S105  # keyring lookup name, not a secret value
GSHEET_WRITE_ACCESS_TOKEN_KEY = "gsheet:write_access_token"  # noqa: S105  # keyring lookup name, not a secret value
GSHEET_WRITE_ACCESS_TOKEN_EXPIRES_KEY = "gsheet:write_access_token_expires_at"  # noqa: S105  # keyring lookup name, not a secret value

# Market-data price feeds. Only Tiingo needs a credential — CoinGecko's keyless
# tier serves the endpoint this codebase uses.
TIINGO_API_TOKEN_KEY = "tiingo:api_token"  # noqa: S105  # keyring lookup name, not a secret value


def _resolve_profile(profile: str | None) -> str | None:
    from moneybin.config import get_current_profile
    from moneybin.utils.user_config import normalize_profile_name

    if profile is None:
        try:
            profile = get_current_profile()
        except RuntimeError:
            return None
    return normalize_profile_name(profile)


class SecretNotFoundError(Exception):
    """Raised when a secret cannot be found in keychain or environment."""


class SecretUnavailableError(SecretNotFoundError):
    """Raised when the OS keychain reports a read as denied, not missing.

    Subclasses ``SecretNotFoundError`` so every existing ``except
    SecretNotFoundError`` call site keeps working unchanged — this only lets
    a caller that wants to react more precisely catch it first. Most
    platforms can't actually raise this: macOS reports a sandbox-denied read
    identically to a missing item (``errSecItemNotFound``), so this fires
    only where the keyring backend itself distinguishes the two (e.g. a
    locked Linux secret service, or an explicit macOS keychain ACL denial).
    """


class SecretStorageUnavailableError(Exception):
    """Raised when a keychain write or deletion cannot be completed safely.

    Distinct from ``SecretNotFoundError`` — read paths can fall back to env
    vars, but writes have nowhere to go and must surface a clear error
    rather than silently losing the value.
    """


class SecretStore:
    """Keychain and environment variable interface for secrets.

    Three operations for keychain-backed secrets (encryption keys, E2E keys):
    - get_key(name): keychain → env var → SecretNotFoundError
    - set_key(name, value): write to keychain
    - delete_key(name): clear from keychain

    One operation for env-var-only secrets (API keys, server credentials):
    - get_env(name): env var → SecretNotFoundError

    Each ``SecretStore`` is scoped to a single profile — its keychain entries
    live under ``service="moneybin-<profile>"``. Passing ``profile=None``
    falls back to the current profile (set via ``set_current_profile``).
    """

    def __init__(self, profile: str | None = None) -> None:
        """Capture one normalized profile for keychain and environment access."""
        self._profile = _resolve_profile(profile)
        self._service = (
            f"{_SERVICE_PREFIX}-{self._profile}" if self._profile else _SERVICE_PREFIX
        )
        self._username_prefix = ""
        self._allow_env_fallback = True

    @property
    def profile(self) -> str | None:
        """The normalized profile captured when this store was constructed."""
        return self._profile

    @classmethod
    def for_sync(cls, profile_id: str | None) -> "SecretStore":
        """Preserve broker credential addresses without environment fallback."""
        if profile_id is not None and (
            not profile_id or not profile_id.isascii() or not profile_id.isalnum()
        ):
            raise ValueError("Invalid sync profile ID")
        # Broker identity does not depend on a resolved database profile.
        store = cls.__new__(cls)
        store._profile = profile_id
        store._service = "moneybin-sync"
        store._username_prefix = f"{profile_id}:" if profile_id else ""
        store._allow_env_fallback = False
        return store

    def env_var_name(self, name: str) -> str:
        """Name the environment fallback for this captured profile."""
        if self._profile:
            profile = self._profile.upper().replace("-", "_")
            return f"{_ENV_PREFIX}PROFILE__{profile}__{name}"
        return f"{_ENV_PREFIX}{name}"

    def get_key(self, name: str) -> str:
        """Retrieve from keychain, then this profile's environment fallback."""
        denied = False
        try:
            value = keyring.get_password(self._service, self._username_prefix + name)
        except NoKeyringError:
            if not self._allow_env_fallback:
                raise SecretStorageUnavailableError(
                    "Sync requires an available secure OS keychain."
                ) from None
            value = None
        except KeyringError:
            value = None
            denied = True
        if value is not None:
            return value

        env_var = self.env_var_name(name)
        if self._allow_env_fallback:
            value = os.environ.get(env_var)
            if value is not None:
                return value
        if denied:
            guidance = (
                f" Set env var {env_var} to bypass the keychain."
                if self._allow_env_fallback
                else " Unlock the OS keychain and retry."
            )
            raise SecretUnavailableError(
                f"Secret '{name}' could not be read — the OS keychain denied "
                f"access or is unavailable; existence could not be determined.{guidance}"
            ) from None
        guidance = (
            f" Set it via OS keychain (moneybin db init) or env var {env_var}."
            if self._allow_env_fallback
            else " Run `moneybin sync login`."
        )
        raise SecretNotFoundError(f"Secret '{name}' not found.{guidance}")

    def get_env(self, name: str) -> str:
        """Retrieve a configuration secret from its global environment name."""
        env_var = f"{_ENV_PREFIX}{name}"
        value = os.environ.get(env_var)
        if value is not None:
            return value
        raise SecretNotFoundError(f"Secret '{name}' not found. Set env var {env_var}.")

    def has_keychain_entry(self, name: str) -> bool:
        """Check keychain presence without consulting environment fallback."""
        try:
            return (
                keyring.get_password(self._service, self._username_prefix + name)
                is not None
            )
        except NoKeyringError:
            return False
        except KeyringError:
            raise SecretUnavailableError(
                f"Secret '{name}' could not be checked — the OS keychain denied "
                "access or is unavailable; existence could not be determined."
            ) from None

    def set_key(self, name: str, value: str) -> None:
        """Store a secret in the OS keychain, never a fallback file."""
        try:
            keyring.set_password(self._service, self._username_prefix + name, value)
        except KeyringError:
            guidance = (
                f" Or supply the value via env var {self.env_var_name(name)}."
                if self._allow_env_fallback
                else " Sync login and refresh require writable secure storage."
            )
            raise SecretStorageUnavailableError(
                f"Unable to store secret '{name}' in the OS keychain. "
                f"Configure or unlock a secure OS keychain and retry.{guidance}"
            ) from None
        logger.debug(f"Stored secret '{name}' in OS keychain")

    def delete_key(self, name: str) -> None:
        """Clear a keychain entry; refusal is distinct from absence."""
        try:
            keyring.delete_password(self._service, self._username_prefix + name)
        except PasswordDeleteError:
            # Backends may use this error for deletion failures as well as misses.
            # Only report absence when a fresh read confirms it.
            try:
                present = self.has_keychain_entry(name)
            except SecretUnavailableError:
                raise SecretStorageUnavailableError(
                    f"Secret '{name}' removal could not be verified."
                ) from None
            if present:
                raise SecretStorageUnavailableError(
                    f"Secret '{name}' could not be removed from the OS keychain."
                ) from None
            raise SecretNotFoundError(
                f"Secret '{name}' not found in keychain."
            ) from None
        except NoKeyringError:
            if not self._allow_env_fallback:
                raise SecretStorageUnavailableError(
                    "Sync credentials could not be removed; configure the OS keychain."
                ) from None
            raise SecretNotFoundError(
                f"Secret '{name}' not found (no keyring backend available)."
            ) from None
        except KeyringError:
            raise SecretStorageUnavailableError(
                f"Secret '{name}' could not be removed — the OS keychain denied access."
            ) from None
        logger.debug(f"Removed secret '{name}' from OS keychain")
