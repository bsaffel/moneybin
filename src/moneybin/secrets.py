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
import keyring.errors

logger = logging.getLogger(__name__)

_SERVICE_PREFIX = "moneybin"
_ENV_PREFIX = "MONEYBIN_"


def _resolve_service_name(profile: str | None) -> str:
    """Resolve the keychain service name for a profile.

    Each profile gets its own keychain entry under
    ``service="moneybin-<profile>"`` so that creating, deleting, or rotating
    one profile's key cannot clobber another's. When no profile is supplied,
    falls back to the current profile, then to the legacy ``"moneybin"``
    service for back-compat with pre-scoping tests.
    """
    if profile is None:
        from moneybin.config import get_current_profile

        try:
            profile = get_current_profile()
        except RuntimeError:
            return _SERVICE_PREFIX
    return f"{_SERVICE_PREFIX}-{profile}"


class SecretNotFoundError(Exception):
    """Raised when a secret cannot be found in keychain or environment."""


class SecretStorageUnavailableError(Exception):
    """Raised when no OS keyring backend is available to persist a secret.

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
        """Initialize the store, scoping the keychain service to ``profile``.

        Args:
            profile: Profile name. When None, resolves from the current
                profile (``get_current_profile()``); falls back to the
                unscoped legacy service name when no profile is set.
        """
        self._service = _resolve_service_name(profile)

    def get_key(self, name: str) -> str:
        """Retrieve a secret from OS keychain, falling back to env var.

        Args:
            name: Secret name (e.g. "DATABASE__ENCRYPTION_KEY").
                  Keychain lookup uses service="moneybin-<profile>",
                  username=name. Env var lookup uses MONEYBIN_{name}.

        Returns:
            The secret value.

        Raises:
            SecretNotFoundError: If the secret is not in keychain or env var.
        """
        # Try OS keychain first; missing backend (headless CI, minimal
        # containers) is treated as a keychain miss so the env-var fallback
        # below can satisfy the read.
        try:
            value = keyring.get_password(self._service, name)
        except keyring.errors.NoKeyringError:  # type: ignore[reportAttributeAccessIssue]  # keyring stubs omit errors submodule
            value = None
        if value is not None:
            return value

        # Fall back to environment variable
        env_var = f"{_ENV_PREFIX}{name}"
        value = os.environ.get(env_var)
        if value is not None:
            return value

        raise SecretNotFoundError(
            f"Secret '{name}' not found. Set it via OS keychain "
            f"(moneybin db init) or env var {env_var}."
        )

    def get_env(self, name: str) -> str:
        """Retrieve a secret from environment variable only.

        Use for secrets that don't need keychain storage (API keys,
        server credentials).

        Args:
            name: Secret name (e.g. "SYNC__API_KEY").
                  Looks up MONEYBIN_{name}.

        Returns:
            The secret value.

        Raises:
            SecretNotFoundError: If the env var is not set.
        """
        env_var = f"{_ENV_PREFIX}{name}"
        value = os.environ.get(env_var)
        if value is not None:
            return value

        raise SecretNotFoundError(f"Secret '{name}' not found. Set env var {env_var}.")

    def has_keychain_entry(self, name: str) -> bool:
        """Check if a keychain entry exists for ``name`` (ignores env vars).

        Useful when callers need to distinguish "key is stored in keychain"
        from "key is only available via env var fallback" — e.g. ``init_db``
        needs to persist env-provided keys so the DB stays openable after
        the env var is unset.
        """
        try:
            return keyring.get_password(self._service, name) is not None
        except keyring.errors.NoKeyringError:  # type: ignore[reportAttributeAccessIssue]  # keyring stubs omit errors submodule
            return False

    def set_key(self, name: str, value: str) -> None:
        """Store a secret in the OS keychain.

        Args:
            name: Secret name (e.g. "DATABASE__ENCRYPTION_KEY").
            value: Secret value to store.
        """
        try:
            keyring.set_password(self._service, name, value)
        except keyring.errors.NoKeyringError:  # type: ignore[reportAttributeAccessIssue]  # keyring stubs omit errors submodule
            env_var = f"{_ENV_PREFIX}{name}"
            raise SecretStorageUnavailableError(
                f"No OS keyring backend available to store secret '{name}'. "
                f"Install a backend (e.g. 'keyrings.alt') or supply the value "
                f"via env var {env_var}."
            ) from None
        logger.debug(f"Stored secret '{name}' in OS keychain")

    def delete_key(self, name: str) -> None:
        """Remove a secret from the OS keychain.

        Args:
            name: Secret name to remove.

        Raises:
            SecretNotFoundError: If the secret does not exist in the keychain.
        """
        try:
            keyring.delete_password(self._service, name)
        except keyring.errors.PasswordDeleteError:  # type: ignore[reportAttributeAccessIssue]  # keyring stubs omit errors submodule
            raise SecretNotFoundError(
                f"Secret '{name}' not found in keychain."
            ) from None
        except keyring.errors.NoKeyringError:  # type: ignore[reportAttributeAccessIssue]  # keyring stubs omit errors submodule
            # No keyring backend (e.g. headless CI without keyrings.alt). There
            # cannot be a stored secret to delete, so treat as a no-op miss.
            raise SecretNotFoundError(
                f"Secret '{name}' not found (no keyring backend available)."
            ) from None
        logger.debug(f"Removed secret '{name}' from OS keychain")
