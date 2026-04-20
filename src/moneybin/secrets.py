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

logger = logging.getLogger(__name__)

_SERVICE_NAME = "moneybin"
_ENV_PREFIX = "MONEYBIN_"


class SecretNotFoundError(Exception):
    """Raised when a secret cannot be found in keychain or environment."""


class SecretStore:
    """Keychain and environment variable interface for secrets.

    Three operations for keychain-backed secrets (encryption keys, E2E keys):
    - get_key(name): keychain → env var → SecretNotFoundError
    - set_key(name, value): write to keychain
    - delete_key(name): clear from keychain

    One operation for env-var-only secrets (API keys, server credentials):
    - get_env(name): env var → SecretNotFoundError
    """

    def get_key(self, name: str) -> str:
        """Retrieve a secret from OS keychain, falling back to env var.

        Args:
            name: Secret name (e.g. "DATABASE__ENCRYPTION_KEY").
                  Keychain lookup uses service="moneybin", username=name.
                  Env var lookup uses MONEYBIN_{name}.

        Returns:
            The secret value.

        Raises:
            SecretNotFoundError: If the secret is not in keychain or env var.
        """
        # Try OS keychain first
        value = keyring.get_password(_SERVICE_NAME, name)
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

    def set_key(self, name: str, value: str) -> None:
        """Store a secret in the OS keychain.

        Args:
            name: Secret name (e.g. "DATABASE__ENCRYPTION_KEY").
            value: Secret value to store.
        """
        keyring.set_password(_SERVICE_NAME, name, value)
        logger.debug("Stored secret '%s' in OS keychain", name)

    def delete_key(self, name: str) -> None:
        """Remove a secret from the OS keychain.

        Args:
            name: Secret name to remove.
        """
        keyring.delete_password(_SERVICE_NAME, name)
        logger.debug("Removed secret '%s' from OS keychain", name)
