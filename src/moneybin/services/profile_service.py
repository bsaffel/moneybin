"""Profile lifecycle management for MoneyBin.

Handles creation, listing, switching, deletion, display, and
configuration of user profiles. Each profile is an isolation boundary
with its own database, logs, and configuration.
"""

from __future__ import annotations

import logging
import re
import shutil
from pathlib import Path

import yaml

from moneybin.config import get_base_dir
from moneybin.utils.user_config import (
    generate_profile_config,
    get_default_profile,
    normalize_profile_name,
    set_default_profile,
)

logger = logging.getLogger(__name__)

_SAFE_KEY = re.compile(r"^[a-z][a-z0-9_]*$")


def _read_yaml(path: Path) -> dict[str, object]:
    """Read a YAML file and return its contents as a dict.

    Returns an empty dict if the file doesn't exist or isn't a mapping.

    Args:
        path: Path to the YAML file.

    Returns:
        Parsed dict, or empty dict.
    """
    if not path.exists():
        return {}
    with open(path) as f:
        loaded = yaml.safe_load(f)
    return loaded if isinstance(loaded, dict) else {}


class ProfileExistsError(Exception):
    """Raised when attempting to create a profile that already exists."""


class ProfileNotFoundError(Exception):
    """Raised when a profile does not exist."""


class ProfileService:
    """Manages profile lifecycle operations.

    Each profile is stored as a subdirectory under ``<base>/profiles/``.
    The active profile is persisted in the user-level config file via
    :func:`~moneybin.utils.user_config.set_default_profile`.
    """

    def __init__(self) -> None:
        """Initialize the service using the resolved base directory."""
        self._base = get_base_dir()
        self._profiles_dir = self._base / "profiles"

    def _profile_dir(self, name: str) -> Path:
        """Return the directory path for a profile (does not check existence).

        Args:
            name: Raw or normalized profile name.

        Returns:
            Path to the profile directory.

        Raises:
            ValueError: If the normalized name escapes the profiles directory.
        """
        profile_dir = self._profiles_dir / normalize_profile_name(name)
        if not profile_dir.is_relative_to(self._profiles_dir):
            raise ValueError(f"Invalid profile name: {name!r}")
        return profile_dir

    def create(self, name: str, *, init_inbox: bool = False) -> Path:
        """Create a new profile with directory structure and config.

        Creates ``<base>/profiles/<normalized_name>/`` with subdirectories
        ``logs/`` and ``temp/``, and a ``config.yaml`` with sensible defaults.

        Args:
            name: Profile name (will be normalized to lowercase with hyphens).
            init_inbox: When True, also create the import-inbox layout at
                ``<inbox_root>/<normalized_name>/{inbox,processed,failed}/``.

        Returns:
            Path to the new profile directory.

        Raises:
            ProfileExistsError: If a profile with the normalized name already exists.
            ValueError: If the name contains no valid characters.
        """
        normalized = normalize_profile_name(name)
        profile_dir = self._profile_dir(name)
        try:
            profile_dir.mkdir(parents=True, exist_ok=False)
        except FileExistsError:
            raise ProfileExistsError(f"Profile '{normalized}' already exists") from None
        try:
            (profile_dir / "logs").mkdir()
            (profile_dir / "temp").mkdir()
            generate_profile_config(profile_dir, normalized)
            self._init_database(profile_dir, normalized)
            if init_inbox:
                self._init_inbox(normalized)
        except Exception:
            # Roll back the partially created profile so the user can retry
            # without hitting ProfileExistsError.
            shutil.rmtree(profile_dir, ignore_errors=True)
            raise
        logger.debug(f"Created profile: {normalized}")
        return profile_dir

    @staticmethod
    def _init_inbox(profile: str) -> Path:
        """Create the import-inbox layout for ``profile`` and return its root."""
        from moneybin.config import ImportSettings, MoneyBinSettings
        from moneybin.services.inbox_service import InboxService

        settings = MoneyBinSettings(profile=profile, import_=ImportSettings())
        service = InboxService(db=None, settings=settings)
        service.ensure_layout()
        return service.root

    def _init_database(self, profile_dir: Path, profile: str) -> None:
        """Initialize an encrypted database for the profile.

        Args:
            profile_dir: Path to the profile directory.
            profile: Normalized profile name. Scopes the keychain entry so
                profiles never share encryption keys.
        """
        from moneybin.database import init_db

        init_db(profile_dir / "moneybin.duckdb", profile=profile)

    def list(self) -> list[dict[str, str | bool]]:
        """List all profiles with their active status.

        Returns:
            List of dicts with keys ``name`` (str), ``active`` (bool), and
            ``path`` (str). Sorted alphabetically by name. Returns an empty
            list when no profiles directory exists.
        """
        if not self._profiles_dir.exists():
            return []
        active = get_default_profile()
        profiles: list[dict[str, str | bool]] = []
        for entry in sorted(self._profiles_dir.iterdir()):
            if entry.is_dir() and (entry / "config.yaml").exists():
                profiles.append({
                    "name": entry.name,
                    "active": entry.name == active,
                    "path": str(entry),
                })
        return profiles

    def switch(self, name: str) -> None:
        """Switch the active profile.

        Updates the global user config so subsequent commands use this profile.

        Args:
            name: Profile name to activate (normalized before lookup).

        Raises:
            ProfileNotFoundError: If the named profile directory does not exist.
            ValueError: If the name contains no valid characters.
        """
        normalized = normalize_profile_name(name)
        profile_dir = self._profile_dir(name)
        if not profile_dir.exists():
            raise ProfileNotFoundError(f"Profile '{normalized}' not found")
        set_default_profile(normalized)
        logger.debug(f"Switched active profile: {normalized}")

    def delete(self, name: str) -> None:
        """Delete a profile and all its data.

        Removes the entire profile directory tree. This operation is
        irreversible.

        Args:
            name: Profile name to delete (normalized before lookup).

        Raises:
            ProfileNotFoundError: If the named profile directory does not exist.
            ValueError: If the name contains no valid characters, or if the
                profile is currently active.
        """
        normalized = normalize_profile_name(name)
        profile_dir = self._profile_dir(name)
        if not profile_dir.exists():
            raise ProfileNotFoundError(f"Profile '{normalized}' not found")
        if normalized == get_default_profile():
            raise ValueError(
                f"Cannot delete the active profile '{normalized}'. "
                "Switch to another profile first: moneybin profile switch <name>"
            )
        shutil.rmtree(profile_dir)
        # Clear the profile's keychain entries — each profile has its own
        # service ("moneybin-<profile>"), so this never touches sibling profiles.
        from moneybin.secrets import SecretStore

        store = SecretStore(profile=normalized)
        for key_name in ("DATABASE__ENCRYPTION_KEY", "DATABASE__PASSPHRASE_SALT"):
            try:
                store.delete_key(key_name)
            except Exception as e:  # noqa: BLE001 — best-effort cleanup; data dir is already gone
                # Don't turn a successful directory removal into a hard failure
                # if keyring cleanup fails (e.g. NoKeyringError on headless
                # systems, locked keychain, network keyring unreachable).
                logger.debug(f"Keychain cleanup for {key_name} failed: {e}")
        logger.debug(f"Deleted profile directory: {normalized}")

    def show(
        self, name: str | None = None
    ) -> dict[str, str | bool | dict[str, object]]:
        """Show resolved settings for a profile.

        Args:
            name: Profile name to inspect. Defaults to the currently active
                profile, falling back to ``"default"`` if none is set.

        Returns:
            Dict with keys: ``name``, ``active``, ``path``, ``database_path``,
            ``database_exists``, and ``config`` (the raw config.yaml contents).

        Raises:
            ProfileNotFoundError: If the named profile directory does not exist.
            ValueError: If the name contains no valid characters.
        """
        active = get_default_profile()
        if name is None:
            name = active or "default"
        normalized = normalize_profile_name(name)
        profile_dir = self._profile_dir(name)
        if not profile_dir.exists():
            raise ProfileNotFoundError(f"Profile '{normalized}' not found")
        config_data = _read_yaml(profile_dir / "config.yaml")
        db_path = profile_dir / "moneybin.duckdb"
        return {
            "name": normalized,
            "active": normalized == active,
            "path": str(profile_dir),
            "database_path": str(db_path),
            "database_exists": db_path.exists(),
            "config": config_data,
        }

    def set(self, name: str, key: str, value: str) -> None:
        """Set a config value in a profile's config.yaml.

        Only supports two-level dot-notation keys (``section.field``).
        Boolean strings (``"true"``/``"false"``) and digit-only strings
        are coerced to their native types before writing.

        Args:
            name: Profile name (normalized before lookup).
            key: Dot-separated config key, e.g. ``"logging.level"``.
            value: String value to set.

        Raises:
            ProfileNotFoundError: If the named profile directory does not exist.
            ValueError: If ``key`` is not in ``section.field`` format, or if
                the name contains no valid characters.
        """
        normalized = normalize_profile_name(name)
        profile_dir = self._profile_dir(name)
        if not profile_dir.exists():
            raise ProfileNotFoundError(f"Profile '{normalized}' not found")
        config_path = profile_dir / "config.yaml"
        data = _read_yaml(config_path)
        parts = key.split(".")
        if len(parts) != 2:
            raise ValueError(
                f"Key must be section.field (e.g., 'logging.level'), got: {key}"
            )
        section, field = parts
        if not _SAFE_KEY.match(section) or not _SAFE_KEY.match(field):
            raise ValueError(f"Key parts must be lowercase identifiers, got: {key!r}")
        if section not in data or not isinstance(data[section], dict):
            data[section] = {}
        section_dict: dict[str, object] = data[section]  # type: ignore[assignment]  # narrowed above
        # Coerce booleans and integers from string representation
        coerced_value: str | bool | int
        if value.lower() in ("true", "false"):
            coerced_value = value.lower() == "true"
        elif value.isdigit():
            coerced_value = int(value)
        else:
            coerced_value = value
        section_dict[field] = coerced_value
        with open(config_path, "w") as f:
            yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
        logger.info(f"Set {key}={value} for profile {normalized}")
