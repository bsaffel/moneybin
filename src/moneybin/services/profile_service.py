"""Profile lifecycle management for MoneyBin.

Handles creation, listing, switching, deletion, display, and
configuration of user profiles. Each profile is an isolation boundary
with its own database, logs, and configuration.
"""

import logging
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
        """
        return self._profiles_dir / normalize_profile_name(name)

    def create(self, name: str) -> Path:
        """Create a new profile with directory structure and config.

        Creates ``<base>/profiles/<normalized_name>/`` with subdirectories
        ``logs/`` and ``temp/``, and a ``config.yaml`` with sensible defaults.

        Args:
            name: Profile name (will be normalized to lowercase with hyphens).

        Returns:
            Path to the new profile directory.

        Raises:
            ProfileExistsError: If a profile with the normalized name already exists.
            ValueError: If the name contains no valid characters.
        """
        normalized = normalize_profile_name(name)
        profile_dir = self._profile_dir(name)
        if profile_dir.exists():
            raise ProfileExistsError(f"Profile '{normalized}' already exists")
        profile_dir.mkdir(parents=True)
        (profile_dir / "logs").mkdir()
        (profile_dir / "temp").mkdir()
        generate_profile_config(profile_dir, normalized)
        logger.info(f"Created profile: {normalized}")
        return profile_dir

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
        logger.info(f"Switched to profile: {normalized}")

    def delete(self, name: str) -> None:
        """Delete a profile and all its data.

        Removes the entire profile directory tree. This operation is
        irreversible.

        Args:
            name: Profile name to delete (normalized before lookup).

        Raises:
            ProfileNotFoundError: If the named profile directory does not exist.
            ValueError: If the name contains no valid characters.
        """
        normalized = normalize_profile_name(name)
        profile_dir = self._profile_dir(name)
        if not profile_dir.exists():
            raise ProfileNotFoundError(f"Profile '{normalized}' not found")
        shutil.rmtree(profile_dir)
        logger.info(f"Deleted profile: {normalized}")

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
        if name is None:
            name = get_default_profile() or "default"
        normalized = normalize_profile_name(name)
        profile_dir = self._profile_dir(name)
        if not profile_dir.exists():
            raise ProfileNotFoundError(f"Profile '{normalized}' not found")
        config_path = profile_dir / "config.yaml"
        config_data: dict[str, object] = {}
        if config_path.exists():
            with open(config_path) as f:
                loaded = yaml.safe_load(f)
                if isinstance(loaded, dict):
                    config_data = loaded
        active = get_default_profile()
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
        data: dict[str, object] = {}
        if config_path.exists():
            with open(config_path) as f:
                loaded = yaml.safe_load(f)
                if isinstance(loaded, dict):
                    data = loaded
        parts = key.split(".")
        if len(parts) != 2:
            raise ValueError(
                f"Key must be section.field (e.g., 'logging.level'), got: {key}"
            )
        section, field = parts
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
