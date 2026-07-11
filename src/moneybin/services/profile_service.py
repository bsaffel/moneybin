"""Profile lifecycle management for MoneyBin.

Handles creation, listing, switching, deletion, display, and
configuration of user profiles. Each profile is an isolation boundary
with its own database, logs, and configuration.
"""

from __future__ import annotations

import contextlib
import logging
import os
import re
import shutil
from collections.abc import Generator
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


@contextlib.contextmanager
def _restrictive_umask() -> Generator[None, None, None]:
    """Force ``os.umask(0o077)`` for the wrapped block.

    Used around ``mkdir`` calls so directories land at ``0o700`` atomically,
    without a window at the default umask between ``mkdir`` and ``chmod``.
    """
    prev = os.umask(0o077)
    try:
        yield
    finally:
        os.umask(prev)


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

    def exists(self, name: str) -> bool:
        """True if the profile directory exists at all — registered or not.

        `create()` refuses on the bare directory, so callers offering recovery
        advice need this to know whether `profile create` is even open to them.
        """
        try:
            return self._profile_dir(name).exists()
        except ValueError:
            return False

    def is_registered(self, name: str) -> bool:
        """True if the profile was set up (has a config.yaml), not just db-init'd.

        Distinguishes a fully-registered profile from a bare directory a manual
        `db init` may have left behind — the two need different onboarding
        guidance.
        """
        try:
            return (self._profile_dir(name) / "config.yaml").exists()
        except ValueError:
            return False

    def has_database(self, name: str) -> bool:
        """True if the profile directory already holds a database file.

        `create()` adopts an unregistered directory and initializes a database only
        when one is absent, so a caller that wants to tell the user which of the two
        happened has to ask before calling. (It reports on the file's presence, not
        its health — see `db_path` / `system doctor` for that.)
        """
        try:
            return (self._profile_dir(name) / "moneybin.duckdb").exists()
        except ValueError:
            return False

    def ensure_registered(self, name: str, *, init_inbox: bool = False) -> Path:
        """Scaffold the non-database half of a profile: logs, temp, inbox, config.

        A bare `moneybin db init` leaves a directory (and database) that `list()`
        hides — it filters on `config.yaml` — and that never got an inbox. This
        fills in what's missing, in place, without touching an existing database.

        `create()` composes this for both the fresh and the adopted path, so there
        is one scaffolding routine rather than two that can drift. Callers that
        must run their own checks *before* scaffolding (e.g. `DemoService`, whose
        data-safety guard has to clear first) call it directly.

        `config.yaml` is written LAST because it is the commit marker: `list()`
        shows a profile once it exists, and `create()` refuses once it does. Write
        it before the inbox and a failure half-way leaves a profile that is visible,
        incomplete, and no longer completable by `create` — the dead end this whole
        contract exists to remove.

        Idempotent: a fully-registered profile is left alone.
        """
        normalized = normalize_profile_name(name)
        profile_dir = self._profile_dir(name)
        with _restrictive_umask():
            (profile_dir / "logs").mkdir(mode=0o700, exist_ok=True)
            (profile_dir / "temp").mkdir(mode=0o700, exist_ok=True)
        if init_inbox:
            self._init_inbox(normalized)
        if not (profile_dir / "config.yaml").exists():
            generate_profile_config(profile_dir, normalized)
            logger.debug(f"Completed registration for profile: {normalized}")
        return profile_dir

    def create(self, name: str, *, init_inbox: bool = False) -> Path:
        """Create a profile, or complete an unregistered one, in place.

        Creates ``<base>/profiles/<normalized_name>/`` with subdirectories
        ``logs/`` and ``temp/``, a ``config.yaml`` with sensible defaults, and an
        encrypted database.

        A directory with no ``config.yaml`` is *adopted*, not refused: a bare
        ``moneybin db init``, a hand ``mkdir``, or a partial delete leaves exactly
        that, and refusing it would strand the profile with no repair verb
        (``profile list`` hides it, and it has no inbox). Adoption never touches an
        existing database and never rolls the directory back — it may already hold
        the user's data.

        Args:
            name: Profile name (will be normalized to lowercase with hyphens).
            init_inbox: When True, also create the import-inbox layout at
                ``<inbox_root>/<normalized_name>/{inbox,processed,failed}/``.

        Returns:
            Path to the profile directory.

        Raises:
            ProfileExistsError: If a *registered* profile (one with a
                ``config.yaml``) already exists under the normalized name.
            ValueError: If the name contains no valid characters.
        """
        normalized = normalize_profile_name(name)
        profile_dir = self._profile_dir(name)
        # 0o700: profile dir holds the encrypted DB, privacy.log.jsonl, and
        # other per-profile state; not readable by other users. Restrictive
        # umask makes the perms apply at creation time — no window where the
        # dir exists at the umask-default (typically 0o755).
        #
        # mkdir(exist_ok=False) doubles as the existence check, so the
        # registered-vs-bare decision happens atomically against the filesystem
        # rather than in a check-then-act window.
        try:
            with _restrictive_umask():
                profile_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
            adopted = False
        except FileExistsError:
            if (profile_dir / "config.yaml").exists():
                raise ProfileExistsError(
                    f"Profile '{normalized}' already exists"
                ) from None
            adopted = True
            # An adopted directory was made by something else — `db init`, or a hand
            # `mkdir` — at whatever the ambient umask was, typically 0o755. The
            # encrypted database and privacy log are about to live behind it under
            # our name, so it gets the same 0o700 the fresh path guarantees rather
            # than keeping perms we never chose.
            profile_dir.chmod(0o700)
        try:
            # Database before registration: `_init_database` is the step that
            # actually fails in the field (a locked or unavailable OS keychain), and
            # `ensure_registered` writes the `config.yaml` commit marker. Registering
            # first would leave a failed create visible to `list()` and refused by
            # `create()` — stranding the user exactly as before.
            if not (profile_dir / "moneybin.duckdb").exists():
                self._init_database(profile_dir, normalized)
            self.ensure_registered(normalized, init_inbox=init_inbox)
        except Exception:
            # Roll back only a directory we made ourselves. An adopted directory
            # may hold a `db init`'d database — the thing the caller is trying to
            # recover — and rmtree would destroy it. It stays unregistered, so a
            # retry of `create` still completes it.
            if not adopted:
                shutil.rmtree(profile_dir, ignore_errors=True)
            raise
        logger.debug(f"{'Completed' if adopted else 'Created'} profile: {normalized}")
        return profile_dir

    @staticmethod
    def _init_inbox(profile: str) -> Path:
        """Create the import-inbox layout for ``profile`` and return its root."""
        from moneybin.config import MoneyBinSettings
        from moneybin.services.inbox_service import InboxService

        # Don't pass import_= explicitly — let MoneyBinSettings build it via
        # its default_factory so MONEYBIN_IMPORT___INBOX_ROOT env overrides
        # apply (e.g. test isolation in tests/conftest.py).
        settings = MoneyBinSettings(profile=profile)
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
        # Capture the opaque sync id before the dir (and its profile_id file) are
        # removed, so the broker tokens scoped to it can be cleared afterward.
        sync_id_file = profile_dir / "profile_id"
        sync_profile_id = (
            sync_id_file.read_text().strip() if sync_id_file.exists() else None
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
        # Clear the profile's scoped broker sync tokens (a separate keyring
        # service from the SecretStore above), so a delete leaves nothing behind.
        if sync_profile_id:
            from moneybin.connectors.sync_client import SyncClient

            SyncClient.clear_tokens_for_profile(sync_profile_id)
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
