"""User configuration management for MoneyBin.

This module manages user-level configuration stored in ~/.moneybin/config.yaml,
including default profile settings and user preferences.
"""

import logging
import re
from datetime import date
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class UserConfig(BaseModel):
    """User-level configuration stored in ~/.moneybin/config.yaml."""

    active_profile: str | None = Field(
        default=None,
        description="Active profile name (user's first name or chosen identifier)",
    )

    @field_validator("active_profile")
    @classmethod
    def validate_active_profile(cls, v: str | None) -> str | None:
        """Validate and normalize profile name."""
        if v is None:
            return None

        # Normalize to lowercase with hyphens
        normalized = normalize_profile_name(v)
        return normalized


def get_user_config_path() -> Path:
    """Get path to user config file.

    Returns:
        Path: <base>/config.yaml, where <base> honors MONEYBIN_HOME.
        Test isolation depends on this — without it, e2e tests would
        write to the user's real ~/.moneybin/config.yaml.
    """
    from moneybin.config import get_base_dir

    return get_base_dir() / "config.yaml"


def normalize_profile_name(name: str) -> str:
    """Normalize profile name to lowercase with hyphens.

    Converts spaces and underscores to hyphens, removes special characters,
    and converts to lowercase.

    Args:
        name: Raw profile name (e.g., "John Smith", "Alice_Work", "BOB")

    Returns:
        str: Normalized profile name (e.g., "john-smith", "alice-work", "bob")

    Raises:
        ValueError: If name is empty or contains only invalid characters

    Examples:
        >>> normalize_profile_name("John Smith")
        'john-smith'
        >>> normalize_profile_name("Alice_Work")
        'alice-work'
        >>> normalize_profile_name("BOB")
        'bob'
    """
    if not name or not name.strip():
        raise ValueError("Profile name cannot be empty")

    # Convert to lowercase
    normalized = name.lower()

    # Replace spaces and underscores with hyphens
    normalized = normalized.replace(" ", "-").replace("_", "-")

    # Remove any characters that aren't alphanumeric or hyphens
    normalized = re.sub(r"[^a-z0-9-]", "", normalized)

    # Remove consecutive hyphens
    normalized = re.sub(r"-+", "-", normalized)

    # Remove leading/trailing hyphens
    normalized = normalized.strip("-")

    if not normalized:
        raise ValueError(
            f"Profile name '{name}' contains no valid characters. "
            "Profile names must contain letters or numbers."
        )

    return normalized


def load_user_config() -> UserConfig:
    """Load user configuration from ~/.moneybin/config.yaml.

    Returns:
        UserConfig: User configuration object

    Note:
        Returns default UserConfig if file doesn't exist or cannot be read.
        Migrates old ``default_profile`` key to ``active_profile`` on load.
    """
    config_path = get_user_config_path()

    if not config_path.exists():
        logger.debug(f"User config file not found: {config_path}")
        return UserConfig()

    try:
        with open(config_path) as f:
            raw_data = yaml.safe_load(f)
            data: dict[str, str | None] = raw_data if isinstance(raw_data, dict) else {}
            # Migrate old default_profile key to active_profile
            if "default_profile" in data and "active_profile" not in data:
                data["active_profile"] = data.pop("default_profile")
            return UserConfig(**data)
    except (yaml.YAMLError, OSError) as e:
        logger.warning(f"Failed to load user config from {config_path}: {e}")
        return UserConfig()


def save_user_config(config: UserConfig) -> None:
    """Save user configuration to ~/.moneybin/config.yaml.

    Args:
        config: User configuration object to save

    Raises:
        OSError: If unable to write config file
    """
    config_path = get_user_config_path()

    # Ensure directory exists
    config_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(config_path, "w") as f:
            data = config.model_dump(exclude_none=True)
            yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
        logger.debug(f"Saved user config to {config_path}")
    except OSError as e:
        logger.error(f"Failed to save user config to {config_path}: {e}")
        raise


def get_default_profile() -> str | None:
    """Get the active profile name from user config.

    Returns:
        str | None: Active profile name, or None if not set
    """
    config = load_user_config()
    return config.active_profile


def set_default_profile(profile_name: str) -> None:
    """Set the active profile name in user config.

    Args:
        profile_name: Profile name to set as active (will be normalized)

    Raises:
        ValueError: If profile name is invalid
    """
    # Normalize the profile name
    normalized = normalize_profile_name(profile_name)

    # Load existing config
    config = load_user_config()

    # Update active profile
    config.active_profile = normalized

    # Save config
    save_user_config(config)

    logger.debug(f"Set active profile to: {normalized}")


def generate_profile_config(profile_dir: Path, profile_name: str) -> Path:
    """Generate a per-profile config.yaml with sensible defaults.

    Args:
        profile_dir: Directory for the profile (will be created).
        profile_name: Profile name (for header comment).

    Returns:
        Path to the created config.yaml.
    """
    profile_dir.mkdir(parents=True, exist_ok=True)
    config_path = profile_dir / "config.yaml"

    config_data = {
        "database": {
            "encryption_key_mode": "auto",
        },
        "logging": {
            "level": "INFO",
            "log_to_file": True,
        },
        "sync": {
            "enabled": False,
        },
    }

    header = f"# Profile: {profile_name}\n# Created: {date.today()}\n\n"
    with open(config_path, "w") as f:
        f.write(header)
        yaml.safe_dump(config_data, f, default_flow_style=False, sort_keys=False)

    return config_path


def prompt_for_profile_name() -> str:
    """Prompt user for their first name to use as default profile.

    Returns:
        str: Normalized profile name

    Raises:
        ValueError: If user provides invalid input
        KeyboardInterrupt: If user cancels (Ctrl+C)
    """
    import typer

    typer.echo("\n👋 Welcome to MoneyBin!\n")
    typer.echo("To get started, please enter your first name.")
    typer.echo("This will be your default profile name.")
    typer.echo(
        "(You can create additional profiles later for other people or purposes)\n"
    )

    while True:
        try:
            name = input("First name: ").strip()

            if not name:
                typer.echo("❌ Please enter a name.\n")
                continue

            # Normalize the name
            try:
                normalized = normalize_profile_name(name)
                typer.echo(f"\n✅ Your profile name will be: {normalized}")

                # Confirm with user
                confirm = input("Is this okay? [Y/n]: ").strip().lower()
                if confirm in ("", "y", "yes"):
                    return normalized
                typer.echo("\nLet's try again.\n")

            except ValueError as e:
                typer.echo(f"❌ {e}")
                typer.echo("Please try again with a different name.\n")

        except (KeyboardInterrupt, EOFError):
            typer.echo("\n\n⚠️  Setup cancelled. You'll be prompted again next time.")
            raise KeyboardInterrupt("User cancelled profile setup") from None


def ensure_default_profile() -> str:
    """Ensure a default profile exists, prompting user if necessary.

    Returns:
        str: The default profile name

    Raises:
        KeyboardInterrupt: If user cancels setup
    """
    import typer

    # Check if default profile is already set
    default_profile = get_default_profile()

    if default_profile:
        return default_profile

    # Prompt user for their name
    profile_name = prompt_for_profile_name()

    # Create the profile directory structure first; only persist as the
    # default after creation (and DB init) succeed. Persisting before
    # success would leave config.yaml pointing at a profile that doesn't
    # exist if create/init fails — every subsequent command would then
    # error out with "profile directory not found".
    from moneybin.services.profile_service import ProfileExistsError, ProfileService

    try:
        svc = ProfileService()
        profile_dir = svc.create(profile_name)
    except ProfileExistsError:
        from moneybin.config import get_base_dir

        profile_dir = get_base_dir() / "profiles" / profile_name
    except Exception as e:  # noqa: BLE001 — first-run wizard must surface any setup failure as a clean message
        # Profile directory rollback is handled by ProfileService.create();
        # surface a clean error instead of a raw traceback so the user can retry.
        typer.echo(f"\n❌ Failed to create profile '{profile_name}': {e}", err=True)
        typer.echo(
            "💡 Run 'moneybin profile create <name>' to retry, or set "
            "MONEYBIN_PROFILE to use an existing profile.",
            err=True,
        )
        raise typer.Exit(1) from e

    set_default_profile(profile_name)

    typer.echo(f"\n🎉 Your default profile '{profile_name}' has been created!")
    typer.echo(f"    Data will be stored in: {profile_dir}")

    # ProfileService.create() already initializes the encrypted database,
    # so no separate init step is needed here.

    return profile_name


def reset_user_config() -> None:
    """Reset user configuration by deleting the config file.

    This will prompt user for profile setup on next run.
    """
    config_path = get_user_config_path()

    import typer

    if config_path.exists():
        config_path.unlink()
        logger.info(f"Deleted user config: {config_path}")
        typer.echo(f"✅ Reset user configuration: {config_path}")
    else:
        typer.echo("ℹ️  No user configuration to reset.")
