"""User configuration management for MoneyBin.

This module manages user-level configuration stored in ~/.moneybin/config.yaml,
including default profile settings and user preferences.
"""

import logging
import re
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


class UserConfig(BaseModel):
    """User-level configuration stored in ~/.moneybin/config.yaml."""

    default_profile: str | None = Field(
        default=None,
        description="Default profile name (user's first name or chosen identifier)",
    )

    @field_validator("default_profile")
    @classmethod
    def validate_default_profile(cls, v: str | None) -> str | None:
        """Validate and normalize profile name."""
        if v is None:
            return None

        # Normalize to lowercase with hyphens
        normalized = normalize_profile_name(v)
        return normalized


def get_user_config_path() -> Path:
    """Get path to user config file.

    Returns:
        Path: ~/.moneybin/config.yaml
    """
    return Path.home() / ".moneybin" / "config.yaml"


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
    """
    config_path = get_user_config_path()

    if not config_path.exists():
        logger.debug(f"User config file not found: {config_path}")
        return UserConfig()

    try:
        with open(config_path) as f:
            raw_data = yaml.safe_load(f)
            data: dict[str, str | None] = raw_data if isinstance(raw_data, dict) else {}
            return UserConfig(**data)
    except Exception as e:
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
        logger.info(f"Saved user config to {config_path}")
    except Exception as e:
        logger.error(f"Failed to save user config to {config_path}: {e}")
        raise


def get_default_profile() -> str | None:
    """Get the default profile name from user config.

    Returns:
        str | None: Default profile name, or None if not set
    """
    config = load_user_config()
    return config.default_profile


def set_default_profile(profile_name: str) -> None:
    """Set the default profile name in user config.

    Args:
        profile_name: Profile name to set as default (will be normalized)

    Raises:
        ValueError: If profile name is invalid
    """
    # Normalize the profile name
    normalized = normalize_profile_name(profile_name)

    # Load existing config
    config = load_user_config()

    # Update default profile
    config.default_profile = normalized

    # Save config
    save_user_config(config)

    logger.info(f"Set default profile to: {normalized}")


def prompt_for_profile_name() -> str:
    """Prompt user for their first name to use as default profile.

    Returns:
        str: Normalized profile name

    Raises:
        ValueError: If user provides invalid input
        KeyboardInterrupt: If user cancels (Ctrl+C)
    """
    print("\n👋 Welcome to MoneyBin!\n")
    print("To get started, please enter your first name.")
    print("This will be your default profile name.")
    print("(You can create additional profiles later for other people or purposes)\n")

    while True:
        try:
            name = input("First name: ").strip()

            if not name:
                print("❌ Please enter a name.\n")
                continue

            # Normalize the name
            try:
                normalized = normalize_profile_name(name)
                print(f"\n✅ Your profile name will be: {normalized}")

                # Confirm with user
                confirm = input("Is this okay? [Y/n]: ").strip().lower()
                if confirm in ("", "y", "yes"):
                    return normalized
                print("\nLet's try again.\n")

            except ValueError as e:
                print(f"❌ {e}")
                print("Please try again with a different name.\n")

        except (KeyboardInterrupt, EOFError):
            print("\n\n⚠️  Setup cancelled. You'll be prompted again next time.")
            raise KeyboardInterrupt("User cancelled profile setup") from None


def ensure_default_profile() -> str:
    """Ensure a default profile exists, prompting user if necessary.

    Returns:
        str: The default profile name

    Raises:
        KeyboardInterrupt: If user cancels setup
    """
    # Check if default profile is already set
    default_profile = get_default_profile()

    if default_profile:
        return default_profile

    # Prompt user for their name
    profile_name = prompt_for_profile_name()

    # Save as default
    set_default_profile(profile_name)

    print(f"\n🎉 Your default profile '{profile_name}' has been created!")
    print(f"    Data will be stored in: data/{profile_name}/\n")

    return profile_name


def reset_user_config() -> None:
    """Reset user configuration by deleting the config file.

    This will prompt user for profile setup on next run.
    """
    config_path = get_user_config_path()

    if config_path.exists():
        config_path.unlink()
        logger.info(f"Deleted user config: {config_path}")
        print(f"✅ Reset user configuration: {config_path}")
    else:
        print("ℹ️  No user configuration to reset.")
