"""Centralized configuration management for MoneyBin application.

This module provides a modern Pydantic Settings-based configuration system
that consolidates all application settings with environment variable integration,
type validation, and clear error handling.
"""

import os
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseConfig(BaseModel):
    """Database configuration settings."""

    model_config = ConfigDict(frozen=True)

    path: Path = Field(
        default=Path("data/default/moneybin.duckdb"),
        description="Path to DuckDB database file",
    )
    backup_path: Path | None = Field(
        default=None, description="Path to database backup directory"
    )
    create_dirs: bool = Field(
        default=True, description="Automatically create database directories"
    )

    @field_validator("path")
    @classmethod
    def validate_database_path(cls, v: Path) -> Path:
        """Ensure database path has correct extension."""
        if not str(v).endswith((".db", ".duckdb")):
            raise ValueError("Database path must end with .db or .duckdb")
        return v


class DataConfig(BaseModel):
    """Data processing and storage configuration."""

    model_config = ConfigDict(frozen=True)

    raw_data_path: Path = Field(
        default=Path("data/raw"), description="Path to raw data directory"
    )
    temp_data_path: Path = Field(
        default=Path("data/temp"), description="Path to temporary data directory"
    )
    save_raw_data: bool = Field(
        default=True, description="Whether to save raw extracted data"
    )
    incremental_loading: bool = Field(
        default=True, description="Use incremental loading by default"
    )


class LoggingConfig(BaseModel):
    """Logging configuration settings."""

    model_config = ConfigDict(frozen=True)

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO", description="Logging level"
    )
    log_to_file: bool = Field(default=True, description="Enable file logging")
    log_file_path: Path = Field(
        default=Path("logs/default/moneybin.log"), description="Path to log file"
    )
    max_file_size_mb: int = Field(
        default=50, ge=1, le=1000, description="Maximum log file size in MB"
    )
    backup_count: int = Field(
        default=5, ge=1, le=50, description="Number of log file backups to keep"
    )


class SyncConfig(BaseModel):
    """Configuration for MoneyBin Sync service (optional paid tier).

    The sync service provides automatic bank data synchronization through
    hosted connectors (Plaid, Yodlee, etc.) with E2E encryption.

    Security Model:
    - All bank access tokens stored server-side only
    - Client authenticates via OAuth2/Auth0 (future)
    - Client never handles or sees Plaid access tokens
    - All bank API communication happens server-side
    """

    model_config = ConfigDict(frozen=True)

    enabled: bool = Field(
        default=False,
        description="Enable MoneyBin Sync service (paid tier)",
    )
    server_url: str | None = Field(
        default=None,
        description="MoneyBin Sync server URL (e.g., https://sync.moneybin.app)",
    )
    api_key: str | None = Field(
        default=None,
        description="API key for MoneyBin Sync service (legacy - prefer OAuth)",
    )
    use_local_server: bool = Field(
        default=True,
        description="Use local server code directly (development mode)",
    )

    # OAuth/Auth0 configuration (future)
    oauth_client_id: str | None = Field(
        default=None,
        description="OAuth2 client ID (for Auth0 integration)",
    )
    oauth_client_secret: str | None = Field(
        default=None,
        description="OAuth2 client secret (for Auth0 integration)",
    )
    oauth_domain: str | None = Field(
        default=None,
        description="OAuth2/Auth0 domain (e.g., moneybin.auth0.com)",
    )
    oauth_audience: str | None = Field(
        default=None,
        description="OAuth2 API audience/identifier",
    )


class MoneyBinSettings(BaseSettings):
    """Main application settings with environment variable integration.

    This class consolidates all configuration settings and provides
    automatic loading from environment variables with validation.

    Environment variables are loaded with the MONEYBIN_ prefix.
    For nested configs, use double underscores: MONEYBIN_DATABASE__PATH

    Profile Support:
    - Loads from .env.{profile} files (e.g., .env.dev, .env.prod)
    - Falls back to .env for backward compatibility
    - Profile defaults to "dev" for safety
    """

    # Core configuration sections
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    data: DataConfig = Field(default_factory=DataConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    sync: SyncConfig = Field(default_factory=SyncConfig)

    # Application settings
    debug: bool = Field(default=False, description="Enable debug mode")
    environment: Literal["development", "staging", "production"] = Field(
        default="development", description="Application environment"
    )
    profile: str = Field(
        default="default",
        description="User profile name (e.g., alice, bob, household)",
    )

    @field_validator("profile")
    @classmethod
    def validate_profile_name(cls, v: str) -> str:
        """Ensure profile name is safe for use as a filename and normalize it."""
        from moneybin.utils.user_config import normalize_profile_name

        if not v:
            raise ValueError("Profile name cannot be empty")

        # Normalize profile name to lowercase with hyphens
        # This allows users to provide "John Smith", "Alice_Work", etc.
        # and converts them to "john-smith", "alice-work", etc.
        try:
            normalized = normalize_profile_name(v)
            return normalized
        except ValueError as e:
            raise ValueError(f"Invalid profile name: {e}") from e

    def __init__(self, **kwargs: Any):
        """Initialize settings with environment variable overrides.

        Args:
            **kwargs: Additional configuration overrides
        """
        # Get profile name (will be normalized by validator)
        profile = kwargs.get("profile", "default")

        # Make paths profile-aware if not explicitly provided
        # Structure: data/{profile}/[raw, temp]

        # Check for legacy DUCKDB_PATH environment variable
        duckdb_path = os.getenv("DUCKDB_PATH")

        # Set database path if not explicitly provided or if using old default
        if "database" not in kwargs or (
            "database" in kwargs
            and kwargs["database"].path == Path("data/default/moneybin.duckdb")
        ):
            if duckdb_path:
                kwargs["database"] = DatabaseConfig(path=Path(duckdb_path))
            else:
                # Use profile-aware path
                kwargs["database"] = DatabaseConfig(
                    path=Path(f"data/{profile}/moneybin.duckdb")
                )

        if "data" not in kwargs:
            # Use profile-aware data paths
            kwargs["data"] = DataConfig(
                raw_data_path=Path(f"data/{profile}/raw"),
                temp_data_path=Path(f"data/{profile}/temp"),
            )

        if "logging" not in kwargs:
            # Use profile-aware log path
            kwargs["logging"] = LoggingConfig(
                log_file_path=Path(f"logs/{profile}/moneybin.log")
            )

        super().__init__(**kwargs)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: Any,
        env_settings: Any,
        dotenv_settings: Any,
        file_secret_settings: Any,
    ) -> tuple[Any, ...]:
        """Customize how settings are loaded to support profile-based env files.

        This method is called by Pydantic Settings to determine the order and
        sources of configuration loading.
        """
        # Get profile from init settings if provided
        # Pyright reports "Type of 'get' is partially unknown" because init_settings
        # from Pydantic doesn't have well-defined types in the stubs.
        init_dict = init_settings.init_kwargs if init_settings else {}
        profile = init_dict.get("profile", "dev")  # type: ignore[reportUnknownMemberType]

        # Determine which env file to load based on profile
        profile_env_file = Path(f".env.{profile}")
        if profile_env_file.exists():
            env_file = str(profile_env_file)
        else:
            # Fall back to .env for backward compatibility
            env_file = ".env"

        # Create custom dotenv settings with the profile-specific file
        from pydantic_settings import DotEnvSettingsSource

        custom_dotenv = DotEnvSettingsSource(
            settings_cls,
            env_file=env_file,
            env_file_encoding="utf-8",
        )

        # Return sources in priority order (later sources override earlier ones)
        return (
            init_settings,
            env_settings,
            custom_dotenv,
            file_secret_settings,
        )

    model_config = SettingsConfigDict(
        env_file=".env",  # Default, but overridden by settings_customise_sources
        env_file_encoding="utf-8",
        env_prefix="MONEYBIN_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",  # Allow extra env vars that don't match our schema
        frozen=True,
    )

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate application environment."""
        if v == "production" and os.getenv("DEBUG", "").lower() in ("true", "1"):
            raise ValueError("DEBUG mode cannot be enabled in production")
        return v

    def create_directories(self) -> None:
        """Create necessary directories for the application."""
        directories = [
            self.database.path.parent,
            self.data.raw_data_path,
            self.data.temp_data_path,
            self.logging.log_file_path.parent,
        ]

        if self.database.backup_path:
            directories.append(self.database.backup_path)

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

    def validate_required_credentials(self) -> None:
        """Validate that required credentials are present.

        Note: Plaid credentials are now validated on the server side.
        This method is kept for potential future client-side validation needs.
        """
        # No client-side credentials to validate currently
        pass


# Global settings - single instance for current profile
_current_settings: MoneyBinSettings | None = None


def _get_initial_profile() -> str:
    """Get the initial profile from user config or default to 'default'.

    Returns:
        str: The profile name to use (from user config or 'default')
    """
    try:
        from moneybin.utils.user_config import get_default_profile

        profile = get_default_profile()
        return profile if profile else "default"
    except Exception:
        # If we can't load user config, default to 'default'
        return "default"


_current_profile: str = _get_initial_profile()


def get_settings() -> MoneyBinSettings:
    """Get the settings instance for the current user profile.

    This function provides a singleton pattern for accessing configuration
    throughout the application. Settings are loaded once and cached for the
    current profile.

    Returns:
        MoneyBinSettings: The configuration instance for the current profile

    Raises:
        ValueError: If required configuration is missing or invalid

    Note:
        The profile is determined by _current_profile, which can be changed
        via set_current_profile(). Changing the profile invalidates the cache.
    """
    global _current_settings, _current_profile

    # Return cached settings if available
    if _current_settings is not None:
        return _current_settings

    # Load and cache new settings for current profile
    try:
        settings = MoneyBinSettings(profile=_current_profile)
        settings.validate_required_credentials()

        # Create directories if configured to do so
        if settings.database.create_dirs:
            settings.create_directories()

        _current_settings = settings
        return settings

    except Exception as e:
        raise ValueError(
            f"Configuration error for profile '{_current_profile}': {e}"
        ) from e


def set_current_profile(profile: str) -> None:
    """Set the current active user profile.

    Changing the profile invalidates the cached settings, forcing a reload
    on the next call to get_settings().

    Args:
        profile: User profile name (e.g., 'alice', 'bob', 'household')
                 Will be normalized to lowercase with hyphens

    Raises:
        ValueError: If profile name contains invalid characters
    """
    from moneybin.utils.user_config import normalize_profile_name

    global _current_profile, _current_settings

    if not profile:
        raise ValueError("Profile name cannot be empty")

    # Normalize profile name
    try:
        normalized = normalize_profile_name(profile)

        # Only invalidate cache if profile actually changed
        if normalized != _current_profile:
            _current_profile = normalized
            _current_settings = None  # Invalidate cache
    except ValueError as e:
        raise ValueError(f"Invalid profile name: {e}") from e


def get_current_profile() -> str:
    """Get the current active user profile.

    Returns:
        str: The current profile name (e.g., 'alice', 'bob', 'default')
    """
    return _current_profile


def get_settings_for_profile(profile: str) -> MoneyBinSettings:
    """Get settings for a specific user profile.

    This is a convenience function that switches to the specified profile
    and returns its settings. The profile change persists after this call.

    Args:
        profile: User profile name (e.g., 'alice', 'bob', 'household')
                 Will be normalized to lowercase with hyphens

    Returns:
        MoneyBinSettings: The configuration instance for the specified profile

    Raises:
        ValueError: If required configuration is missing or invalid

    Note:
        This function changes the current profile via set_current_profile().
        If you need the current profile to remain unchanged, consider saving
        it first and restoring it after.
    """
    if not profile:
        raise ValueError("Profile name cannot be empty")

    # Switch to the requested profile
    try:
        set_current_profile(profile)
        return get_settings()
    except ValueError as e:
        raise ValueError(f"Invalid profile name: {e}") from e


def reload_settings(profile: str | None = None) -> MoneyBinSettings:
    """Reload settings from environment variables.

    This function forces a reload of the configuration, useful for testing
    or when environment variables change at runtime.

    Args:
        profile: Profile to reload. If None, reloads current profile.
                 If specified and different from current, switches to that profile.

    Returns:
        MoneyBinSettings: The reloaded configuration instance
    """
    global _current_settings, _current_profile

    # If profile specified, switch to it (which invalidates cache)
    if profile is not None and profile != _current_profile:
        set_current_profile(profile)
    else:
        # Just invalidate current cache to force reload
        _current_settings = None

    return get_settings()


def clear_settings_cache() -> None:
    """Clear cached settings and reset to test profile.

    This function is primarily for testing to ensure clean state between tests.
    It clears the cached settings and resets the current profile to 'test'.
    """
    global _current_settings, _current_profile

    _current_settings = None
    _current_profile = "test"


# Convenience functions for common configuration access
def get_database_path() -> Path:
    """Get the configured database path for the current profile.

    Returns:
        Path: The database path
    """
    return get_settings().database.path


def get_raw_data_path() -> Path:
    """Get the configured raw data path for the current profile.

    Returns:
        Path: The raw data path
    """
    return get_settings().data.raw_data_path


def get_sync_config() -> SyncConfig:
    """Get the sync service configuration for the current profile.

    Returns:
        SyncConfig: The sync service configuration
    """
    return get_settings().sync


def get_logging_config() -> LoggingConfig:
    """Get the logging configuration for the current profile.

    Returns:
        LoggingConfig: The logging configuration
    """
    return get_settings().logging
