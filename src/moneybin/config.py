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
        default=Path("data/duckdb/moneybin.duckdb"),
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


class PlaidConfig(BaseModel):
    """Plaid API configuration settings."""

    model_config = ConfigDict(frozen=True)

    client_id: str = Field(..., description="Plaid client ID")
    secret: str = Field(..., description="Plaid secret key")
    environment: Literal["sandbox", "development", "production"] = Field(
        default="sandbox", description="Plaid environment"
    )
    days_lookback: int = Field(
        default=365,
        ge=1,
        le=730,
        description="Default days to look back for transactions",
    )
    batch_size: int = Field(
        default=500, ge=1, le=500, description="Batch size for API requests"
    )
    max_retries: int = Field(
        default=3, ge=0, le=10, description="Maximum API retry attempts"
    )
    retry_delay: float = Field(
        default=1.0, ge=0.1, le=10.0, description="Delay between retries in seconds"
    )


class DataConfig(BaseModel):
    """Data processing and storage configuration."""

    model_config = ConfigDict(frozen=True)

    raw_data_path: Path = Field(
        default=Path("data/raw"), description="Path to raw data directory"
    )
    processed_data_path: Path = Field(
        default=Path("data/processed"), description="Path to processed data directory"
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
        default=Path("logs/moneybin.log"), description="Path to log file"
    )
    max_file_size_mb: int = Field(
        default=50, ge=1, le=1000, description="Maximum log file size in MB"
    )
    backup_count: int = Field(
        default=5, ge=1, le=50, description="Number of log file backups to keep"
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
    plaid: PlaidConfig = Field(
        default_factory=lambda: PlaidConfig(
            client_id="", secret="", environment="sandbox"
        )
    )
    data: DataConfig = Field(default_factory=DataConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

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
        """Ensure profile name is safe for use as a filename."""
        import re

        if not v:
            raise ValueError("Profile name cannot be empty")

        # Allow alphanumeric, dash, and underscore only
        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError(
                "Profile name must contain only alphanumeric characters, "
                "dashes, and underscores"
            )

        return v

    def __init__(self, **kwargs: Any):
        """Initialize settings with environment variable overrides.

        Args:
            **kwargs: Additional configuration overrides
        """
        # Handle legacy DUCKDB_PATH environment variable
        if "database" not in kwargs:
            duckdb_path = os.getenv("DUCKDB_PATH")
            if duckdb_path:
                kwargs["database"] = DatabaseConfig(path=Path(duckdb_path))

        # Handle legacy Plaid environment variables
        if "plaid" not in kwargs:
            plaid_config: dict[str, Any] = {}
            client_id = os.getenv("PLAID_CLIENT_ID")
            secret = os.getenv("PLAID_SECRET")
            env = os.getenv("PLAID_ENV", "sandbox")

            if client_id:
                plaid_config["client_id"] = client_id
            if secret:
                plaid_config["secret"] = secret
            if env in ("sandbox", "development", "production"):
                plaid_config["environment"] = env

            if plaid_config and client_id and secret:
                kwargs["plaid"] = PlaidConfig(**plaid_config)

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
            self.data.processed_data_path,
            self.data.temp_data_path,
            self.logging.log_file_path.parent,
        ]

        if self.database.backup_path:
            directories.append(self.database.backup_path)

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

    def validate_required_credentials(self) -> None:
        """Validate that required credentials are present."""
        errors: list[str] = []

        if not self.plaid.client_id:
            errors.append("PLAID_CLIENT_ID is required")
        if not self.plaid.secret:
            errors.append("PLAID_SECRET is required")

        if errors:
            raise ValueError(f"Missing required configuration: {', '.join(errors)}")


# Global settings instances - lazy loaded per profile
_settings_cache: dict[str, MoneyBinSettings] = {}
_current_profile: str = "default"


def get_settings(profile: str | None = None) -> MoneyBinSettings:
    """Get the settings instance for the specified user profile.

    This function provides a singleton pattern per profile for accessing configuration
    throughout the application. Settings are loaded once per profile and cached.

    Args:
        profile: User profile name (e.g., 'alice', 'bob'). Defaults to current profile.

    Returns:
        MoneyBinSettings: The configuration instance for the specified profile

    Raises:
        ValueError: If required configuration is missing or invalid
    """
    global _current_profile, _settings_cache

    # Use current profile if none specified
    if profile is None:
        profile = _current_profile

    # Return cached settings if available
    if profile in _settings_cache:
        return _settings_cache[profile]

    # Load and cache new settings for this profile
    try:
        settings = MoneyBinSettings(profile=profile)
        settings.validate_required_credentials()

        # Create directories if configured to do so
        if settings.database.create_dirs:
            settings.create_directories()

        _settings_cache[profile] = settings
        return settings

    except Exception as e:
        raise ValueError(f"Configuration error for profile '{profile}': {e}") from e


def set_current_profile(profile: str) -> None:
    """Set the current active user profile.

    Args:
        profile: User profile name (e.g., 'alice', 'bob', 'household')

    Raises:
        ValueError: If profile name contains invalid characters
    """
    import re

    global _current_profile

    if not profile:
        raise ValueError("Profile name cannot be empty")

    # Validate profile name is safe for filenames
    if not re.match(r"^[a-zA-Z0-9_-]+$", profile):
        raise ValueError(
            f"Invalid profile: {profile}. "
            "Profile name must contain only alphanumeric characters, dashes, and underscores"
        )

    _current_profile = profile


def get_current_profile() -> str:
    """Get the current active user profile.

    Returns:
        str: The current profile name (e.g., 'alice', 'bob', 'default')
    """
    return _current_profile


def get_settings_for_profile(profile: str) -> MoneyBinSettings:
    """Get settings for a specific user profile.

    This is an explicit function for getting settings for a specific profile
    without changing the current profile.

    Args:
        profile: User profile name (e.g., 'alice', 'bob', 'household')

    Returns:
        MoneyBinSettings: The configuration instance for the specified profile

    Raises:
        ValueError: If required configuration is missing or invalid
    """
    import re

    if not profile:
        raise ValueError("Profile name cannot be empty")

    # Validate profile name is safe for filenames
    if not re.match(r"^[a-zA-Z0-9_-]+$", profile):
        raise ValueError(
            f"Invalid profile: {profile}. "
            "Profile name must contain only alphanumeric characters, dashes, and underscores"
        )

    return get_settings(profile)


def reload_settings(profile: str | None = None) -> MoneyBinSettings:
    """Reload settings from environment variables.

    This function forces a reload of the configuration, useful for testing
    or when environment variables change at runtime.

    Args:
        profile: Profile to reload. If None, reloads current profile.

    Returns:
        MoneyBinSettings: The reloaded configuration instance
    """
    global _settings_cache, _current_profile

    if profile is None:
        profile = _current_profile

    # Clear cached settings for this profile
    if profile in _settings_cache:
        del _settings_cache[profile]

    return get_settings(profile)


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


def get_plaid_config() -> PlaidConfig:
    """Get the Plaid configuration for the current profile.

    Returns:
        PlaidConfig: The Plaid configuration
    """
    return get_settings().plaid


def get_logging_config() -> LoggingConfig:
    """Get the logging configuration for the current profile.

    Returns:
        LoggingConfig: The logging configuration
    """
    return get_settings().logging
