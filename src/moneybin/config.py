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
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="MONEYBIN_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",  # Allow extra env vars that don't match our schema
        frozen=True,
    )

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

    def __init__(self, **kwargs: Any):
        """Initialize settings with environment variable overrides."""
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


# Global settings instance - lazy loaded
_settings: MoneyBinSettings | None = None


def get_settings() -> MoneyBinSettings:
    """Get the global settings instance.

    This function provides a singleton pattern for accessing configuration
    throughout the application. Settings are loaded once and cached.

    Returns:
        MoneyBinSettings: The global configuration instance

    Raises:
        ValueError: If required configuration is missing or invalid
    """
    global _settings

    if _settings is None:
        try:
            _settings = MoneyBinSettings()
            _settings.validate_required_credentials()

            # Create directories if configured to do so
            if _settings.database.create_dirs:
                _settings.create_directories()

        except Exception as e:
            raise ValueError(f"Configuration error: {e}") from e

    return _settings


def reload_settings() -> MoneyBinSettings:
    """Reload settings from environment variables.

    This function forces a reload of the configuration, useful for testing
    or when environment variables change at runtime.

    Returns:
        MoneyBinSettings: The reloaded configuration instance
    """
    global _settings
    _settings = None
    return get_settings()


# Convenience functions for common configuration access
def get_database_path() -> Path:
    """Get the configured database path."""
    return get_settings().database.path


def get_raw_data_path() -> Path:
    """Get the configured raw data path."""
    return get_settings().data.raw_data_path


def get_plaid_config() -> PlaidConfig:
    """Get the Plaid configuration."""
    return get_settings().plaid


def get_logging_config() -> LoggingConfig:
    """Get the logging configuration."""
    return get_settings().logging
