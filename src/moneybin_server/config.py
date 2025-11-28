"""Server configuration for MoneyBin Sync service.

This module provides configuration settings for the hosted MoneyBin Sync server,
including API credentials, server settings, and connector configurations.
"""

import os
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PlaidConfig(BaseModel):
    """Plaid API configuration (server-side only).

    These credentials are never shipped to clients and remain on hosted infrastructure.
    """

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


class ServerConfig(BaseSettings):
    """Main server configuration for MoneyBin Sync.

    This configuration is used by the hosted sync service and includes
    all connector credentials and server settings.
    """

    model_config = SettingsConfigDict(
        env_file=".env.server",
        env_file_encoding="utf-8",
        env_prefix="MONEYBIN_SERVER_",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",
        frozen=True,
    )

    # Connector configurations
    plaid: PlaidConfig = Field(
        default_factory=lambda: PlaidConfig(
            client_id="", secret="", environment="sandbox"
        )
    )

    # Future: Add more connector configs
    # yodlee: YodleeConfig = Field(default_factory=YodleeConfig)

    # Server settings (future use)
    debug: bool = Field(default=False, description="Enable debug mode")
    environment: Literal["development", "staging", "production"] = Field(
        default="development", description="Server environment"
    )

    def __init__(self, **kwargs: Any):
        """Initialize server settings with environment variable overrides."""
        # Handle legacy Plaid environment variables for backward compatibility
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

    def validate_required_credentials(self) -> None:
        """Validate that required connector credentials are present."""
        errors: list[str] = []

        if not self.plaid.client_id:
            errors.append("PLAID_CLIENT_ID is required")
        if not self.plaid.secret:
            errors.append("PLAID_SECRET is required")

        if errors:
            raise ValueError(f"Missing required configuration: {', '.join(errors)}")


# Global server settings instance
_server_settings: ServerConfig | None = None


def get_server_settings() -> ServerConfig:
    """Get the server settings instance.

    Returns:
        ServerConfig: The server configuration instance

    Raises:
        ValueError: If required configuration is missing or invalid
    """
    global _server_settings

    if _server_settings is None:
        _server_settings = ServerConfig()
        _server_settings.validate_required_credentials()

    return _server_settings


def reload_server_settings() -> ServerConfig:
    """Reload server settings from environment variables.

    Useful for testing or when environment variables change at runtime.

    Returns:
        ServerConfig: The reloaded server configuration instance
    """
    global _server_settings
    _server_settings = None
    return get_server_settings()


# Convenience functions for common configuration access
def get_plaid_config() -> PlaidConfig:
    """Get the Plaid configuration for the server.

    Returns:
        PlaidConfig: The Plaid configuration
    """
    return get_server_settings().plaid
