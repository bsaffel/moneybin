"""Secure secrets management utilities for MoneyBin application.

This module provides secure credential management following security best practices
for financial applications, including environment variable validation and future
encrypted storage capabilities.
"""

import logging
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, field_validator

logger = logging.getLogger(__name__)


class SecureConfig(BaseModel):
    """Base class for secure configuration management."""

    model_config = ConfigDict(
        frozen=True,  # Immutable after creation
        validate_assignment=True,  # Validate on assignment
        extra="forbid",  # Prevent additional fields
    )


class DatabaseCredentials(SecureConfig):
    """Secure database connection credentials."""

    database_path: str = Field(..., description="Path to DuckDB database file")
    backup_path: str | None = Field(None, description="Path for database backups")
    encryption_key: str | None = Field(None, description="Database encryption key")

    @classmethod
    def from_environment(cls) -> "DatabaseCredentials":
        """Load database credentials from environment variables.

        Returns:
            DatabaseCredentials: Validated database configuration
        """
        return cls(
            database_path=os.getenv("DUCKDB_PATH", "data/duckdb/financial.db"),
            backup_path=os.getenv("DUCKDB_BACKUP_PATH"),
            encryption_key=os.getenv("DUCKDB_ENCRYPTION_KEY"),
        )


class APICredentials(SecureConfig):
    """Base class for API credential management."""

    def validate_credentials(self) -> bool:
        """Validate that all required credentials are present.

        Returns:
            bool: True if all credentials are valid
        """
        raise NotImplementedError("Subclasses must implement validate_credentials")


class PlaidCredentials(APICredentials):
    """Secure Plaid API credentials management."""

    client_id: str = Field(..., description="Plaid client ID")
    secret: str = Field(..., description="Plaid secret key")
    environment: str = Field(default="sandbox", description="Plaid environment")
    webhook_url: str | None = Field(
        None, description="Webhook URL for Plaid notifications"
    )

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate Plaid environment setting."""
        valid_envs = ["sandbox", "development", "production"]
        if v not in valid_envs:
            raise ValueError(f"Environment must be one of {valid_envs}")
        return v

    @classmethod
    def from_environment(cls) -> "PlaidCredentials":
        """Load Plaid credentials from environment variables.

        Returns:
            PlaidCredentials: Validated Plaid configuration

        Raises:
            ValueError: If required environment variables are missing
        """
        client_id = os.getenv("PLAID_CLIENT_ID")
        secret = os.getenv("PLAID_SECRET")
        environment = os.getenv("PLAID_ENV", "sandbox")
        webhook_url = os.getenv("PLAID_WEBHOOK_URL")

        if not client_id:
            raise ValueError("PLAID_CLIENT_ID environment variable is required")
        if not secret:
            raise ValueError("PLAID_SECRET environment variable is required")

        return cls(
            client_id=client_id,
            secret=secret,
            environment=environment,
            webhook_url=webhook_url,
        )

    def validate_credentials(self) -> bool:
        """Validate that Plaid credentials are properly configured.

        Returns:
            bool: True if credentials are valid
        """
        return bool(self.client_id and self.secret)


class QuickBooksCredentials(APICredentials):
    """Secure QuickBooks API credentials management."""

    client_id: str = Field(..., description="QuickBooks client ID")
    client_secret: str = Field(..., description="QuickBooks client secret")
    redirect_uri: str = Field(..., description="OAuth redirect URI")
    scope: str = Field(
        default="com.intuit.quickbooks.accounting", description="API scope"
    )
    discovery_document: str = Field(
        default="https://appcenter.intuit.com/api/v1/connection/oauth2",
        description="OAuth discovery document URL",
    )

    @classmethod
    def from_environment(cls) -> "QuickBooksCredentials":
        """Load QuickBooks credentials from environment variables.

        Returns:
            QuickBooksCredentials: Validated QuickBooks configuration

        Raises:
            ValueError: If required environment variables are missing
        """
        client_id = os.getenv("QUICKBOOKS_CLIENT_ID")
        client_secret = os.getenv("QUICKBOOKS_CLIENT_SECRET")
        redirect_uri = os.getenv("QUICKBOOKS_REDIRECT_URI")
        scope = os.getenv("QUICKBOOKS_SCOPE", "com.intuit.quickbooks.accounting")

        if not client_id:
            raise ValueError("QUICKBOOKS_CLIENT_ID environment variable is required")
        if not client_secret:
            raise ValueError(
                "QUICKBOOKS_CLIENT_SECRET environment variable is required"
            )
        if not redirect_uri:
            raise ValueError("QUICKBOOKS_REDIRECT_URI environment variable is required")

        return cls(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            scope=scope,
        )

    def validate_credentials(self) -> bool:
        """Validate that QuickBooks credentials are properly configured.

        Returns:
            bool: True if credentials are valid
        """
        return bool(self.client_id and self.client_secret and self.redirect_uri)


@dataclass
class AccessTokenStore:
    """Secure storage for API access tokens.

    In production, this should use encrypted storage or a secure key management service.
    For development, it uses environment variables with clear naming conventions.
    """

    def get_plaid_tokens(self) -> dict[str, str]:
        """Retrieve all Plaid access tokens from environment.

        Returns:
            Dict[str, str]: Mapping of institution names to access tokens
        """
        tokens: dict[str, str] = {}
        for key, value in os.environ.items():
            if key.startswith("PLAID_TOKEN_"):
                institution_name = (
                    key.replace("PLAID_TOKEN_", "").lower().replace("_", " ")
                )
                tokens[institution_name] = value

        logger.info(f"Found {len(tokens)} Plaid institution tokens")
        return tokens

    def get_quickbooks_tokens(self) -> dict[str, str]:
        """Retrieve QuickBooks access tokens from environment.

        Returns:
            Dict[str, str]: QuickBooks access and refresh tokens
        """
        return {
            "access_token": os.getenv("QUICKBOOKS_ACCESS_TOKEN", ""),
            "refresh_token": os.getenv("QUICKBOOKS_REFRESH_TOKEN", ""),
            "company_id": os.getenv("QUICKBOOKS_COMPANY_ID", ""),
        }

    def store_token(self, service: str, institution: str, token: str) -> None:
        """Store an access token securely.

        Args:
            service: Service name (e.g., 'plaid', 'quickbooks')
            institution: Institution name
            token: Access token to store

        Note:
            In production, implement encrypted storage here
        """
        env_var_name = (
            f"{service.upper()}_TOKEN_{institution.upper().replace(' ', '_')}"
        )
        logger.info(f"Token should be stored as environment variable: {env_var_name}")

        # TODO: Implement encrypted token storage for production
        # For now, log instructions for manual setup
        logger.warning("Manual token storage required - add to .env file")


class SecretsManager:
    """Central secrets management for MoneyBin application.

    Provides secure access to all API credentials and sensitive configuration
    following security best practices for financial applications.
    """

    def __init__(self, env_file: Path | None = None):
        """Initialize secrets manager.

        Args:
            env_file: Optional path to .env file
        """
        if env_file and env_file.exists():
            load_dotenv(env_file)
        else:
            load_dotenv()  # Load from default .env file

        self.token_store = AccessTokenStore()

        # Validate environment setup
        self._validate_environment_setup()

    def _validate_environment_setup(self) -> None:
        """Validate that the environment is properly configured for security."""
        # Check for common security misconfigurations
        secret = os.getenv("PLAID_SECRET")
        if secret and secret.startswith("sk_"):
            logger.warning(
                "Plaid secret appears to be a production key - ensure you're using the correct environment"
            )

        # Ensure we're not in debug mode for production
        if (
            os.getenv("DEBUG", "").lower() in ("true", "1")
            and os.getenv("PLAID_ENV") == "production"
        ):
            logger.error("DEBUG mode should not be enabled in production environment")

    def get_plaid_credentials(self) -> PlaidCredentials:
        """Get validated Plaid API credentials.

        Returns:
            PlaidCredentials: Validated Plaid configuration
        """
        return PlaidCredentials.from_environment()

    def get_quickbooks_credentials(self) -> QuickBooksCredentials:
        """Get validated QuickBooks API credentials.

        Returns:
            QuickBooksCredentials: Validated QuickBooks configuration
        """
        return QuickBooksCredentials.from_environment()

    def get_database_credentials(self) -> DatabaseCredentials:
        """Get validated database credentials.

        Returns:
            DatabaseCredentials: Validated database configuration
        """
        return DatabaseCredentials.from_environment()

    def get_all_access_tokens(self) -> dict[str, dict[str, str]]:
        """Get all access tokens organized by service.

        Returns:
            Dict[str, Dict[str, str]]: Service -> institution/type -> token mapping
        """
        return {
            "plaid": self.token_store.get_plaid_tokens(),
            "quickbooks": self.token_store.get_quickbooks_tokens(),
        }

    def validate_all_credentials(self) -> dict[str, bool]:
        """Validate all configured credentials.

        Returns:
            Dict[str, bool]: Service -> validation status mapping
        """
        validation_results = {}

        try:
            plaid_creds = self.get_plaid_credentials()
            validation_results["plaid"] = plaid_creds.validate_credentials()
        except Exception as e:
            logger.error(f"Plaid credentials validation failed: {e}")
            validation_results["plaid"] = False

        try:
            qb_creds = self.get_quickbooks_credentials()
            validation_results["quickbooks"] = qb_creds.validate_credentials()
        except Exception as e:
            logger.warning(f"QuickBooks credentials not configured: {e}")
            validation_results["quickbooks"] = False

        try:
            db_creds = self.get_database_credentials()
            validation_results["database"] = bool(db_creds.database_path)
        except Exception as e:
            logger.error(f"Database credentials validation failed: {e}")
            validation_results["database"] = False

        return validation_results


def setup_secure_environment() -> None:
    """Set up secure environment configuration for MoneyBin.

    This function creates necessary directories and sample configuration files
    for secure credential management.
    """
    # Create necessary directories
    directories = [
        Path("config"),
        Path("data/raw/plaid"),
        Path("data/processed"),
        Path("logs"),
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {directory}")

    # Create sample .env file if it doesn't exist
    env_file = Path(".env")
    if not env_file.exists():
        sample_content = """# MoneyBin Secure Configuration
# Copy this file to .env and fill in your actual credentials

# Plaid API Configuration
# Get these from https://dashboard.plaid.com/team/keys
PLAID_CLIENT_ID=your_plaid_client_id_here
PLAID_SECRET=your_plaid_secret_here
PLAID_ENV=sandbox  # sandbox, development, or production

# Institution Access Tokens (add after linking accounts through Plaid Link)
# PLAID_TOKEN_WELLS_FARGO=access-sandbox-xxx
# PLAID_TOKEN_CHASE=access-sandbox-yyy
# PLAID_TOKEN_CAPITAL_ONE=access-sandbox-zzz
# PLAID_TOKEN_FIDELITY=access-sandbox-aaa
# PLAID_TOKEN_ETRADE=access-sandbox-bbb

# QuickBooks API Configuration (optional)
# Get these from https://developer.intuit.com/app/developer/myapps
# QUICKBOOKS_CLIENT_ID=your_quickbooks_client_id
# QUICKBOOKS_CLIENT_SECRET=your_quickbooks_client_secret
# QUICKBOOKS_REDIRECT_URI=http://localhost:8080/callback
# QUICKBOOKS_ACCESS_TOKEN=your_access_token
# QUICKBOOKS_REFRESH_TOKEN=your_refresh_token
# QUICKBOOKS_COMPANY_ID=your_company_id

# Database Configuration
DUCKDB_PATH=data/duckdb/financial.db
# DUCKDB_BACKUP_PATH=data/backups/
# DUCKDB_ENCRYPTION_KEY=your_encryption_key_here

# Application Configuration
LOG_LEVEL=INFO
LOG_TO_FILE=true
LOG_FILE_PATH=logs/moneybin.log
LOG_MAX_FILE_SIZE_MB=50
LOG_BACKUP_COUNT=5
DEBUG=false

# Security Configuration
# WEBHOOK_SECRET=your_webhook_secret_for_plaid_notifications
"""

        with open(env_file, "w") as f:
            f.write(sample_content)

        logger.info(f"Created secure environment template at {env_file}")
        logger.warning("IMPORTANT: Fill in your actual credentials in .env file")
        logger.warning("NEVER commit .env file to version control")
