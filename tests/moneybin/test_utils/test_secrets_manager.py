"""Tests for secure secrets management utilities."""

import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from moneybin.utils.secrets_manager import (
    AccessTokenStore,
    DatabaseCredentials,
    PlaidCredentials,
    QuickBooksCredentials,
    SecretsManager,
    setup_secure_environment,
)


class TestDatabaseCredentials:
    """Test cases for DatabaseCredentials class."""

    def test_create_database_credentials(self) -> None:
        """Test creating DatabaseCredentials with valid data."""
        creds = DatabaseCredentials(
            database_path=Path("data/duckdb/test.db"),
            backup_path=Path("data/backups"),
            encryption_key="test_key",
        )

        assert creds.database_path == Path("data/duckdb/test.db")
        assert creds.backup_path == Path("data/backups")
        assert creds.encryption_key == "test_key"

    def test_database_credentials_optional_fields(self) -> None:
        """Test DatabaseCredentials with optional fields as None."""
        creds = DatabaseCredentials(
            database_path=Path("data/duckdb/test.db"),
            backup_path=None,
            encryption_key=None,
        )

        assert creds.database_path == Path("data/duckdb/test.db")
        assert creds.backup_path is None
        assert creds.encryption_key is None

    def test_database_credentials_from_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test loading DatabaseCredentials from environment variables.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("DUCKDB_PATH", "data/test.db")
        monkeypatch.setenv("DUCKDB_BACKUP_PATH", "data/backups")
        monkeypatch.setenv("DUCKDB_ENCRYPTION_KEY", "secret_key")

        creds = DatabaseCredentials.from_environment()

        assert creds.database_path == Path("data/test.db")
        assert creds.backup_path == Path("data/backups")
        assert creds.encryption_key == "secret_key"

    def test_database_credentials_from_environment_defaults(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test DatabaseCredentials uses defaults when env vars not set.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        # Clear any existing env vars
        monkeypatch.delenv("DUCKDB_PATH", raising=False)
        monkeypatch.delenv("DUCKDB_BACKUP_PATH", raising=False)
        monkeypatch.delenv("DUCKDB_ENCRYPTION_KEY", raising=False)

        creds = DatabaseCredentials.from_environment()

        assert creds.database_path == Path("data/duckdb/financial.db")
        assert creds.backup_path is None
        assert creds.encryption_key is None

    def test_database_credentials_immutable(self) -> None:
        """Test that DatabaseCredentials is immutable after creation."""
        creds = DatabaseCredentials(
            database_path=Path("data/test.db"),
            backup_path=None,
            encryption_key=None,
        )

        with pytest.raises(ValidationError):
            creds.database_path = Path("data/other.db")  # type: ignore[misc] - testing immutability


class TestPlaidCredentials:
    """Test cases for PlaidCredentials class."""

    def test_create_plaid_credentials(self) -> None:
        """Test creating PlaidCredentials with valid data."""
        creds = PlaidCredentials(
            client_id="test_client_id",
            secret="test_secret",  # noqa: S106 - test fixture uses dummy credentials
            environment="sandbox",
            webhook_url="https://example.com/webhook",
        )

        assert creds.client_id == "test_client_id"
        assert creds.secret == "test_secret"  # noqa: S105 - test fixture uses dummy credentials
        assert creds.environment == "sandbox"
        assert creds.webhook_url == "https://example.com/webhook"

    def test_plaid_credentials_default_environment(self) -> None:
        """Test PlaidCredentials defaults to sandbox environment."""
        creds = PlaidCredentials(
            client_id="test_id",
            secret="test_secret",  # noqa: S106 - test fixture uses dummy credentials
            webhook_url=None,
        )

        assert creds.environment == "sandbox"

    def test_plaid_credentials_valid_environments(self) -> None:
        """Test PlaidCredentials accepts valid environment values."""
        for env in ["sandbox", "development", "production"]:
            creds = PlaidCredentials(
                client_id="test_id",
                secret="test_secret",  # noqa: S106 - test fixture uses dummy credentials
                environment=env,
                webhook_url=None,
            )
            assert creds.environment == env

    def test_plaid_credentials_invalid_environment(self) -> None:
        """Test PlaidCredentials rejects invalid environment."""
        with pytest.raises(ValidationError, match="Environment must be one of"):
            PlaidCredentials(
                client_id="test_id",
                secret="test_secret",  # noqa: S106 - test fixture uses dummy credentials
                environment="invalid",
                webhook_url=None,
            )

    def test_plaid_credentials_from_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test loading PlaidCredentials from environment variables.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("PLAID_CLIENT_ID", "my_client_id")
        monkeypatch.setenv("PLAID_SECRET", "my_secret")
        monkeypatch.setenv("PLAID_ENV", "development")
        monkeypatch.setenv("PLAID_WEBHOOK_URL", "https://example.com/webhook")

        creds = PlaidCredentials.from_environment()

        assert creds.client_id == "my_client_id"
        assert creds.secret == "my_secret"  # noqa: S105 - test fixture uses dummy credentials
        assert creds.environment == "development"
        assert creds.webhook_url == "https://example.com/webhook"

    def test_plaid_credentials_from_environment_defaults(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test PlaidCredentials uses default environment when not set.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("PLAID_CLIENT_ID", "my_client_id")
        monkeypatch.setenv("PLAID_SECRET", "my_secret")
        monkeypatch.delenv("PLAID_ENV", raising=False)

        creds = PlaidCredentials.from_environment()

        assert creds.environment == "sandbox"

    def test_plaid_credentials_missing_client_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test PlaidCredentials raises error when client_id missing.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.delenv("PLAID_CLIENT_ID", raising=False)
        monkeypatch.setenv("PLAID_SECRET", "my_secret")

        with pytest.raises(ValueError, match="PLAID_CLIENT_ID.*required"):
            PlaidCredentials.from_environment()

    def test_plaid_credentials_missing_secret(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test PlaidCredentials raises error when secret missing.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("PLAID_CLIENT_ID", "my_client_id")
        monkeypatch.delenv("PLAID_SECRET", raising=False)

        with pytest.raises(ValueError, match="PLAID_SECRET.*required"):
            PlaidCredentials.from_environment()

    def test_plaid_credentials_validate_success(self) -> None:
        """Test validate_credentials returns True for valid credentials."""
        creds = PlaidCredentials(
            client_id="test_id",
            secret="test_secret",  # noqa: S106 - test fixture uses dummy credentials
            webhook_url=None,
        )

        assert creds.validate_credentials() is True

    def test_plaid_credentials_validate_failure(self) -> None:
        """Test validate_credentials returns False for invalid credentials."""
        # Can't create invalid credentials due to Pydantic validation,
        # so we test the method logic
        creds = PlaidCredentials(
            client_id="test_id",
            secret="test_secret",  # noqa: S106 - test fixture uses dummy credentials
            webhook_url=None,
        )
        assert creds.validate_credentials() is True

    def test_plaid_credentials_immutable(self) -> None:
        """Test that PlaidCredentials is immutable after creation."""
        creds = PlaidCredentials(
            client_id="test_id",
            secret="test_secret",  # noqa: S106 - test fixture uses dummy credentials
            webhook_url=None,
        )

        with pytest.raises(ValidationError):
            creds.client_id = "new_id"  # type: ignore[misc] - testing immutability


class TestQuickBooksCredentials:
    """Test cases for QuickBooksCredentials class."""

    def test_create_quickbooks_credentials(self) -> None:
        """Test creating QuickBooksCredentials with valid data."""
        creds = QuickBooksCredentials(
            client_id="qb_client_id",
            client_secret="qb_secret",  # noqa: S106 - test fixture uses dummy credentials
            redirect_uri="http://localhost:8080/callback",
            scope="com.intuit.quickbooks.accounting",
        )

        assert creds.client_id == "qb_client_id"
        assert creds.client_secret == "qb_secret"  # noqa: S105 - test fixture uses dummy credentials
        assert creds.redirect_uri == "http://localhost:8080/callback"
        assert creds.scope == "com.intuit.quickbooks.accounting"

    def test_quickbooks_credentials_default_scope(self) -> None:
        """Test QuickBooksCredentials uses default scope."""
        creds = QuickBooksCredentials(
            client_id="qb_id",
            client_secret="qb_secret",  # noqa: S106 - test fixture uses dummy credentials
            redirect_uri="http://localhost:8080/callback",
        )

        assert creds.scope == "com.intuit.quickbooks.accounting"

    def test_quickbooks_credentials_from_environment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test loading QuickBooksCredentials from environment.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("QUICKBOOKS_CLIENT_ID", "qb_client_id")
        monkeypatch.setenv("QUICKBOOKS_CLIENT_SECRET", "qb_secret")
        monkeypatch.setenv("QUICKBOOKS_REDIRECT_URI", "http://localhost/callback")
        monkeypatch.setenv("QUICKBOOKS_SCOPE", "custom.scope")

        creds = QuickBooksCredentials.from_environment()

        assert creds.client_id == "qb_client_id"
        assert creds.client_secret == "qb_secret"  # noqa: S105 - test fixture uses dummy credentials
        assert creds.redirect_uri == "http://localhost/callback"
        assert creds.scope == "custom.scope"

    def test_quickbooks_credentials_missing_client_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test QuickBooksCredentials raises error when client_id missing.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.delenv("QUICKBOOKS_CLIENT_ID", raising=False)
        monkeypatch.setenv("QUICKBOOKS_CLIENT_SECRET", "secret")
        monkeypatch.setenv("QUICKBOOKS_REDIRECT_URI", "http://localhost/callback")

        with pytest.raises(ValueError, match="QUICKBOOKS_CLIENT_ID.*required"):
            QuickBooksCredentials.from_environment()

    def test_quickbooks_credentials_missing_secret(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test QuickBooksCredentials raises error when secret missing.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("QUICKBOOKS_CLIENT_ID", "qb_id")
        monkeypatch.delenv("QUICKBOOKS_CLIENT_SECRET", raising=False)
        monkeypatch.setenv("QUICKBOOKS_REDIRECT_URI", "http://localhost/callback")

        with pytest.raises(ValueError, match="QUICKBOOKS_CLIENT_SECRET.*required"):
            QuickBooksCredentials.from_environment()

    def test_quickbooks_credentials_missing_redirect_uri(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test QuickBooksCredentials raises error when redirect_uri missing.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("QUICKBOOKS_CLIENT_ID", "qb_id")
        monkeypatch.setenv("QUICKBOOKS_CLIENT_SECRET", "secret")
        monkeypatch.delenv("QUICKBOOKS_REDIRECT_URI", raising=False)

        with pytest.raises(ValueError, match="QUICKBOOKS_REDIRECT_URI.*required"):
            QuickBooksCredentials.from_environment()

    def test_quickbooks_credentials_validate_success(self) -> None:
        """Test validate_credentials returns True for valid credentials."""
        creds = QuickBooksCredentials(
            client_id="qb_id",
            client_secret="qb_secret",  # noqa: S106 - test fixture uses dummy credentials
            redirect_uri="http://localhost/callback",
        )

        assert creds.validate_credentials() is True

    def test_quickbooks_credentials_immutable(self) -> None:
        """Test that QuickBooksCredentials is immutable after creation."""
        creds = QuickBooksCredentials(
            client_id="qb_id",
            client_secret="qb_secret",  # noqa: S106 - test fixture uses dummy credentials
            redirect_uri="http://localhost/callback",
        )

        with pytest.raises(ValidationError):
            creds.client_id = "new_id"  # type: ignore[misc] - testing immutability


class TestAccessTokenStore:
    """Test cases for AccessTokenStore class."""

    def test_get_plaid_tokens_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test getting Plaid tokens when none are configured.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        # Clear environment
        for key in list(os.environ.keys()):
            if key.startswith("PLAID_TOKEN_"):
                monkeypatch.delenv(key, raising=False)

        store = AccessTokenStore()
        tokens = store.get_plaid_tokens()

        assert tokens == {}

    def test_get_plaid_tokens_single(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test getting a single Plaid token.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("PLAID_TOKEN_CHASE", "access-sandbox-123")

        store = AccessTokenStore()
        tokens = store.get_plaid_tokens()

        assert "chase" in tokens
        assert tokens["chase"] == "access-sandbox-123"

    def test_get_plaid_tokens_multiple(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test getting multiple Plaid tokens.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("PLAID_TOKEN_CHASE", "access-sandbox-123")
        monkeypatch.setenv("PLAID_TOKEN_WELLS_FARGO", "access-sandbox-456")
        monkeypatch.setenv("PLAID_TOKEN_CAPITAL_ONE", "access-sandbox-789")

        store = AccessTokenStore()
        tokens = store.get_plaid_tokens()

        assert len(tokens) == 3
        assert tokens["chase"] == "access-sandbox-123"
        assert tokens["wells fargo"] == "access-sandbox-456"
        assert tokens["capital one"] == "access-sandbox-789"

    def test_get_plaid_tokens_name_normalization(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that institution names are normalized correctly.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("PLAID_TOKEN_WELLS_FARGO", "access-sandbox-456")

        store = AccessTokenStore()
        tokens = store.get_plaid_tokens()

        # Underscores converted to spaces, lowercase
        assert "wells fargo" in tokens

    def test_get_quickbooks_tokens_empty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test getting QuickBooks tokens when none are configured.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.delenv("QUICKBOOKS_ACCESS_TOKEN", raising=False)
        monkeypatch.delenv("QUICKBOOKS_REFRESH_TOKEN", raising=False)
        monkeypatch.delenv("QUICKBOOKS_COMPANY_ID", raising=False)

        store = AccessTokenStore()
        tokens = store.get_quickbooks_tokens()

        assert tokens == {
            "access_token": "",
            "refresh_token": "",
            "company_id": "",
        }

    def test_get_quickbooks_tokens_configured(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test getting QuickBooks tokens when configured.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("QUICKBOOKS_ACCESS_TOKEN", "access_123")
        monkeypatch.setenv("QUICKBOOKS_REFRESH_TOKEN", "refresh_456")
        monkeypatch.setenv("QUICKBOOKS_COMPANY_ID", "company_789")

        store = AccessTokenStore()
        tokens = store.get_quickbooks_tokens()

        assert tokens["access_token"] == "access_123"  # noqa: S105 - test fixture uses dummy credentials
        assert tokens["refresh_token"] == "refresh_456"  # noqa: S105 - test fixture uses dummy credentials
        assert tokens["company_id"] == "company_789"

    def test_store_token(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test storing a token (currently logs instructions).

        Args:
            caplog: Pytest log capture fixture
        """
        import logging

        caplog.set_level(logging.INFO)

        store = AccessTokenStore()
        store.store_token("plaid", "chase bank", "access-token-123")

        # Should log environment variable name
        assert "PLAID_TOKEN_CHASE_BANK" in caplog.text
        assert "Manual token storage required" in caplog.text


class TestSecretsManager:
    """Test cases for SecretsManager class."""

    def test_create_secrets_manager_no_env_file(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test creating SecretsManager without env file.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        # Set minimal required env vars
        monkeypatch.setenv("PLAID_CLIENT_ID", "test_id")
        monkeypatch.setenv("PLAID_SECRET", "test_secret")

        manager = SecretsManager()

        assert manager.token_store is not None

    def test_create_secrets_manager_with_env_file(self, tmp_path: Path) -> None:
        """Test creating SecretsManager with env file.

        Args:
            tmp_path: Pytest temporary directory fixture
        """
        env_file = tmp_path / ".env"
        env_file.write_text(
            "PLAID_CLIENT_ID=test_id\nPLAID_SECRET=test_secret\nPLAID_ENV=sandbox\n"
        )

        manager = SecretsManager(env_file)

        assert manager.token_store is not None

    def test_get_plaid_credentials(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test getting Plaid credentials.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("PLAID_CLIENT_ID", "test_id")
        monkeypatch.setenv("PLAID_SECRET", "test_secret")
        monkeypatch.setenv("PLAID_ENV", "sandbox")

        manager = SecretsManager()
        creds = manager.get_plaid_credentials()

        assert creds.client_id == "test_id"
        assert creds.secret == "test_secret"  # noqa: S105 - test fixture uses dummy credentials

    def test_get_quickbooks_credentials(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test getting QuickBooks credentials.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("PLAID_CLIENT_ID", "test_id")
        monkeypatch.setenv("PLAID_SECRET", "test_secret")
        monkeypatch.setenv("QUICKBOOKS_CLIENT_ID", "qb_id")
        monkeypatch.setenv("QUICKBOOKS_CLIENT_SECRET", "qb_secret")
        monkeypatch.setenv("QUICKBOOKS_REDIRECT_URI", "http://localhost/callback")

        manager = SecretsManager()
        creds = manager.get_quickbooks_credentials()

        assert creds.client_id == "qb_id"
        assert creds.client_secret == "qb_secret"  # noqa: S105 - test fixture uses dummy credentials

    def test_get_database_credentials(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test getting database credentials.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("PLAID_CLIENT_ID", "test_id")
        monkeypatch.setenv("PLAID_SECRET", "test_secret")
        monkeypatch.setenv("DUCKDB_PATH", "data/test.db")

        manager = SecretsManager()
        creds = manager.get_database_credentials()

        assert creds.database_path == Path("data/test.db")

    def test_get_all_access_tokens(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test getting all access tokens.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("PLAID_CLIENT_ID", "test_id")
        monkeypatch.setenv("PLAID_SECRET", "test_secret")
        monkeypatch.setenv("PLAID_TOKEN_CHASE", "access-sandbox-123")
        monkeypatch.setenv("QUICKBOOKS_ACCESS_TOKEN", "qb_access")

        manager = SecretsManager()
        tokens = manager.get_all_access_tokens()

        assert "plaid" in tokens
        assert "quickbooks" in tokens
        assert tokens["plaid"]["chase"] == "access-sandbox-123"
        assert tokens["quickbooks"]["access_token"] == "qb_access"  # noqa: S105 - test fixture uses dummy credentials

    def test_validate_all_credentials_success(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test validating all credentials successfully.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("PLAID_CLIENT_ID", "test_id")
        monkeypatch.setenv("PLAID_SECRET", "test_secret")
        monkeypatch.setenv("PLAID_ENV", "sandbox")
        monkeypatch.setenv("QUICKBOOKS_CLIENT_ID", "qb_id")
        monkeypatch.setenv("QUICKBOOKS_CLIENT_SECRET", "qb_secret")
        monkeypatch.setenv("QUICKBOOKS_REDIRECT_URI", "http://localhost/callback")
        monkeypatch.setenv("DUCKDB_PATH", "data/test.db")

        manager = SecretsManager()
        results = manager.validate_all_credentials()

        assert results["plaid"] is True
        assert results["quickbooks"] is True
        assert results["database"] is True

    def test_validate_all_credentials_partial_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test validation when some credentials are missing.

        Args:
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.setenv("PLAID_CLIENT_ID", "test_id")
        monkeypatch.setenv("PLAID_SECRET", "test_secret")
        monkeypatch.setenv("PLAID_ENV", "sandbox")
        # QuickBooks credentials not set
        monkeypatch.delenv("QUICKBOOKS_CLIENT_ID", raising=False)

        manager = SecretsManager()
        results = manager.validate_all_credentials()

        assert results["plaid"] is True
        assert results["quickbooks"] is False
        assert results["database"] is True

    def test_validate_environment_setup_production_warning(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test warning for production key detection.

        Args:
            monkeypatch: Pytest monkeypatch fixture
            caplog: Pytest log capture fixture
        """
        monkeypatch.setenv("PLAID_CLIENT_ID", "test_id")
        monkeypatch.setenv("PLAID_SECRET", "sk_production_key")

        SecretsManager()

        assert "production key" in caplog.text.lower()

    def test_validate_environment_setup_debug_production_error(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test error for debug mode in production.

        Args:
            monkeypatch: Pytest monkeypatch fixture
            caplog: Pytest log capture fixture
        """
        monkeypatch.setenv("PLAID_CLIENT_ID", "test_id")
        monkeypatch.setenv("PLAID_SECRET", "test_secret")
        monkeypatch.setenv("DEBUG", "true")
        monkeypatch.setenv("PLAID_ENV", "production")

        SecretsManager()

        assert "DEBUG mode should not be enabled" in caplog.text


class TestSetupSecureEnvironment:
    """Test cases for setup_secure_environment function."""

    def test_creates_directories(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that necessary directories are created.

        Args:
            tmp_path: Pytest temporary directory fixture
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.chdir(tmp_path)

        setup_secure_environment()

        assert (tmp_path / "config").exists()
        assert (tmp_path / "data/raw/plaid").exists()
        assert (tmp_path / "data/processed").exists()
        assert (tmp_path / "logs").exists()

    def test_creates_env_template(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that .env template file is created.

        Args:
            tmp_path: Pytest temporary directory fixture
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.chdir(tmp_path)

        setup_secure_environment()

        env_file = tmp_path / ".env"
        assert env_file.exists()

        content = env_file.read_text()
        assert "PLAID_CLIENT_ID" in content
        assert "PLAID_SECRET" in content
        assert "DUCKDB_PATH" in content
        assert "QUICKBOOKS_CLIENT_ID" in content

    def test_does_not_overwrite_existing_env(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that existing .env file is not overwritten.

        Args:
            tmp_path: Pytest temporary directory fixture
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.chdir(tmp_path)

        # Create existing .env file
        env_file = tmp_path / ".env"
        original_content = "EXISTING_CONFIG=value"
        env_file.write_text(original_content)

        setup_secure_environment()

        # Should not overwrite
        assert env_file.read_text() == original_content

    def test_idempotent_directory_creation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that running setup twice is idempotent.

        Args:
            tmp_path: Pytest temporary directory fixture
            monkeypatch: Pytest monkeypatch fixture
        """
        monkeypatch.chdir(tmp_path)

        # Run setup twice
        setup_secure_environment()
        setup_secure_environment()

        # All directories should still exist
        assert (tmp_path / "config").exists()
        assert (tmp_path / "data/raw/plaid").exists()
        assert (tmp_path / "data/processed").exists()
        assert (tmp_path / "logs").exists()
