"""Plaid API extractor using straightforward SDK calls.

This module uses the Plaid Python SDK with minimal wrapping to fetch
accounts and transactions, returning simple tabular data structures.
"""

import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import polars as pl
from dotenv import load_dotenv
from plaid.api import plaid_api
from plaid.api_client import ApiClient
from plaid.configuration import Configuration
from plaid.exceptions import ApiException
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from pydantic import BaseModel, ConfigDict, Field

from .plaid_schemas import AccountSchema, TransactionSchema

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class PlaidCredentials(BaseModel):
    """Secure credential management for Plaid API access."""

    model_config = ConfigDict(frozen=True)

    client_id: str = Field(..., description="Plaid client ID")
    secret: str = Field(..., description="Plaid secret key")
    environment: str = Field(default="sandbox", description="Plaid environment")

    @classmethod
    def from_environment(cls) -> "PlaidCredentials":
        """Load credentials from environment variables securely."""
        client_id = os.getenv("PLAID_CLIENT_ID")
        secret = os.getenv("PLAID_SECRET")
        environment = os.getenv("PLAID_ENV", "sandbox")

        if not client_id:
            raise ValueError("PLAID_CLIENT_ID environment variable is required")
        if not secret:
            raise ValueError("PLAID_SECRET environment variable is required")

        return cls(client_id=client_id, secret=secret, environment=environment)


@dataclass
class PlaidExtractionConfig:
    """Configuration for Plaid data extraction operations."""

    days_lookback: int = 365
    batch_size: int = 500
    max_retries: int = 3
    retry_delay: float = 1.0
    save_raw_data: bool = True
    raw_data_path: Path = Path("data/raw/plaid")


class PlaidExtractor:
    """Secure Plaid API client using modern SDK structure."""

    def __init__(self, config: PlaidExtractionConfig | None = None):
        """Initialize the Plaid extractor with secure credentials."""
        self.config = config or PlaidExtractionConfig()
        self.credentials = PlaidCredentials.from_environment()

        # Initialize Plaid client with modern SDK
        configuration = Configuration(
            host=self._get_plaid_environment(),
            api_key={
                "clientId": self.credentials.client_id,
                "secret": self.credentials.secret,
            },
        )
        api_client = ApiClient(configuration)
        # Type as Any to avoid pyright partial-unknowns from the SDK stubs
        self.client: Any = plaid_api.PlaidApi(api_client)

        # Ensure output directory exists
        self.config.raw_data_path.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Initialized Plaid extractor for {self.credentials.environment} environment"
        )

    def _get_plaid_environment(self) -> str:
        """Get the appropriate Plaid environment URL."""
        env_name = self.credentials.environment.lower()
        if env_name == "production":
            return "https://production.plaid.com"
        if env_name == "development":
            return "https://development.plaid.com"
        return "https://sandbox.plaid.com"

    def create_sandbox_access_token(
        self,
        institution_id: str = "ins_109508",
        initial_products: list[str] | None = None,
    ) -> str:
        """Create and exchange a Plaid Sandbox public token for an access token.

        This method exists to support integration tests and local development.

        Args:
            institution_id: Plaid institution ID for sandbox (e.g., "ins_109508").
            initial_products: List of product names (e.g., ["transactions"]).

        Returns:
            str: Sandbox access token suitable for subsequent API calls.
        """
        env_name = self.credentials.environment.lower()
        if env_name != "sandbox":
            raise ValueError("create_sandbox_access_token is only available in sandbox")

        # Late imports to keep production path lean and avoid top-level dependency churn
        from plaid.model.item_public_token_exchange_request import (  # noqa: WPS433
            ItemPublicTokenExchangeRequest,
        )
        from plaid.model.products import Products  # noqa: WPS433
        from plaid.model.sandbox_public_token_create_request import (  # noqa: WPS433
            SandboxPublicTokenCreateRequest,
        )

        products = initial_products or ["transactions"]
        create_req = SandboxPublicTokenCreateRequest(
            institution_id=institution_id,
            initial_products=[Products(p) for p in products],
        )

        logger.info("Creating Plaid sandbox public token…")
        create_resp: Any = self.client.sandbox_public_token_create(create_req)
        public_token = getattr(create_resp, "public_token", None)
        if not isinstance(public_token, str) or not public_token:
            raise RuntimeError("Failed to create sandbox public token")

        logger.info("Exchanging Plaid sandbox public token for access token…")
        exchange_req = ItemPublicTokenExchangeRequest(public_token=public_token)
        exchange_resp: Any = self.client.item_public_token_exchange(exchange_req)
        access_token = getattr(exchange_resp, "access_token", None)
        if not isinstance(access_token, str) or not access_token:
            raise RuntimeError(
                "Failed to exchange sandbox public token for access token"
            )

        return access_token

    def get_accounts(self, access_token: str) -> pl.DataFrame:
        """Fetch accounts using the Plaid SDK and return a DataFrame."""
        try:
            request = AccountsGetRequest(access_token=access_token)
            response: Any = self.client.accounts_get(request)

            # Validate SDK objects directly into Pydantic models
            accounts = getattr(response, "accounts", [])
            account_models: list[AccountSchema] = []
            for acct in accounts:
                # Pass Plaid SDK objects directly; schema validators handle enum coercion
                account_models.append(AccountSchema.model_validate(acct))

            institution_id = getattr(
                getattr(response, "item", None), "institution_id", None
            )

            # Convert models to rows for DataFrame
            now_iso = datetime.now().isoformat()
            accounts_data: list[dict[str, Any]] = []
            for m in account_models:
                row = m.model_dump(mode="python")
                row["institution_id"] = institution_id
                row["extracted_at"] = now_iso
                accounts_data.append(row)

            df = pl.DataFrame(accounts_data)

            if self.config.save_raw_data:
                output_path = (
                    self.config.raw_data_path
                    / f"accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
                )
                df.write_parquet(output_path)
                logger.info(f"Saved accounts data to {output_path}")

            return df

        except Exception as e:
            logger.error(f"Failed to extract accounts: {e}")
            raise

    def get_transactions(
        self,
        access_token: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> pl.DataFrame:
        """Fetch transactions using the Plaid SDK and return a DataFrame."""
        if not start_date:
            start_date = datetime.now() - timedelta(days=self.config.days_lookback)
        if not end_date:
            end_date = datetime.now()

        try:
            transactions_data: list[dict[str, Any]] = []
            offset = 0

            while True:
                request = TransactionsGetRequest(
                    access_token=access_token,
                    start_date=start_date.date(),
                    end_date=end_date.date(),
                    options=TransactionsGetRequestOptions(
                        count=self.config.batch_size, offset=offset
                    ),
                )

                # Call Plaid with retries to handle PRODUCT_NOT_READY in sandbox
                response: Any | None = None
                for attempt in range(self.config.max_retries + 1):
                    try:
                        response = self.client.transactions_get(request)
                        break
                    except (
                        ApiException
                    ) as api_exc:  # sandbox may return PRODUCT_NOT_READY
                        # Extract error_code if available to decide on retry
                        error_code = None
                        try:
                            # body is JSON string per SDK
                            import json as _json  # local import to avoid global unused if pruned

                            body = getattr(api_exc, "body", None)
                            if isinstance(body, (str, bytes)):
                                details = _json.loads(body)
                                error_code = details.get("error_code")
                        except Exception:
                            error_code = None

                        if (
                            error_code == "PRODUCT_NOT_READY"
                            and attempt < self.config.max_retries
                        ):
                            time.sleep(self.config.retry_delay)
                            continue
                        raise

                if response is None:
                    raise RuntimeError(
                        "Failed to fetch transactions: no response from Plaid"
                    )

                # Validate SDK objects directly into Pydantic models
                tx_models: list[TransactionSchema] = []
                for tx in getattr(response, "transactions", []):
                    # Pass Plaid SDK objects directly; schema validators handle enum coercion
                    tx_models.append(TransactionSchema.model_validate(tx))

                # Convert models to rows for DataFrame
                now_iso = datetime.now().isoformat()
                for m in tx_models:
                    row = m.model_dump(mode="python")
                    row["extracted_at"] = now_iso
                    transactions_data.append(row)

                total = getattr(response, "total_transactions", 0)
                offset += self.config.batch_size
                if offset >= total or not tx_models:
                    break

            df = pl.DataFrame(transactions_data)

            if self.config.save_raw_data:
                output_path = (
                    self.config.raw_data_path
                    / f"transactions_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{datetime.now().strftime('%H%M%S')}.parquet"
                )
                df.write_parquet(output_path)
                logger.info(
                    f"Saved {len(transactions_data)} transactions to {output_path}"
                )

            return df

        except Exception as e:
            logger.error(f"Failed to extract transactions: {e}")
            raise

    def extract_all_data(
        self, access_token: str, institution_name: str | None = None
    ) -> dict[str, pl.DataFrame]:
        """Extract all available data types for an institution."""
        job_id = str(uuid4())
        institution_name = institution_name or "unknown"

        logger.info(
            f"Starting data extraction for {institution_name} (job_id: {job_id})"
        )

        results = {}

        try:
            # Extract core data
            results["accounts"] = self.get_accounts(access_token)
            results["transactions"] = self.get_transactions(access_token)

            # Create empty DataFrames for optional data types
            results["investment_holdings"] = pl.DataFrame()
            results["securities"] = pl.DataFrame()
            results["investment_transactions"] = pl.DataFrame()
            results["liabilities"] = pl.DataFrame()
            results["identity"] = pl.DataFrame()

            logger.info(f"Completed data extraction for {institution_name}")
            return results

        except Exception as e:
            logger.error(f"Failed during data extraction for {institution_name}: {e}")
            raise


class PlaidConnectionManager:
    """Manages multiple Plaid connections and access tokens securely."""

    def __init__(self):
        """Initialize connection manager."""
        self.extractor = PlaidExtractor()

    def extract_all_institutions(self) -> dict[str, dict[str, pl.DataFrame]]:
        """Extract data from all configured institutions."""
        all_data = {}

        # Get all Plaid tokens from environment
        plaid_tokens = {
            key.replace("PLAID_TOKEN_", "").lower().replace("_", " "): value
            for key, value in os.environ.items()
            if key.startswith("PLAID_TOKEN_")
        }

        if not plaid_tokens:
            logger.warning("No Plaid tokens found in environment variables")
            logger.info(
                "Add tokens using format: PLAID_TOKEN_INSTITUTION_NAME=access-token"
            )
            return all_data

        logger.info(f"Starting extraction from {len(plaid_tokens)} institutions")

        for institution, token in plaid_tokens.items():
            logger.info(f"Extracting data from {institution}")
            try:
                all_data[institution] = self.extractor.extract_all_data(
                    token, institution
                )
                logger.info(f"✅ Successfully extracted data from {institution}")
            except Exception as e:
                logger.error(f"❌ Failed to extract data from {institution}: {e}")
                all_data[institution] = {}

        return all_data


def create_sample_env_file() -> None:
    """Create a sample .env file with required Plaid configuration."""
    sample_content = """# Plaid API Configuration
# Get these from https://dashboard.plaid.com/team/keys

# Required: Plaid API credentials
PLAID_CLIENT_ID=your_plaid_client_id_here
PLAID_SECRET=your_plaid_secret_here
PLAID_ENV=sandbox  # sandbox, development, or production

# Institution Access Tokens (add after linking accounts)
# PLAID_TOKEN_WELLS_FARGO=access-sandbox-xxx
# PLAID_TOKEN_CHASE=access-sandbox-yyy
# PLAID_TOKEN_CAPITAL_ONE=access-sandbox-zzz
# PLAID_TOKEN_FIDELITY=access-sandbox-aaa
# PLAID_TOKEN_ETRADE=access-sandbox-bbb

# Optional: Database configuration
DUCKDB_PATH=data/duckdb/financial.db

# Optional: Logging configuration
LOG_LEVEL=INFO
LOG_TO_FILE=true
LOG_FILE_PATH=logs/moneybin.log
"""

    env_path = Path(".env.example")
    with open(env_path, "w") as f:
        f.write(sample_content)

    logger.info(f"Created sample environment file at {env_path}")
