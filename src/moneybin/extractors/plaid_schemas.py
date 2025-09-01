"""Essential Pydantic schemas for Plaid API data extraction.

This module provides core data validation schemas for Plaid API responses,
focusing on accounts and transactions - the primary data types needed
for financial data extraction.
"""

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PlaidCredentials(BaseModel):
    """Secure credential management for Plaid API access."""

    model_config = ConfigDict(frozen=True)

    client_id: str = Field(..., description="Plaid client ID")
    secret: str = Field(..., description="Plaid secret key")
    environment: str = Field(default="sandbox", description="Plaid environment")

    @classmethod
    def from_environment(cls) -> "PlaidCredentials":
        """Load credentials from environment variables securely."""
        import os

        client_id = os.getenv("PLAID_CLIENT_ID")
        secret = os.getenv("PLAID_SECRET")
        environment = os.getenv("PLAID_ENV", "sandbox")

        if not client_id:
            raise ValueError("PLAID_CLIENT_ID environment variable is required")
        if not secret:
            raise ValueError("PLAID_SECRET environment variable is required")

        return cls(client_id=client_id, secret=secret, environment=environment)


class PlaidEnvironment(Enum):
    """Plaid API environment options."""

    SANDBOX = "sandbox"
    DEVELOPMENT = "development"
    PRODUCTION = "production"


class AccountType(Enum):
    """Plaid account type enumeration."""

    DEPOSITORY = "depository"
    CREDIT = "credit"
    LOAN = "loan"
    INVESTMENT = "investment"
    OTHER = "other"


class AccountSubtype(Enum):
    """Plaid account subtype enumeration."""

    # Core subtypes only
    CHECKING = "checking"
    SAVINGS = "savings"
    CREDIT_CARD = "credit card"
    MORTGAGE = "mortgage"
    AUTO = "auto"
    STUDENT = "student"
    INVESTMENT_401K = "401k"
    INVESTMENT_403B = "403b"
    IRA = "ira"
    BROKERAGE = "brokerage"


class PaymentChannel(Enum):
    """Transaction payment channel enumeration."""

    ONLINE = "online"
    IN_STORE = "in store"
    ATM = "atm"
    OTHER = "other"


class TransactionType(Enum):
    """Transaction type enumeration."""

    DIGITAL = "digital"
    PLACE = "place"
    SPECIAL = "special"
    UNRESOLVED = "unresolved"


# Base Models


class BaseSchema(BaseModel):
    """Base schema with common configuration."""

    model_config = ConfigDict(
        validate_assignment=True,
        extra="forbid",
        use_enum_values=True,
        str_strip_whitespace=True,
        from_attributes=True,
        populate_by_name=True,
    )


# Core Data Schemas


class LocationSchema(BaseSchema):
    """Schema for transaction location data."""

    address: str | None = None
    city: str | None = None
    region: str | None = None
    postal_code: str | None = None
    country: str | None = None
    lat: float | None = Field(None, ge=-90, le=90)
    lon: float | None = Field(None, ge=-180, le=180)
    store_number: str | None = None


class BalanceSchema(BaseSchema):
    """Schema for account balance information."""

    available: float | None = Field(None, description="Available balance")
    current: float | None = Field(None, description="Current balance")
    limit: float | None = Field(None, description="Credit limit or overdraft limit")
    iso_currency_code: str | None = Field(None, max_length=3)
    unofficial_currency_code: str | None = None
    last_updated_datetime: datetime | None = None


class AccountSchema(BaseSchema):
    """Schema for Plaid account data."""

    account_id: str = Field(..., description="Plaid account ID")
    balances: BalanceSchema
    mask: str | None = Field(None, max_length=4)
    name: str = Field(..., description="Account name")
    official_name: str | None = None
    persistent_account_id: str | None = None
    subtype: str | None = None
    type: str
    verification_status: str | None = None

    @field_validator("type", mode="before")
    @classmethod
    def coerce_account_type(cls, v: Any) -> Any:
        """Accept Plaid SDK enum or string and convert to string."""
        if v is None:
            return None
        if isinstance(v, Enum):
            return v.value
        return str(v)

    @field_validator("subtype", mode="before")
    @classmethod
    def coerce_account_subtype(cls, v: Any) -> Any:
        """Accept Plaid SDK enum-like subtype and convert to string."""
        if v is None:
            return None
        if isinstance(v, Enum):
            return v.value
        return str(v)


class TransactionSchema(BaseSchema):
    """Schema for Plaid transaction data."""

    transaction_id: str = Field(..., description="Plaid transaction ID")
    account_id: str = Field(..., description="Associated account ID")
    amount: Decimal = Field(..., description="Transaction amount")
    iso_currency_code: str = Field(..., max_length=3)
    unofficial_currency_code: str | None = None

    # Dates
    transaction_date: date = Field(..., description="Transaction date", alias="date")
    authorized_date: date | None = None
    authorized_datetime: datetime | None = None
    transaction_datetime: datetime | None = Field(None, alias="datetime")

    # Description and classification
    name: str | None = None
    merchant_name: str | None = None
    original_description: str | None = None
    account_owner: str | None = None

    # Categories
    category: list[str] = Field(default_factory=list)
    category_id: str | None = None

    # Enhanced categorization
    personal_finance_category: dict[str, Any] | None = None

    # Transaction details
    payment_channel: str | None = None
    transaction_type: str | None = None
    transaction_code: str | None = None

    # Location
    location: LocationSchema | None = None

    # Status
    pending: bool = False
    pending_transaction_id: str | None = None

    # Merchant details
    website: str | None = None
    logo_url: str | None = None

    @field_validator(
        "payment_channel", "transaction_type", "transaction_code", mode="before"
    )
    @classmethod
    def coerce_transaction_enums(cls, v: Any) -> Any:
        """Coerce Plaid SDK enums for transaction fields into strings."""
        if v is None:
            return None
        if isinstance(v, Enum):
            return v.value
        return v

    @field_validator("category", mode="before")
    @classmethod
    def coerce_category(cls, v: Any) -> Any:
        """Ensure category is a list of strings; Plaid may return None."""
        if v is None:
            return []
        if isinstance(v, list):
            items = cast(list[object], v)
            return [str(x) for x in items]
        # Fallback: coerce single value to a singleton list
        return [str(v)]

    @field_validator("personal_finance_category", mode="before")
    @classmethod
    def coerce_personal_finance_category(cls, v: Any) -> Any:
        """Convert Plaid SDK PersonalFinanceCategory objects into dicts."""
        if v is None:
            return None
        if isinstance(v, dict):
            return v
        to_dict = getattr(v, "to_dict", None)
        if callable(to_dict):
            try:
                converted = to_dict()
                return converted if isinstance(converted, dict) else None
            except Exception:
                # Best-effort; ignore conversion errors
                return None
        # Best-effort: build a dict from public attributes
        result: dict[str, Any] = {}
        for k in dir(v):
            if k.startswith("_"):
                continue
            value = getattr(v, k, None)
            if value is not None:
                result[k] = value
        return result or None

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v: Decimal) -> Decimal:
        """Validate transaction amount is reasonable."""
        if abs(v) > Decimal("1000000"):  # $1M limit
            raise ValueError("Transaction amount exceeds reasonable limit")
        return v


# Response Schemas


class PlaidAccountsResponse(BaseSchema):
    """Complete schema for Plaid accounts endpoint response."""

    accounts: list[AccountSchema]
    item: dict[str, Any]
    request_id: str


class PlaidTransactionsResponse(BaseSchema):
    """Complete schema for Plaid transactions endpoint response."""

    accounts: list[AccountSchema]
    transactions: list[TransactionSchema]
    total_transactions: int
    item: dict[str, Any]
    request_id: str


# Utility Functions


def sanitize_for_duckdb(value: Any) -> Any:
    """Sanitize values for DuckDB insertion.

    Args:
        value: Value to sanitize

    Returns:
        Any: Sanitized value safe for DuckDB
    """
    if value is None:
        return None

    if isinstance(value, (list, dict)):
        # Convert complex types to JSON strings for DuckDB
        import json

        return json.dumps(value)

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, (date, datetime)):
        return value.isoformat()

    return value


def create_table_from_schema(schema_class: BaseModel, table_name: str) -> str:
    """Generate DuckDB CREATE TABLE statement from Pydantic schema.

    Args:
        schema_class: Pydantic model class
        table_name: Name for the DuckDB table

    Returns:
        str: SQL CREATE TABLE statement
    """
    # This would be implemented to generate DDL from Pydantic schemas
    # For now, return a placeholder
    return f"-- CREATE TABLE {table_name} generated from {schema_class.__name__}"
