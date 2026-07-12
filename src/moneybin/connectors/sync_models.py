"""Pydantic models for SyncClient ↔ moneybin-sync API contract.

Single source of truth for request/response shapes. All server responses
validated at the boundary. Service-layer result types live here too so
related types live together.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

# ---- Server response models ----


class AuthToken(BaseModel):
    """Response from POST /auth/device/token and POST /auth/refresh."""

    access_token: str
    refresh_token: str
    expires_in: int = Field(gt=0)
    token_type: Literal["Bearer"] = "Bearer"  # noqa: S105  # literal constant, not a hardcoded password


class LinkInitiateResponse(BaseModel):
    """Response from POST /sync/link/initiate."""

    session_id: str = Field(min_length=1, max_length=128)
    link_url: str
    link_type: Literal["widget_flow", "token_paste"]
    expiration: datetime


class LinkStatusResponse(BaseModel):
    """Response from GET /sync/link/status."""

    session_id: str
    status: Literal["pending", "linked", "failed"]
    provider_item_id: str | None = None
    institution_name: str | None = None
    error: str | None = None
    expiration: datetime


class SyncTriggerResponse(BaseModel):
    """Response from POST /sync/trigger (synchronous)."""

    job_id: str
    status: Literal["pending", "running", "completed", "failed"]
    transaction_count: int | None = None


class SyncAckResponse(BaseModel):
    """Response from POST /sync/ack."""

    job_id: str
    status: Literal["acked"]


class SyncAccount(BaseModel):
    """One account entry in GET /sync/data response."""

    account_id: str
    account_type: str | None = None
    account_subtype: str | None = None
    institution_name: str | None = None
    official_name: str | None = None
    mask: str | None = Field(default=None, max_length=8)


class SyncTransaction(BaseModel):
    """One transaction in GET /sync/data response.

    NOTE: amount preserves Plaid convention (positive = expense). The sign
    flip happens in prep.stg_plaid__transactions, NOT here, NOT in the loader.
    Every field below ``pending`` is Plaid's additional default-returned data
    (location/PFC flattened to scalars by the broker); each is optional so a
    broker that predates the capture change still validates.
    """

    transaction_id: str
    account_id: str
    transaction_date: date
    amount: Decimal
    description: str | None = None
    merchant_name: str | None = None
    category: str | None = None
    pending: bool = False
    original_description: str | None = None
    iso_currency_code: str | None = None
    authorized_date: date | None = None
    pending_transaction_id: str | None = None
    payment_channel: str | None = None
    check_number: str | None = None
    merchant_entity_id: str | None = None
    location_address: str | None = None
    location_city: str | None = None
    location_region: str | None = None
    location_postal_code: str | None = None
    location_country: str | None = None
    location_latitude: float | None = None
    location_longitude: float | None = None
    category_detailed: str | None = None
    category_confidence: str | None = None


class SyncBalance(BaseModel):
    """One balance snapshot in GET /sync/data response."""

    account_id: str
    balance_date: date
    current_balance: Decimal | None = None
    available_balance: Decimal | None = None


class SyncSecurity(BaseModel):
    """One security in GET /sync/data response (Plaid Security, item-scoped).

    Field names match the raw.plaid_securities columns; wire names arrive via
    aliases so model_dump() feeds the loader without renaming.
    """

    model_config = ConfigDict(populate_by_name=True)

    security_id: str
    provider_item_id: str
    institution_security_id: str | None = None
    institution_id: str | None = None
    ticker_symbol: str | None = None
    market_identifier_code: str | None = None
    security_name: str | None = Field(default=None, alias="name")
    security_type: str | None = Field(default=None, alias="type")
    close_price: Decimal | None = None
    close_price_as_of: date | None = None
    iso_currency_code: str | None = None
    unofficial_currency_code: str | None = None
    cusip: str | None = None
    isin: str | None = None
    is_cash_equivalent: bool | None = None


class SyncInvestmentTransaction(BaseModel):
    """One investment ledger event in GET /sync/data response.

    CAUTION: amount keeps Plaid's convention (positive = cash out); the sign
    flip happens only in prep.stg_plaid__investment_transactions.
    """

    model_config = ConfigDict(populate_by_name=True)

    investment_transaction_id: str
    account_id: str
    provider_item_id: str
    security_id: str | None = None
    transaction_date: date = Field(alias="date")
    transaction_datetime: datetime | None = None
    transaction_name: str | None = Field(default=None, alias="name")
    quantity: Decimal | None = None
    amount: Decimal
    price: Decimal | None = None
    fees: Decimal | None = None
    iso_currency_code: str | None = None
    unofficial_currency_code: str | None = None
    investment_transaction_type: str | None = Field(default=None, alias="type")
    investment_transaction_subtype: str | None = Field(default=None, alias="subtype")


class SyncHoldingTaxLot(BaseModel):
    """One HoldingTaxLot entry inside SyncHolding.tax_lots[]."""

    model_config = ConfigDict(populate_by_name=True)

    institution_lot_id: str | None = None
    original_purchase_datetime: datetime | None = None
    quantity: Decimal | None = None
    purchase_price: Decimal | None = None
    cost_basis: Decimal | None = None
    current_value: Decimal | None = None
    position_type: str | None = None


class SyncHolding(BaseModel):
    """One holdings-snapshot position in GET /sync/data response."""

    model_config = ConfigDict(populate_by_name=True)

    account_id: str
    provider_item_id: str
    security_id: str
    institution_price: Decimal | None = None
    institution_price_as_of: date | None = None
    institution_value: Decimal | None = None
    cost_basis: Decimal | None = None
    quantity: Decimal | None = None
    iso_currency_code: str | None = None
    unofficial_currency_code: str | None = None
    vested_quantity: Decimal | None = None
    vested_value: Decimal | None = None
    tax_lots: list[SyncHoldingTaxLot] = Field(default_factory=list)


class InstitutionResult(BaseModel):
    """Per-institution result inside SyncMetadata.institutions[]."""

    provider_item_id: str
    institution_name: str | None = None
    status: Literal["completed", "failed"]
    transaction_count: int | None = None
    error: str | None = None
    error_code: str | None = None
    transactions_window_start: date | None = Field(
        default=None,
        description=(
            "ISO date the server used as the /investments/transactions/get start "
            "boundary for THIS item; required whenever the item has holdings rows."
        ),
    )


class SyncMetadata(BaseModel):
    """metadata block in GET /sync/data response."""

    job_id: str
    synced_at: datetime
    institutions: list[InstitutionResult]


class SyncDataResponse(BaseModel):
    """Full response shape from GET /sync/data."""

    accounts: list[SyncAccount]
    transactions: list[SyncTransaction]
    balances: list[SyncBalance]
    removed_transactions: list[str]
    securities: list[SyncSecurity] = Field(default_factory=list)
    investment_transactions: list[SyncInvestmentTransaction] = Field(
        default_factory=list
    )
    investment_holdings: list[SyncHolding] = Field(default_factory=list)
    metadata: SyncMetadata


class ConnectedInstitution(BaseModel):
    """One entry in GET /institutions response."""

    id: str
    provider_item_id: str
    provider: str
    institution_name: str | None = None
    status: Literal["active", "error", "revoked"]
    last_sync: datetime | None = None
    created_at: datetime
    error_code: str | None = Field(
        default=None,
        description="Provider error code (e.g. ITEM_LOGIN_REQUIRED). Advisory — treat as None when absent.",
    )


# ---- Service-layer result types ----


class PullResult(BaseModel):
    """Return value from SyncService.pull()."""

    job_id: str
    transactions_loaded: int
    accounts_loaded: int
    balances_loaded: int
    transactions_removed: int
    securities_loaded: int = 0
    investment_transactions_loaded: int = 0
    holdings_loaded: int = 0
    holding_lots_loaded: int = 0
    institutions: list[InstitutionResult]
    transforms_applied: bool = False
    transforms_duration_seconds: float | None = None
    transforms_error: str | None = None
    opening_bootstrap_rows: int = 0
    investment_source_overlap_accounts: list[str] = Field(default_factory=list)
    security_resolution: dict[str, int] = Field(default_factory=dict)
    security_resolution_error: str | None = None


class LinkResult(BaseModel):
    """Return value from SyncService.link()."""

    provider_item_id: str
    institution_name: str | None = None
    pull_result: PullResult | None = None


class SyncConnectionView(BaseModel):
    """Return value from SyncService.list_connections() — enriched with user-facing guidance."""

    id: str
    provider_item_id: str
    institution_name: str | None
    provider: str
    status: Literal["active", "error", "revoked"]
    last_sync: datetime | None
    error_code: str | None = Field(
        default=None,
        description="Provider error code (e.g. ITEM_LOGIN_REQUIRED). Advisory — treat as None when absent.",
    )
    guidance: str | None = None  # user-facing next-step message when status != 'active'
