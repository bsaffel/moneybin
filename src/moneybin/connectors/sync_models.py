"""Pydantic models for SyncClient ↔ moneybin-server API contract.

Single source of truth for request/response shapes. All server responses
validated at the boundary. Service-layer result types live here too so
related types live together.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field

# ---- Server response models ----


class AuthToken(BaseModel):
    """Response from POST /auth/device/token and POST /auth/refresh."""

    access_token: str
    refresh_token: str
    expires_in: int = Field(gt=0)
    token_type: Literal["Bearer"] = "Bearer"  # noqa: S105  # literal constant, not a hardcoded password


class ConnectInitiateResponse(BaseModel):
    """Response from POST /sync/connect/initiate."""

    session_id: str = Field(min_length=1, max_length=128)
    link_url: str
    connect_type: Literal["widget_flow", "token_paste"]
    expiration: datetime


class ConnectStatusResponse(BaseModel):
    """Response from GET /sync/connect/status."""

    session_id: str
    status: Literal["pending", "connected", "failed"]
    provider_item_id: str | None = None
    institution_name: str | None = None
    error: str | None = None
    expiration: datetime


class SyncTriggerResponse(BaseModel):
    """Response from POST /sync/trigger (synchronous)."""

    job_id: str
    status: Literal["pending", "running", "completed", "failed"]
    transaction_count: int | None = None


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
    """

    transaction_id: str
    account_id: str
    transaction_date: date
    amount: Decimal
    description: str | None = None
    merchant_name: str | None = None
    category: str | None = None
    pending: bool = False


class SyncBalance(BaseModel):
    """One balance snapshot in GET /sync/data response."""

    account_id: str
    balance_date: date
    current_balance: Decimal | None = None
    available_balance: Decimal | None = None


class InstitutionResult(BaseModel):
    """Per-institution result inside SyncMetadata.institutions[]."""

    provider_item_id: str
    institution_name: str | None = None
    status: Literal["completed", "failed"]
    transaction_count: int | None = None
    error: str | None = None
    error_code: str | None = None


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
        description="Provider error code (e.g. ITEM_LOGIN_REQUIRED). Populated when status is 'error'.",
    )


# ---- Service-layer result types ----


class PullResult(BaseModel):
    """Return value from SyncService.pull()."""

    job_id: str
    transactions_loaded: int
    accounts_loaded: int
    balances_loaded: int
    transactions_removed: int
    institutions: list[InstitutionResult]


class ConnectResult(BaseModel):
    """Return value from SyncService.connect()."""

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
        description="Provider error code (e.g. ITEM_LOGIN_REQUIRED). Populated when status is 'error'.",
    )
    guidance: str | None = None  # user-facing next-step message when status != 'active'
