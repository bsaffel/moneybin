"""Tests for SyncClient request/response Pydantic models."""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from moneybin.connectors.sync_models import (
    AuthToken,
    ConnectedInstitution,
    ConnectInitiateResponse,
    ConnectStatusResponse,
    InstitutionResult,
    SyncDataResponse,
    SyncTransaction,
    SyncTriggerResponse,
)


def test_auth_token_valid() -> None:
    token = AuthToken(
        access_token="eyJ...",  # noqa: S106  # test fixture, not a real credential
        refresh_token="v1.M...",  # noqa: S106  # test fixture, not a real credential
        expires_in=3600,
    )
    assert token.token_type == "Bearer"  # noqa: S105  # test fixture, not a real credential
    assert token.expires_in == 3600


def test_auth_token_rejects_zero_expires_in() -> None:
    with pytest.raises(ValidationError):
        AuthToken(access_token="x", refresh_token="y", expires_in=0)  # noqa: S106  # test fixture, not a real credential


def test_connect_initiate_valid_widget_flow() -> None:
    resp = ConnectInitiateResponse(
        session_id="sess_abc",
        link_url="https://hosted.plaid.com/link/x",
        connect_type="widget_flow",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    assert resp.connect_type == "widget_flow"


def test_connect_initiate_rejects_unknown_connect_type() -> None:
    with pytest.raises(ValidationError):
        ConnectInitiateResponse(
            session_id="sess_abc",
            link_url="https://hosted.plaid.com/link/x",
            connect_type="oauth_redirect",  # type: ignore[arg-type]  # not in Literal
            expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
        )


def test_connect_status_pending() -> None:
    resp = ConnectStatusResponse(
        session_id="sess_abc",
        status="pending",
        expiration=datetime(2026, 5, 13, 13, 30, tzinfo=UTC),
    )
    assert resp.status == "pending"
    assert resp.provider_item_id is None


def test_sync_transaction_preserves_decimal() -> None:
    txn = SyncTransaction(
        transaction_id="txn_001",
        account_id="acc_001",
        transaction_date=date(2026, 4, 7),
        amount=Decimal("42.50"),
        description="COFFEE",
        pending=False,
    )
    assert txn.amount == Decimal("42.50")
    assert isinstance(txn.amount, Decimal)


def test_sync_data_response_parses_full_payload() -> None:
    payload = {
        "accounts": [
            {
                "account_id": "acc_001",
                "account_type": "depository",
                "account_subtype": "checking",
                "institution_name": "Chase",
                "official_name": "Total Checking",
                "mask": "1234",
            }
        ],
        "transactions": [
            {
                "transaction_id": "txn_001",
                "account_id": "acc_001",
                "transaction_date": "2026-04-07",
                "amount": "42.50",
                "description": "COFFEE",
                "merchant_name": "Best Coffee",
                "category": "FOOD_AND_DRINK",
                "pending": False,
            }
        ],
        "balances": [
            {
                "account_id": "acc_001",
                "balance_date": "2026-04-08",
                "current_balance": "1234.56",
                "available_balance": "1200.00",
            }
        ],
        "removed_transactions": ["txn_old"],
        "metadata": {
            "job_id": "job_abc",
            "synced_at": "2026-04-08T12:00:00Z",
            "institutions": [
                {
                    "provider_item_id": "item_abc",
                    "institution_name": "Chase",
                    "status": "completed",
                    "transaction_count": 1,
                }
            ],
        },
    }
    parsed = SyncDataResponse.model_validate(payload)
    assert len(parsed.transactions) == 1
    assert parsed.transactions[0].amount == Decimal("42.50")
    assert parsed.removed_transactions == ["txn_old"]
    assert parsed.metadata.institutions[0].provider_item_id == "item_abc"


def test_institution_result_failed_with_error_code() -> None:
    r = InstitutionResult(
        provider_item_id="item_x",
        institution_name="Schwab",
        status="failed",
        error="login required",
        error_code="ITEM_LOGIN_REQUIRED",
    )
    assert r.status == "failed"
    assert r.error_code == "ITEM_LOGIN_REQUIRED"


def test_connected_institution_minimal() -> None:
    inst = ConnectedInstitution(
        id="uuid-1",
        provider_item_id="item_abc",
        provider="plaid",
        status="active",
        created_at=datetime(2026, 3, 15, 8, 30, tzinfo=UTC),
    )
    assert inst.status == "active"


def test_sync_trigger_response_status_literal() -> None:
    with pytest.raises(ValidationError):
        SyncTriggerResponse(job_id="j1", status="weird_status")  # type: ignore[arg-type]
