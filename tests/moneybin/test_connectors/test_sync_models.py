"""Tests for investment extensions to the /sync/data wire models."""

from datetime import date
from decimal import Decimal
from typing import Any

from moneybin.connectors.sync_models import (
    SyncDataResponse,
    SyncHolding,
    SyncInvestmentTransaction,
    SyncSecurity,
)

_BASE: dict[str, Any] = {
    "accounts": [],
    "transactions": [],
    "balances": [],
    "removed_transactions": [],
    "metadata": {
        "job_id": "job-1",
        "synced_at": "2026-07-08T12:00:00Z",
        "institutions": [],
    },
}


def test_payload_without_investment_arrays_validates() -> None:
    resp = SyncDataResponse.model_validate(_BASE)
    assert resp.securities == []
    assert resp.investment_transactions == []
    assert resp.investment_holdings == []


def test_investment_arrays_parse_wire_names() -> None:
    payload = {
        **_BASE,
        "metadata": {
            **_BASE["metadata"],
            "institutions": [
                {
                    "provider_item_id": "item_1",
                    "status": "completed",
                    "transactions_window_start": "2024-07-08",
                }
            ],
        },
        "securities": [
            {
                "security_id": "sec_plaid_1",
                "provider_item_id": "item_1",
                "ticker_symbol": "AAPL",
                "market_identifier_code": "XNAS",
                "name": "Apple Inc.",
                "type": "equity",
                "close_price": "214.55",
                "close_price_as_of": "2026-07-08",
                "iso_currency_code": "USD",
                "is_cash_equivalent": False,
            }
        ],
        "investment_transactions": [
            {
                "investment_transaction_id": "itx_1",
                "account_id": "acc_1",
                "provider_item_id": "item_1",
                "security_id": "sec_plaid_1",
                "date": "2026-07-06",
                "name": "BUY AAPL",
                "quantity": "10.0",
                "amount": "2145.50",
                "price": "214.55",
                "fees": "0.0",
                "type": "buy",
                "subtype": "buy",
            }
        ],
        "investment_holdings": [
            {
                "account_id": "acc_1",
                "provider_item_id": "item_1",
                "security_id": "sec_plaid_1",
                "quantity": "10.0",
                "cost_basis": "1980.00",
                "iso_currency_code": "USD",
                "tax_lots": [
                    {
                        "institution_lot_id": "lot_7f",
                        "original_purchase_datetime": "2021-03-11T00:00:00Z",
                        "quantity": "6.0",
                        "purchase_price": "121.00",
                        "cost_basis": "726.00",
                        "position_type": "long",
                    }
                ],
            }
        ],
    }
    resp = SyncDataResponse.model_validate(payload)
    sec = resp.securities[0]
    assert isinstance(sec, SyncSecurity)
    assert sec.security_name == "Apple Inc."
    assert sec.security_type == "equity"
    txn = resp.investment_transactions[0]
    assert isinstance(txn, SyncInvestmentTransaction)
    assert txn.provider_item_id == "item_1"
    assert txn.transaction_date == date(2026, 7, 6)
    assert txn.transaction_name == "BUY AAPL"
    assert txn.investment_transaction_type == "buy"
    assert txn.amount == Decimal("2145.50")
    holding = resp.investment_holdings[0]
    assert isinstance(holding, SyncHolding)
    assert holding.provider_item_id == "item_1"
    assert holding.tax_lots[0].cost_basis == Decimal("726.00")
    assert resp.metadata.institutions[0].transactions_window_start == date(2024, 7, 8)


def test_empty_tax_lots_and_dump_uses_ddl_names() -> None:
    holding = SyncHolding.model_validate({
        "account_id": "a",
        "provider_item_id": "item_1",
        "security_id": "s",
    })
    assert holding.tax_lots == []
    txn = SyncInvestmentTransaction.model_validate({
        "investment_transaction_id": "i",
        "account_id": "a",
        "provider_item_id": "item_1",
        "date": "2026-01-02",
        "amount": "0",
        "type": "cash",
        "subtype": "deposit",
    })
    dumped = txn.model_dump()
    assert dumped["transaction_date"] == date(2026, 1, 2)
    assert dumped["investment_transaction_type"] == "cash"
    assert "date" not in dumped
