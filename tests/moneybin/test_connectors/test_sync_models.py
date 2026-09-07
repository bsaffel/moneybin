"""Tests for investment extensions to the /sync/data wire models."""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from moneybin.connectors.sync_models import (
    DeviceAuthorizationChallenge,
    SyncAccount,
    SyncBalance,
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


def test_device_authorization_challenge_defaults_interval_to_five_seconds() -> None:
    challenge = DeviceAuthorizationChallenge.model_validate({
        "device_code": "secret-device-code",
        "user_code": "ABCD-EFGH",
        "verification_uri": "https://auth.example/activate",
        "expires_in": 900,
    })

    assert challenge.interval == 5.0


def test_account_keeps_persistent_account_id_and_name() -> None:
    """The broker sends both; an undeclared field is destroyed, not carried.

    Pydantic's default ``extra='ignore'`` drops an unmodelled key at
    ``model_validate`` with no error and no log line, so a field the broker
    adds reaches nothing downstream until the client declares it. That is the
    exact mechanism by which ``persistent_account_id`` — Plaid's
    cross-connection account identity — reached nothing downstream between the
    first Plaid sync release (#134) and the change that declared it. Asserting
    the wire keys survive is the only signal this layer offers.
    """
    account = SyncAccount.model_validate({
        "account_id": "acc_1",
        "persistent_account_id": "ppa_survives_reconnect",
        "account_type": "credit",
        "account_subtype": "credit card",
        "name": "Freedom Unlimited",
        "official_name": "Ultimate Rewards®",
        "mask": "1234",
    })

    assert account.persistent_account_id == "ppa_survives_reconnect"
    assert account.name == "Freedom Unlimited"


def test_account_without_the_new_fields_still_validates() -> None:
    """A broker predating the field must not start failing validation."""
    account = SyncAccount.model_validate({"account_id": "acc_1"})

    assert account.persistent_account_id is None
    assert account.name is None


def test_account_blank_identity_fields_normalise_to_none() -> None:
    """A blank must not reach the resolver truthy.

    ``persistent_account_id`` becomes a ``persistent_token`` strong ref, which
    is scoped globally and auto-adopts without review. A whitespace-only value
    is truthy in Python, so two unrelated accounts carrying one would merge
    into a single ledger silently. Nothing on the wire promises an empty field
    arrives as null rather than ``""`` — ``prep.stg_plaid__accounts`` wraps
    every free-text Plaid column in ``NULLIF(TRIM(...), '')`` for that reason.
    """
    account = SyncAccount.model_validate({
        "account_id": "acc_1",
        "persistent_account_id": "   ",
        "name": "",
    })

    assert account.persistent_account_id is None
    assert account.name is None


def test_account_blank_display_fields_normalise_to_none() -> None:
    """A blank must not win the display fallback either.

    ``_resolve_accounts`` picks ``name or official_name or "<institution>
    account"``, so a whitespace-only string short-circuits the chain and the
    account surfaces under a name that renders empty. ``institution_name``
    reaches the resolver's own ``institution`` field by the same route.
    """
    account = SyncAccount.model_validate({
        "account_id": "acc_1",
        "official_name": "   ",
        "institution_name": "",
    })

    assert account.official_name is None
    assert account.institution_name is None


def test_balance_keeps_every_wire_field_the_broker_sends() -> None:
    """The broker sends nine balance keys; an undeclared one is destroyed.

    Same mechanism as ``persistent_account_id`` above. ``margin_loan_amount`` is
    the one that costs money: Plaid documents ``current`` on an investment
    account as the total value of *assets* and ``margin_loan_amount`` as the
    *borrowed funds* held against them, so for as long as the client drops it a
    margin account overstates net worth by the whole loan.

    ``limit`` is declared as ``balance_limit`` because the wire name is a SQL
    reserved word, and because ``credit_limit`` already means the *user-asserted*
    limit on ``app.account_settings`` — a different fact from the one the
    institution reports.
    """
    balance = SyncBalance.model_validate({
        "account_id": "acc_1",
        "balance_date": "2026-06-14",
        "current_balance": 4321.00,
        "available_balance": 1200.00,
        "limit": 5000.00,
        "iso_currency_code": "USD",
        "unofficial_currency_code": None,
        "last_updated_datetime": "2026-06-14T12:00:00+00:00",
        "margin_loan_amount": 250.00,
    })

    assert balance.margin_loan_amount == Decimal("250.00")
    assert balance.balance_limit == Decimal("5000.00")
    assert balance.last_updated_datetime == datetime(2026, 6, 14, 12, 0, tzinfo=UTC)


def test_balance_without_the_new_fields_still_validates() -> None:
    """A broker predating the fields must not start failing validation."""
    balance = SyncBalance.model_validate({
        "account_id": "acc_1",
        "balance_date": "2026-06-14",
    })

    assert balance.margin_loan_amount is None
    assert balance.balance_limit is None
    assert balance.last_updated_datetime is None


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
