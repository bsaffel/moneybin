"""Inside, boundary and outside fixtures for the accepted comparison matrix."""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from moneybin.connectors.sync_models import SyncInvestmentTransaction
from moneybin.database import Database
from moneybin.extractors.plaid.extractor import PlaidExtractor
from tests.moneybin.test_investments.comparison_helpers import (
    install_comparison_models,
)


def _pair(
    db: Database,
    *,
    type_: str = "buy",
    subtype: str | None = None,
    provider_type: str = "buy",
    provider_subtype: str = "buy",
    day: int = 10,
    left_quantity: str | None = "1",
    right_quantity: str | None = "1",
    left_price: str | None = "100",
    right_price: str | None = "100",
    left_amount: str = "-100",
    right_amount: str = "100",
    left_fees: str = "0",
    right_fees: str = "0",
) -> None:
    db.execute(
        """INSERT INTO raw.manual_investment_transactions (
        source_transaction_id, import_id, account_id, security_id, type, subtype,
        trade_date, quantity, price, amount, fees, currency_code, created_by
    ) VALUES ('manual', 'import', 'account', 'security', ?, ?, '2026-01-10',
              ?, ?, ?, ?, 'USD', 'cli')""",
        [type_, subtype, left_quantity, left_price, left_amount, left_fees],
    )
    row = SyncInvestmentTransaction(
        investment_transaction_id="plaid",
        account_id="native_account",
        provider_item_id="origin",
        security_id="native_security",
        date=date(2026, 1, day),
        name="Source context",
        quantity=Decimal(right_quantity) if right_quantity is not None else None,
        price=Decimal(right_price) if right_price is not None else None,
        amount=Decimal(right_amount),
        fees=Decimal(right_fees),
        iso_currency_code="USD",
        type=provider_type,
        subtype=provider_subtype,
    )
    PlaidExtractor(db)._load_investment_transactions(  # pyright: ignore[reportPrivateUsage]
        [row],
        "sync_pair",
        datetime(2026, 2, 1, tzinfo=UTC),
        datetime(2026, 2, 1, tzinfo=UTC),
    )
    install_comparison_models(db)


@pytest.mark.parametrize(
    ("type_", "provider_type", "provider_subtype", "days"),
    [
        ("buy", "buy", "buy", 5),
        ("sell", "sell", "sell", 5),
        ("reinvest", "buy", "dividend reinvestment", 5),
        ("dividend", "cash", "dividend", 3),
        ("interest", "cash", "interest", 3),
        ("capital_gain_distribution", "cash", "long-term capital gain", 3),
        ("return_of_capital", "cash", "return of principal", 3),
        ("fee", "fee", "account fee", 3),
        ("transfer_in", "transfer", "transfer", 7),
        ("transfer_out", "transfer", "transfer", 7),
        ("deposit", "cash", "deposit", 7),
        ("withdrawal", "cash", "withdrawal", 7),
    ],
)
@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_each_type_date_window(
    comparison_db: Database,
    type_: str,
    provider_type: str,
    provider_subtype: str,
    days: int,
    offset: int,
) -> None:
    direction = "-1" if type_ in {"sell", "transfer_out"} else "1"
    cash_only = type_ in {
        "dividend",
        "interest",
        "capital_gain_distribution",
        "return_of_capital",
        "fee",
        "deposit",
        "withdrawal",
    }
    amount = "-100" if type_ in {"buy", "reinvest", "fee", "withdrawal"} else "100"
    _pair(
        comparison_db,
        type_=type_,
        provider_type=provider_type,
        provider_subtype=provider_subtype,
        day=10 + days + offset,
        left_quantity=None if cash_only else direction,
        right_quantity=None if cash_only else direction,
        left_price=None if cash_only else "100",
        right_price=None if cash_only else "100",
        left_amount=amount,
        right_amount=str(-Decimal(amount)),
    )
    assert comparison_db.execute(
        "SELECT date_within_tolerance FROM prep.int_investment_events__evidence"
    ).fetchall() == ([(True,)] if offset <= 0 else [])


@pytest.mark.parametrize(
    ("left", "right", "within"),
    [
        ("50", "50.0099", True),
        ("50", "50.01", True),
        ("50", "50.0101", False),
        ("9999.0001", "10000", True),
        ("9999", "10000", True),
        ("9998.9999", "10000", False),
        ("10000", "9999", True),
        ("10000", "9998.9999", False),
        ("0", "0", True),
        ("0", "0.01", True),
        ("0", "0.0101", False),
    ],
)
def test_price_floor_relative_and_zero_boundaries(
    comparison_db: Database, left: str, right: str, within: bool
) -> None:
    _pair(
        comparison_db,
        left_price=left,
        right_price=right,
        left_amount="-999",
        right_amount="999",
    )
    assert comparison_db.execute(
        "SELECT price_within_tolerance FROM prep.int_investment_events__evidence"
    ).fetchall() == [(within,)]


@pytest.mark.parametrize(
    ("fee", "within"), [("0", True), ("0.01", True), ("0.02", False)]
)
def test_fee_cent_boundary(comparison_db: Database, fee: str, within: bool) -> None:
    _pair(comparison_db, right_fees=fee)
    assert comparison_db.execute(
        "SELECT fees_within_tolerance FROM prep.int_investment_events__evidence"
    ).fetchall() == [(within,)]


def test_price_equation_does_not_erase_quantity_conflict(
    comparison_db: Database,
) -> None:
    _pair(comparison_db, left_quantity="2", left_price="50")
    assert comparison_db.execute(
        "SELECT price_within_tolerance, quantity_within_tolerance, is_candidate FROM prep.int_investment_events__evidence"
    ).fetchall() == [(True, False, False)]


@pytest.mark.parametrize(
    ("type_", "subtype", "provider_subtype"),
    [
        ("dividend", "qualified", "non-qualified dividend"),
        ("capital_gain_distribution", "short_term", "long-term capital gain"),
    ],
)
def test_present_tax_character_conflict(
    comparison_db: Database, type_: str, subtype: str, provider_subtype: str
) -> None:
    _pair(
        comparison_db,
        type_=type_,
        subtype=subtype,
        provider_type="cash",
        provider_subtype=provider_subtype,
        left_quantity=None,
        right_quantity=None,
        left_price=None,
        right_price=None,
        left_amount="100",
        right_amount="-100",
    )
    assert comparison_db.execute(
        "SELECT is_candidate, subtype_conflict FROM prep.int_investment_events__evidence"
    ).fetchall() == [(True, True)]
