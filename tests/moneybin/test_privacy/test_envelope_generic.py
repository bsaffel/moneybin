"""ResponseEnvelope[T]: generic typed-payload contract."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Annotated

from moneybin.privacy.payloads.investments import (
    InvestmentHoldingRow,
    InvestmentHoldingsPayload,
)
from moneybin.privacy.taxonomy import DataClass
from moneybin.protocol.envelope import ResponseEnvelope, build_envelope


@dataclass(frozen=True)
class _TypedRow:
    account_id: Annotated[str, DataClass.ACCOUNT_IDENTIFIER]
    amount: Annotated[Decimal, DataClass.TXN_AMOUNT]


@dataclass(frozen=True)
class _TypedListPayload:
    rows: list[_TypedRow]
    total: Annotated[Decimal, DataClass.AGGREGATE]


def test_build_envelope_accepts_typed_dataclass_payload() -> None:
    payload = _TypedListPayload(
        rows=[_TypedRow(account_id="acct_1234567890", amount=Decimal("10.00"))],
        total=Decimal("10.00"),
    )
    env: ResponseEnvelope[_TypedListPayload] = build_envelope(
        data=payload, sensitivity="medium", total_count=1
    )
    assert env.data is payload  # payload preserved as typed object
    assert env.summary.sensitivity == "medium"


def test_to_dict_serializes_dataclass_payload() -> None:
    payload = _TypedListPayload(
        rows=[_TypedRow(account_id="acct_1234567890", amount=Decimal("10.00"))],
        total=Decimal("10.00"),
    )
    env = build_envelope(data=payload, sensitivity="medium", total_count=1)
    d = env.to_dict()
    assert isinstance(d["data"], dict)
    assert d["data"]["rows"][0]["account_id"] == "acct_1234567890"
    assert d["data"]["rows"][0]["amount"] == Decimal("10.00")


def test_to_json_emits_well_formed_json() -> None:
    payload = _TypedListPayload(
        rows=[_TypedRow(account_id="acct_1234567890", amount=Decimal("10.00"))],
        total=Decimal("10.00"),
    )
    env = build_envelope(data=payload, sensitivity="medium")
    parsed = json.loads(env.to_json())
    assert (
        parsed["data"]["rows"][0]["amount"] == 10.0
    )  # Decimal → float per _DecimalEncoder


def test_dict_payload_still_works_unchanged() -> None:
    # Backwards-compat: existing tools that pass dicts must still serialize correctly.
    env = build_envelope(data={"found": True, "x": 1}, sensitivity="low")
    assert env.data == {"found": True, "x": 1}
    d = env.to_dict()
    assert d["data"] == {"found": True, "x": 1}


def test_list_of_dicts_payload_still_works() -> None:
    env = build_envelope(data=[{"a": 1}, {"a": 2}], sensitivity="low")
    assert env.summary.returned_count == 2
    assert env.to_dict()["data"] == [{"a": 1}, {"a": 2}]


def _holding(currency: str) -> InvestmentHoldingRow:
    return InvestmentHoldingRow(
        account_id="acct-1",
        security_id="sec-1",
        quantity=Decimal("1"),
        cost_basis=Decimal("10.00"),
        average_cost=Decimal("10.00"),
        currency_code=currency,
        market_value=Decimal("12.00"),
        unrealized_gain=Decimal("2.00"),
        price_date=date(2026, 3, 5),
        price_source="tiingo",
        days_since_observed=1,
        valuation_status="priced",
    )


def test_rate_provenance_does_not_displace_the_rows_it_describes() -> None:
    """A provenance list is metadata about the rows, not a second row set.

    ``build_envelope`` finds "the" row collection by looking for a payload's one
    non-auxiliary list, and falls back to no currency and a count of 1 when it
    finds several. Holdings gained ``applied_rates`` beside ``rows``, so without
    an exemption every holdings response — including the overwhelmingly common
    one where nothing converted and the list is empty — would report an unknown
    currency and a count of 1 in place of the number of positions returned.
    """
    payload = InvestmentHoldingsPayload(
        rows=[_holding("USD"), _holding("USD"), _holding("USD")],
        applied_rates=[],
    )

    env = build_envelope(data=payload, sensitivity="medium")

    assert env.summary.returned_count == 3
    assert env.summary.display_currency == "USD"
