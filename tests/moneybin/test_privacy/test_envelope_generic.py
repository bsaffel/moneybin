"""ResponseEnvelope[T]: generic typed-payload contract."""

from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated

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
