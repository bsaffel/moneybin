"""Tests for the non-serialized classes_returned field on ResponseEnvelope."""

from __future__ import annotations

from moneybin.protocol.envelope import build_envelope


def test_classes_returned_carried_but_not_serialized() -> None:
    env = build_envelope(
        data=[{"a": 1}], sensitivity="high", classes_returned=["txn_amount"]
    )
    assert env.classes_returned == ["txn_amount"]
    # Wire payload must NOT leak the internal observability list.
    d = env.to_dict()
    assert "classes_returned" not in d
    assert "classes_returned" not in d["summary"]


def test_classes_returned_defaults_to_none() -> None:
    env = build_envelope(data=[{"a": 1}], sensitivity="low")
    assert env.classes_returned is None
    assert "classes_returned" not in env.to_dict()


def test_classes_returned_multiple_classes() -> None:
    env = build_envelope(
        data=[{"account_id": "****1234", "amount": -5.0}],
        sensitivity="critical",
        classes_returned=["account_identifier", "txn_amount"],
    )
    assert env.classes_returned == ["account_identifier", "txn_amount"]
    assert "classes_returned" not in env.to_dict()
