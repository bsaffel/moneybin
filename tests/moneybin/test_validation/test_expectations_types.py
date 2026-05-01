"""Tests for expectation primitive types."""

import dataclasses


def test_source_transaction_ref_is_frozen_dataclass():
    """SourceTransactionRef is a frozen dataclass with expected fields."""
    from moneybin.validation.expectations import SourceTransactionRef

    ref = SourceTransactionRef(source_transaction_id="csv_abc123", source_type="csv")
    assert ref.source_transaction_id == "csv_abc123"
    assert ref.source_type == "csv"
    # Frozen — must reject mutation
    with __import__("pytest").raises(dataclasses.FrozenInstanceError):
        ref.source_type = "ofx"  # type: ignore[misc]
