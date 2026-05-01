"""Tests for expectation primitive types."""

import pytest
from pydantic import ValidationError

from moneybin.validation.expectations import SourceTransactionRef


def test_source_transaction_ref_constructs_with_expected_fields() -> None:
    ref = SourceTransactionRef(source_transaction_id="csv_abc123", source_type="csv")
    assert ref.source_transaction_id == "csv_abc123"
    assert ref.source_type == "csv"


def test_source_transaction_ref_is_frozen() -> None:
    ref = SourceTransactionRef(source_transaction_id="csv_abc123", source_type="csv")
    with pytest.raises(ValidationError):
        ref.source_type = "ofx"  # type: ignore[misc]


def test_source_transaction_ref_rejects_extra_fields() -> None:
    """Misspelled YAML keys (e.g. source_typ) must fail at construction."""
    with pytest.raises(ValidationError):
        SourceTransactionRef(
            source_transaction_id="csv_abc123",
            source_type="csv",
            source_typ="csv",  # pyright: ignore[reportCallIssue]
        )


def test_source_transaction_ref_rejects_unknown_source_type() -> None:
    """source_type is a closed Literal — values outside csv/ofx must fail."""
    with pytest.raises(ValidationError):
        SourceTransactionRef(source_transaction_id="csv_abc123", source_type="pdf")  # type: ignore[arg-type]
