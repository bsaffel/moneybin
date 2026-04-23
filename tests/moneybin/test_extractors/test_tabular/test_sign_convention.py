"""Tests for sign convention inference."""

from moneybin.extractors.tabular.sign_convention import (
    infer_sign_convention,
)


class TestInferSignConvention:
    """Tests for sign convention inference."""

    def test_negative_is_expense(self) -> None:
        result = infer_sign_convention(
            amount_values=["-42.50", "100.00", "-8.99", "1250.00"],
            debit_values=None,
            credit_values=None,
        )
        assert result.convention == "negative_is_expense"
        assert result.needs_confirmation is False

    def test_all_positive_flagged(self) -> None:
        result = infer_sign_convention(
            amount_values=["42.50", "100.00", "8.99"],
            debit_values=None,
            credit_values=None,
        )
        assert result.convention == "negative_is_expense"
        assert result.needs_confirmation is True

    def test_split_debit_credit(self) -> None:
        result = infer_sign_convention(
            amount_values=None,
            debit_values=["42.50", None, "8.99"],
            credit_values=[None, "100.00", None],
        )
        assert result.convention == "split_debit_credit"
        assert result.needs_confirmation is False

    def test_negative_is_income(self) -> None:
        result = infer_sign_convention(
            amount_values=["42.50", "8.99", "-500.00"],
            debit_values=None,
            credit_values=None,
            header_context="credit",
        )
        assert result.convention == "negative_is_income"
