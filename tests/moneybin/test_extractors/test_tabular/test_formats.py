"""Tests for TabularFormat model and YAML loading."""

from pathlib import Path

import pytest

from moneybin.extractors.tabular.formats import (
    TabularFormat,
    load_builtin_formats,  # noqa: F401  # used in TestLoadBuiltinFormats (Task 9)
)


class TestTabularFormatModel:
    """Tests for the TabularFormat Pydantic model."""

    def test_minimal_valid_format(self) -> None:
        fmt = TabularFormat(
            name="test_bank",
            institution_name="Test Bank",
            header_signature=["Date", "Amount", "Description"],
            field_mapping={
                "transaction_date": "Date",
                "amount": "Amount",
                "description": "Description",
            },
            sign_convention="negative_is_expense",
            date_format="%m/%d/%Y",
        )
        assert fmt.name == "test_bank"
        assert fmt.multi_account is False
        assert fmt.number_format == "us"

    def test_split_debit_credit_requires_both_columns(self) -> None:
        fmt = TabularFormat(
            name="test_split",
            institution_name="Test",
            header_signature=["Date", "Debit", "Credit", "Desc"],
            field_mapping={
                "transaction_date": "Date",
                "debit_amount": "Debit",
                "credit_amount": "Credit",
                "description": "Desc",
            },
            sign_convention="split_debit_credit",
            date_format="%m/%d/%Y",
        )
        assert fmt.sign_convention == "split_debit_credit"

    def test_invalid_sign_convention_rejected(self) -> None:
        with pytest.raises(ValueError, match="sign_convention"):
            TabularFormat(
                name="bad",
                institution_name="Bad",
                header_signature=["Date"],
                field_mapping={"transaction_date": "Date"},
                sign_convention="invalid",
                date_format="%Y",
            )

    def test_invalid_number_format_rejected(self) -> None:
        with pytest.raises(ValueError, match="number_format"):
            TabularFormat(
                name="bad",
                institution_name="Bad",
                header_signature=["Date"],
                field_mapping={"transaction_date": "Date"},
                sign_convention="negative_is_expense",
                date_format="%Y",
                number_format="invalid",
            )

    def test_header_signature_match_subset(self) -> None:
        fmt = TabularFormat(
            name="test",
            institution_name="Test",
            header_signature=["Date", "Amount"],
            field_mapping={"transaction_date": "Date", "amount": "Amount"},
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
        )
        file_headers = ["Date", "Amount", "Description", "Category"]
        assert fmt.matches_headers(file_headers)

    def test_header_signature_no_match(self) -> None:
        fmt = TabularFormat(
            name="test",
            institution_name="Test",
            header_signature=["Date", "Amount", "Payee"],
            field_mapping={},
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
        )
        file_headers = ["Date", "Amount", "Description"]
        assert not fmt.matches_headers(file_headers)

    def test_header_match_case_insensitive(self) -> None:
        fmt = TabularFormat(
            name="test",
            institution_name="Test",
            header_signature=["Transaction Date", "Amount"],
            field_mapping={},
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
        )
        file_headers = ["TRANSACTION DATE", "AMOUNT", "DESC"]
        assert fmt.matches_headers(file_headers)

    def test_to_yaml_roundtrip(self, tmp_path: Path) -> None:
        fmt = TabularFormat(
            name="roundtrip_test",
            institution_name="Test",
            header_signature=["Date", "Amount"],
            field_mapping={"transaction_date": "Date", "amount": "Amount"},
            sign_convention="negative_is_expense",
            date_format="%Y-%m-%d",
            number_format="european",
            skip_trailing_patterns=["^Total"],
        )
        yaml_path = tmp_path / "roundtrip_test.yaml"
        fmt.to_yaml(yaml_path)

        loaded = TabularFormat.from_yaml(yaml_path)
        assert loaded.name == fmt.name
        assert loaded.number_format == "european"
        assert loaded.skip_trailing_patterns == ["^Total"]
