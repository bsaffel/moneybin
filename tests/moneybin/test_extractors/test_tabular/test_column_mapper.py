"""Tests for the column mapping engine (Stage 3)."""

import polars as pl

from moneybin.extractors.tabular.column_mapper import (
    map_columns,
)


def _make_df(columns: dict[str, list[str]]) -> pl.DataFrame:
    """Helper to create a DataFrame from string columns."""
    return pl.DataFrame(columns)


class TestMapColumnsHighConfidence:
    """Tests for high confidence header matching."""

    def test_standard_headers(self) -> None:
        df = _make_df({
            "Transaction Date": ["01/15/2026", "02/20/2026"],
            "Amount": ["-42.50", "100.00"],
            "Description": ["KROGER #1234", "DIRECT DEPOSIT"],
        })
        result = map_columns(df)
        assert result.confidence == "high"
        assert result.field_mapping["transaction_date"] == "Transaction Date"
        assert result.field_mapping["amount"] == "Amount"
        assert result.field_mapping["description"] == "Description"

    def test_chase_like_headers(self) -> None:
        df = _make_df({
            "Transaction Date": ["01/15/2026"],
            "Post Date": ["01/16/2026"],
            "Description": ["KROGER"],
            "Category": ["Groceries"],
            "Type": ["Sale"],
            "Amount": ["-42.50"],
            "Memo": [""],
        })
        result = map_columns(df)
        assert result.confidence == "high"
        assert result.field_mapping["post_date"] == "Post Date"
        assert result.field_mapping["category"] == "Category"

    def test_debit_credit_columns(self) -> None:
        df = _make_df({
            "Date": ["01/15/2026"],
            "Description": ["Payment"],
            "Debit": ["42.50"],
            "Credit": [""],
        })
        result = map_columns(df)
        assert result.field_mapping["debit_amount"] == "Debit"
        assert result.field_mapping["credit_amount"] == "Credit"
        assert result.sign_convention == "split_debit_credit"


class TestMapColumnsMediumConfidence:
    """Tests for medium confidence with content-based matching."""

    def test_generic_headers_with_content_match(self) -> None:
        df = _make_df({
            "Col1": ["01/15/2026", "02/20/2026"],
            "Col2": ["KROGER #1234", "WALMART"],
            "Col3": ["Groceries", "Groceries"],
            "Col4": ["-42.50", "100.00"],
        })
        result = map_columns(df)
        assert result.confidence in ("medium", "high")
        assert "transaction_date" in result.field_mapping
        assert "amount" in result.field_mapping
        assert "description" in result.field_mapping


class TestMapColumnsLowConfidence:
    """Tests for low confidence when required fields are missing."""

    def test_no_date_column(self) -> None:
        df = _make_df({
            "Name": ["Alice", "Bob"],
            "Score": ["95", "87"],
            "Grade": ["A", "B"],
        })
        result = map_columns(df)
        assert result.confidence == "low"


class TestMultiAccountDetection:
    """Tests for multi-account column detection."""

    def test_account_column_detected(self) -> None:
        df = _make_df({
            "Date": ["01/15/2026"],
            "Amount": ["-42.50"],
            "Description": ["KROGER"],
            "Account": ["Chase Checking"],
        })
        result = map_columns(df)
        assert result.is_multi_account is True
        assert result.field_mapping.get("account_name") == "Account"

    def test_no_account_column(self) -> None:
        df = _make_df({
            "Date": ["01/15/2026"],
            "Amount": ["-42.50"],
            "Description": ["KROGER"],
        })
        result = map_columns(df)
        assert result.is_multi_account is False
