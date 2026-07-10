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


class TestMappingResultScore:
    """Verify map_columns produces normalized score + Confidence view."""

    def test_score_present_on_result(self) -> None:
        df = _make_df({
            "Transaction Date": ["01/15/2026", "02/20/2026"],
            "Amount": ["-42.50", "100.00"],
            "Description": ["KROGER #1234", "DIRECT DEPOSIT"],
        })
        result = map_columns(df)
        assert 0.0 <= result.score <= 1.0
        assert result.score >= 0.90

    def test_score_yields_low_for_unmappable(self) -> None:
        df = _make_df({
            "Name": ["Alice", "Bob"],
            "Score": ["95", "87"],
            "Grade": ["A", "B"],
        })
        result = map_columns(df)
        assert result.confidence == "low"
        assert result.score < 0.70

    def test_missing_required_listed_for_low(self) -> None:
        df = _make_df({
            "Name": ["Alice", "Bob"],
            "Score": ["95", "87"],
            "Grade": ["A", "B"],
        })
        result = map_columns(df)
        if result.confidence == "low":
            assert len(result.missing_required) > 0

    def test_missing_credit_amount_for_half_split(self) -> None:
        """Debit-only files must surface 'credit_amount' as missing, not 'amount'.

        Reporting 'amount' sends the user to a contradictory override
        (single-amount layered on partial split). Mirrors the rule in
        validate_partial_mapping.
        """
        df = _make_df({
            "Date": ["01/15/2026", "02/20/2026"],
            "Description": ["KROGER #1234", "WALMART"],
            "Debit": ["42.50", "100.00"],
        })
        result = map_columns(df)
        assert "debit_amount" in result.field_mapping
        assert "credit_amount" not in result.field_mapping
        assert "credit_amount" in result.missing_required
        assert "amount" not in result.missing_required

    def test_missing_debit_amount_for_half_split(self) -> None:
        """Credit-only files must surface 'debit_amount' as missing, not 'amount'.

        Mirror of the debit-only case.
        """
        df = _make_df({
            "Date": ["01/15/2026", "02/20/2026"],
            "Description": ["KROGER #1234", "WALMART"],
            "Credit": ["42.50", "100.00"],
        })
        result = map_columns(df)
        assert "credit_amount" in result.field_mapping
        assert "debit_amount" not in result.field_mapping
        assert "debit_amount" in result.missing_required
        assert "amount" not in result.missing_required

    def test_to_confidence_returns_uniform_value(self) -> None:
        from moneybin.extractors.confidence import Confidence

        df = _make_df({
            "Transaction Date": ["01/15/2026", "02/20/2026"],
            "Amount": ["-42.50", "100.00"],
            "Description": ["KROGER #1234", "DIRECT DEPOSIT"],
        })
        result = map_columns(df)
        c = result.to_confidence(t_high=0.90, t_med=0.70)
        assert isinstance(c, Confidence)
        assert c.tier == result.confidence
        assert c.score == result.score
        assert tuple(result.flagged_fields) == c.flagged
        assert result.missing_required == c.missing_required

    def test_back_compat_confidence_tier_preserved(self) -> None:
        df = _make_df({
            "Col1": ["01/15/2026", "02/20/2026"],
            "Col2": ["KROGER #1234", "WALMART"],
            "Col3": ["Groceries", "Groceries"],
            "Col4": ["-42.50", "100.00"],
        })
        result = map_columns(df)
        assert result.confidence in ("high", "medium", "low")


class TestStructuralRedFlagDowngradesConfidence:
    """A structural red flag forces low confidence regardless of content score.

    Regression coverage: a structurally-suspicious file (e.g. the row consumed
    as a header also parses as a transaction) must not self-accept at
    medium/high just because its column names/content score well — the
    propose->confirm gate must engage.
    """

    def test_structural_red_flag_forces_low_despite_high_content_score(self) -> None:
        df = _make_df({
            "Transaction Date": ["01/15/2026", "02/20/2026"],
            "Amount": ["-42.50", "100.00"],
            "Description": ["KROGER #1234", "DIRECT DEPOSIT"],
        })
        baseline = map_columns(df)
        assert baseline.confidence == "high"  # sanity: would be high without the flag

        result = map_columns(df, structural_red_flag=True)
        assert result.confidence == "low"
        assert (
            result.score == baseline.score
        )  # score reported honestly, only tier drops

    def test_to_confidence_reflects_structural_red_flag(self) -> None:
        df = _make_df({
            "Transaction Date": ["01/15/2026", "02/20/2026"],
            "Amount": ["-42.50", "100.00"],
            "Description": ["KROGER #1234", "DIRECT DEPOSIT"],
        })
        result = map_columns(df, structural_red_flag=True)
        c = result.to_confidence(t_high=0.90, t_med=0.70)
        assert c.tier == "low"
        assert c.tier == result.confidence

    def test_no_structural_red_flag_leaves_confidence_unaffected(self) -> None:
        df = _make_df({
            "Transaction Date": ["01/15/2026", "02/20/2026"],
            "Amount": ["-42.50", "100.00"],
            "Description": ["KROGER #1234", "DIRECT DEPOSIT"],
        })
        result = map_columns(df, structural_red_flag=False)
        assert result.confidence == "high"
