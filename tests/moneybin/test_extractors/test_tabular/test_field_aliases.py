"""Tests for header normalization and alias matching."""

from moneybin.extractors.tabular.field_aliases import (
    FIELD_ALIASES,
    match_header_to_field,
    normalize_header,
)


class TestNormalizeHeader:
    """Tests for the normalize_header function."""

    def test_lowercase(self) -> None:
        assert normalize_header("Transaction Date") == "transaction date"

    def test_strip_whitespace(self) -> None:
        assert normalize_header("  Amount  ") == "amount"

    def test_collapse_multiple_spaces(self) -> None:
        assert normalize_header("Transaction   Date") == "transaction date"

    def test_replace_underscores(self) -> None:
        assert normalize_header("transaction_date") == "transaction date"

    def test_replace_hyphens(self) -> None:
        assert normalize_header("transaction-date") == "transaction date"

    def test_strip_quotes(self) -> None:
        assert normalize_header('"Amount"') == "amount"
        assert normalize_header("'Amount'") == "amount"


class TestMatchHeaderToField:
    """Tests for the match_header_to_field function."""

    def test_exact_alias_match(self) -> None:
        assert match_header_to_field("Transaction Date") == "transaction_date"

    def test_normalized_alias_match(self) -> None:
        assert match_header_to_field("TRANSACTION_DATE") == "transaction_date"

    def test_amount_match(self) -> None:
        assert match_header_to_field("Amount") == "amount"

    def test_description_match(self) -> None:
        assert match_header_to_field("Payee") == "description"

    def test_debit_match(self) -> None:
        assert match_header_to_field("Debit Amount") == "debit_amount"

    def test_credit_match(self) -> None:
        assert match_header_to_field("Credit") == "credit_amount"

    def test_post_date_match(self) -> None:
        assert match_header_to_field("Posting Date") == "post_date"

    def test_check_number_match(self) -> None:
        assert match_header_to_field("Check #") == "check_number"

    def test_account_name_match(self) -> None:
        assert match_header_to_field("Account") == "account_name"

    def test_no_match_returns_none(self) -> None:
        assert match_header_to_field("Gobbledygook Column") is None

    def test_all_aliases_are_normalized(self) -> None:
        """Every alias in the table must equal its normalized form."""
        for field, aliases in FIELD_ALIASES.items():
            for alias in aliases:
                assert alias == normalize_header(alias), (
                    f"Alias '{alias}' for field '{field}' is not pre-normalized"
                )
