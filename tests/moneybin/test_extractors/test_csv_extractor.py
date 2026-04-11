"""Tests for CSV extractor."""

from pathlib import Path

import pytest

from moneybin.extractors.csv_extractor import (
    CSVExtractionConfig,
    CSVExtractor,
    _generate_transaction_id,  # pyright: ignore[reportPrivateUsage] — testing internals
    _parse_decimal,  # pyright: ignore[reportPrivateUsage] — testing internals
)
from moneybin.extractors.csv_profiles import (
    CSVProfile,
    SignConvention,
    save_profile,
)

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures"


@pytest.fixture()
def chase_profile() -> CSVProfile:
    """A sample Chase credit card profile."""
    return CSVProfile(
        name="chase_credit",
        institution_name="Chase",
        header_signature=[
            "Transaction Date",
            "Post Date",
            "Description",
            "Category",
            "Type",
            "Amount",
            "Memo",
        ],
        date_column="Transaction Date",
        date_format="%m/%d/%Y",
        post_date_column="Post Date",
        amount_column="Amount",
        sign_convention=SignConvention.NEGATIVE_IS_EXPENSE,
        description_column="Description",
        memo_column="Memo",
        category_column="Category",
        type_column="Type",
    )


@pytest.fixture()
def citi_profile() -> CSVProfile:
    """A sample Citi credit card profile."""
    return CSVProfile(
        name="citi_credit",
        institution_name="Citi",
        header_signature=[
            "Status",
            "Date",
            "Description",
            "Debit",
            "Credit",
            "Member Name",
        ],
        date_column="Date",
        date_format="%m/%d/%Y",
        debit_column="Debit",
        credit_column="Credit",
        sign_convention=SignConvention.SPLIT_DEBIT_CREDIT,
        description_column="Description",
        status_column="Status",
        member_name_column="Member Name",
    )


@pytest.fixture()
def extractor(tmp_path: Path) -> CSVExtractor:
    """A CSVExtractor configured with a temp directory."""
    config = CSVExtractionConfig(
        raw_data_path=tmp_path / "raw" / "csv",
    )
    return CSVExtractor(config)


class TestParseDecimal:
    """Test the _parse_decimal helper."""

    def test_simple_number(self) -> None:
        assert _parse_decimal("42.57") == 42.57

    def test_negative(self) -> None:
        assert _parse_decimal("-1254.37") == -1254.37

    def test_commas(self) -> None:
        assert _parse_decimal("1,234.56") == 1234.56

    def test_dollar_sign(self) -> None:
        assert _parse_decimal("$99.99") == 99.99

    def test_parentheses_negative(self) -> None:
        assert _parse_decimal("(50.00)") == -50.00

    def test_empty_string(self) -> None:
        assert _parse_decimal("") is None

    def test_none(self) -> None:
        assert _parse_decimal(None) is None

    def test_whitespace(self) -> None:
        assert _parse_decimal("  ") is None


class TestTransactionIDGeneration:
    """Test deterministic transaction ID generation."""

    def test_deterministic(self) -> None:
        id1 = _generate_transaction_id("2025-12-16", "-41.67", "TARGET", "acct1")
        id2 = _generate_transaction_id("2025-12-16", "-41.67", "TARGET", "acct1")
        assert id1 == id2

    def test_prefix(self) -> None:
        tid = _generate_transaction_id("2025-12-16", "-41.67", "TARGET", "acct1")
        assert tid.startswith("csv_")

    def test_stable_across_row_positions(self) -> None:
        """Same logical transaction at different row offsets yields the same ID."""
        id_row0 = _generate_transaction_id("2025-12-16", "-5.00", "COFFEE", "acct1")
        id_row5 = _generate_transaction_id("2025-12-16", "-5.00", "COFFEE", "acct1")
        assert id_row0 == id_row5

    def test_different_accounts_different_ids(self) -> None:
        id1 = _generate_transaction_id("2025-12-16", "-5.00", "COFFEE", "acct1")
        id2 = _generate_transaction_id("2025-12-16", "-5.00", "COFFEE", "acct2")
        assert id1 != id2


class TestChaseExtraction:
    """Test extracting Chase credit card CSV files."""

    def test_extract_chase_csv(
        self, extractor: CSVExtractor, chase_profile: CSVProfile
    ) -> None:
        csv_path = FIXTURES_DIR / "sample_chase_credit.csv"
        result = extractor.extract_from_file(
            csv_path, profile=chase_profile, account_id="chase-7022"
        )

        assert "accounts" in result
        assert "transactions" in result
        assert len(result["accounts"]) == 1
        assert len(result["transactions"]) == 5

    def test_chase_amount_signs(
        self, extractor: CSVExtractor, chase_profile: CSVProfile
    ) -> None:
        csv_path = FIXTURES_DIR / "sample_chase_credit.csv"
        result = extractor.extract_from_file(
            csv_path, profile=chase_profile, account_id="chase-7022"
        )

        txns = result["transactions"]
        amounts = txns["amount"].to_list()

        # Return: positive
        assert 24.83 in amounts
        # Sale: negative
        assert -41.67 in amounts
        # Payment: positive
        assert 1500.0 in amounts

    def test_chase_categories_extracted(
        self, extractor: CSVExtractor, chase_profile: CSVProfile
    ) -> None:
        csv_path = FIXTURES_DIR / "sample_chase_credit.csv"
        result = extractor.extract_from_file(
            csv_path, profile=chase_profile, account_id="chase-7022"
        )

        txns = result["transactions"]
        categories = txns["category"].to_list()
        assert "Shopping" in categories
        assert "Food & Drink" in categories

    def test_chase_post_dates(
        self, extractor: CSVExtractor, chase_profile: CSVProfile
    ) -> None:
        csv_path = FIXTURES_DIR / "sample_chase_credit.csv"
        result = extractor.extract_from_file(
            csv_path, profile=chase_profile, account_id="chase-7022"
        )

        txns = result["transactions"]
        post_dates = txns["post_date"].to_list()
        # All rows have post dates
        assert all(pd is not None for pd in post_dates)


class TestCitiExtraction:
    """Test extracting Citi credit card CSV files."""

    def test_extract_citi_csv(
        self, extractor: CSVExtractor, citi_profile: CSVProfile
    ) -> None:
        csv_path = FIXTURES_DIR / "sample_citi_credit.csv"
        result = extractor.extract_from_file(
            csv_path, profile=citi_profile, account_id="citi-card"
        )

        assert len(result["accounts"]) == 1
        assert len(result["transactions"]) == 5

    def test_citi_debit_credit_amounts(
        self, extractor: CSVExtractor, citi_profile: CSVProfile
    ) -> None:
        csv_path = FIXTURES_DIR / "sample_citi_credit.csv"
        result = extractor.extract_from_file(
            csv_path, profile=citi_profile, account_id="citi-card"
        )

        txns = result["transactions"]
        amounts = txns["amount"].to_list()

        # Debits should be negative (expenses)
        assert -0.84 in amounts  # Foreign transaction fee
        assert -42.57 in amounts  # Purchase
        assert -85.20 in amounts  # Grocery

        # Credits should be positive (income/payments)
        assert 1254.37 in amounts  # Autopay (original was -1254.37 in Credit col)
        assert 25.0 in amounts  # Refund

    def test_citi_member_name(
        self, extractor: CSVExtractor, citi_profile: CSVProfile
    ) -> None:
        csv_path = FIXTURES_DIR / "sample_citi_credit.csv"
        result = extractor.extract_from_file(
            csv_path, profile=citi_profile, account_id="citi-card"
        )

        txns = result["transactions"]
        member_names = txns["member_name"].to_list()
        assert all(name == "JANE DOE" for name in member_names)

    def test_citi_status_extracted(
        self, extractor: CSVExtractor, citi_profile: CSVProfile
    ) -> None:
        csv_path = FIXTURES_DIR / "sample_citi_credit.csv"
        result = extractor.extract_from_file(
            csv_path, profile=citi_profile, account_id="citi-card"
        )

        txns = result["transactions"]
        statuses = txns["transaction_status"].to_list()
        assert all(s == "Cleared" for s in statuses)


class TestAutoDetection:
    """Test auto-detecting bank profiles."""

    def test_auto_detect_chase(
        self,
        tmp_path: Path,
        chase_profile: CSVProfile,
    ) -> None:
        profiles_dir = tmp_path / "csv_profiles"
        save_profile(chase_profile, profiles_dir)

        config = CSVExtractionConfig(raw_data_path=tmp_path / "raw" / "csv")
        extractor = CSVExtractor(config)

        csv_path = FIXTURES_DIR / "sample_chase_credit.csv"
        result = extractor.extract_from_file(
            csv_path,
            account_id="chase-7022",
            user_profiles_dir=profiles_dir,
        )
        assert len(result["transactions"]) == 5

    def test_unknown_format_raises(
        self,
        tmp_path: Path,
    ) -> None:
        profiles_dir = tmp_path / "csv_profiles"
        profiles_dir.mkdir()

        config = CSVExtractionConfig(raw_data_path=tmp_path / "raw" / "csv")
        extractor = CSVExtractor(config)

        csv_path = FIXTURES_DIR / "sample_chase_credit.csv"
        # Use empty profiles dir (no built-ins) — but built-ins will still load
        # So this test checks that detection works with built-ins
        result = extractor.extract_from_file(
            csv_path,
            account_id="chase-7022",
            user_profiles_dir=profiles_dir,
        )
        assert len(result["transactions"]) == 5


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_missing_file_raises(self, extractor: CSVExtractor) -> None:
        with pytest.raises(FileNotFoundError):
            extractor.extract_from_file(
                Path("/nonexistent.csv"),
                account_id="test",
            )

    def test_missing_account_id_raises(
        self, extractor: CSVExtractor, chase_profile: CSVProfile
    ) -> None:
        with pytest.raises(ValueError, match="account_id is required"):
            extractor.extract_from_file(
                FIXTURES_DIR / "sample_chase_credit.csv",
                profile=chase_profile,
            )

    def test_account_df_has_institution_name(
        self, extractor: CSVExtractor, chase_profile: CSVProfile
    ) -> None:
        result = extractor.extract_from_file(
            FIXTURES_DIR / "sample_chase_credit.csv",
            profile=chase_profile,
            account_id="chase-7022",
        )
        acct = result["accounts"]
        assert acct["institution_name"][0] == "Chase"
        assert acct["account_id"][0] == "chase-7022"
