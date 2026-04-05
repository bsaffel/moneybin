"""Tests for CSV profile system."""

from pathlib import Path

import pytest

from moneybin.extractors.csv_profiles import (
    CSVProfile,
    SignConvention,
    _load_profile_from_yaml,  # pyright: ignore[reportPrivateUsage] — testing internals
    detect_profile,
    load_profiles,
    save_profile,
)


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


class TestCSVProfileValidation:
    """Test CSVProfile Pydantic validation."""

    def test_single_amount_requires_amount_column(self) -> None:
        with pytest.raises(ValueError, match="requires amount_column"):
            CSVProfile(
                name="bad",
                institution_name="Bad Bank",
                header_signature=["Date", "Amount"],
                date_column="Date",
                date_format="%m/%d/%Y",
                sign_convention=SignConvention.NEGATIVE_IS_EXPENSE,
                description_column="Description",
                # Missing amount_column
            )

    def test_split_requires_both_debit_and_credit(self) -> None:
        with pytest.raises(ValueError, match="debit_column and credit_column"):
            CSVProfile(
                name="bad",
                institution_name="Bad Bank",
                header_signature=["Date", "Debit"],
                date_column="Date",
                date_format="%m/%d/%Y",
                debit_column="Debit",
                # Missing credit_column
                sign_convention=SignConvention.SPLIT_DEBIT_CREDIT,
                description_column="Description",
            )

    def test_extra_fields_forbidden(self) -> None:
        with pytest.raises(ValueError):
            CSVProfile(
                name="bad",
                institution_name="Bad Bank",
                header_signature=["Date", "Amount"],
                date_column="Date",
                date_format="%m/%d/%Y",
                amount_column="Amount",
                sign_convention=SignConvention.NEGATIVE_IS_EXPENSE,
                description_column="Description",
                unknown_field="oops",  # type: ignore[call-arg]
            )


class TestProfileYAMLRoundTrip:
    """Test saving and loading profiles from YAML."""

    def test_save_and_load(self, tmp_path: Path, chase_profile: CSVProfile) -> None:
        profiles_dir = tmp_path / "csv_profiles"
        save_profile(chase_profile, profiles_dir)

        yaml_path = profiles_dir / "chase_credit.yaml"
        assert yaml_path.exists()

        loaded = _load_profile_from_yaml(yaml_path)
        assert loaded.name == chase_profile.name
        assert loaded.institution_name == chase_profile.institution_name
        assert loaded.sign_convention == SignConvention.NEGATIVE_IS_EXPENSE
        assert loaded.amount_column == "Amount"
        assert loaded.post_date_column == "Post Date"

    def test_save_split_profile(self, tmp_path: Path, citi_profile: CSVProfile) -> None:
        profiles_dir = tmp_path / "csv_profiles"
        save_profile(citi_profile, profiles_dir)

        loaded = _load_profile_from_yaml(profiles_dir / "citi_credit.yaml")
        assert loaded.sign_convention == SignConvention.SPLIT_DEBIT_CREDIT
        assert loaded.debit_column == "Debit"
        assert loaded.credit_column == "Credit"


class TestProfileLoading:
    """Test loading profiles from directories."""

    def test_load_profiles_from_dir(
        self, tmp_path: Path, chase_profile: CSVProfile
    ) -> None:
        profiles_dir = tmp_path / "csv_profiles"
        save_profile(chase_profile, profiles_dir)

        profiles = load_profiles(profiles_dir)
        assert "chase_credit" in profiles
        assert profiles["chase_credit"].institution_name == "Chase"

    def test_load_profiles_empty_dir(self, tmp_path: Path) -> None:
        profiles_dir = tmp_path / "csv_profiles"
        profiles_dir.mkdir()
        profiles = load_profiles(profiles_dir)
        # Should still have built-in profiles
        assert "chase_credit" in profiles
        assert "citi_credit" in profiles

    def test_user_profiles_override_builtins(
        self, tmp_path: Path, chase_profile: CSVProfile
    ) -> None:
        profiles_dir = tmp_path / "csv_profiles"
        # Save a modified Chase profile
        modified = chase_profile.model_copy(
            update={"institution_name": "Chase (Custom)"}
        )
        save_profile(modified, profiles_dir)

        profiles = load_profiles(profiles_dir)
        assert profiles["chase_credit"].institution_name == "Chase (Custom)"


class TestProfileDetection:
    """Test auto-detecting profiles from CSV headers."""

    def test_detect_chase_headers(
        self,
        tmp_path: Path,
        chase_profile: CSVProfile,
    ) -> None:
        profiles_dir = tmp_path / "csv_profiles"
        save_profile(chase_profile, profiles_dir)

        headers = [
            "Transaction Date",
            "Post Date",
            "Description",
            "Category",
            "Type",
            "Amount",
            "Memo",
        ]
        detected = detect_profile(headers, profiles_dir)
        assert detected is not None
        assert detected.name == "chase_credit"

    def test_detect_case_insensitive(
        self,
        tmp_path: Path,
        chase_profile: CSVProfile,
    ) -> None:
        profiles_dir = tmp_path / "csv_profiles"
        save_profile(chase_profile, profiles_dir)

        headers = [
            "transaction date",
            "post date",
            "description",
            "category",
            "type",
            "amount",
            "memo",
        ]
        detected = detect_profile(headers, profiles_dir)
        assert detected is not None
        assert detected.name == "chase_credit"

    def test_detect_superset_headers(
        self,
        tmp_path: Path,
        chase_profile: CSVProfile,
    ) -> None:
        """Headers with extra columns should still match."""
        profiles_dir = tmp_path / "csv_profiles"
        save_profile(chase_profile, profiles_dir)

        headers = [
            "Transaction Date",
            "Post Date",
            "Description",
            "Category",
            "Type",
            "Amount",
            "Memo",
            "Extra Column",
        ]
        detected = detect_profile(headers, profiles_dir)
        assert detected is not None

    def test_detect_unknown_headers(self, tmp_path: Path) -> None:
        profiles_dir = tmp_path / "csv_profiles"
        profiles_dir.mkdir()

        headers = ["Totally", "Unknown", "Format"]
        detected = detect_profile(headers, profiles_dir)
        assert detected is None
