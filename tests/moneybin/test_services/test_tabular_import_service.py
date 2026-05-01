"""Tests for the tabular import service layer."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from _pytest.logging import LogCaptureFixture

from moneybin.services.import_service import (
    _detect_file_type,  # type: ignore[reportPrivateUsage]  # testing private function
)


class TestDetectFileType:
    """Test that file extensions are detected correctly."""

    def test_csv_detected(self) -> None:
        assert _detect_file_type(Path("test.csv")) == "tabular"

    def test_tsv_detected(self) -> None:
        assert _detect_file_type(Path("test.tsv")) == "tabular"

    def test_xlsx_detected(self) -> None:
        assert _detect_file_type(Path("test.xlsx")) == "tabular"

    def test_parquet_detected(self) -> None:
        assert _detect_file_type(Path("test.parquet")) == "tabular"

    def test_feather_detected(self) -> None:
        assert _detect_file_type(Path("test.feather")) == "tabular"

    def test_txt_detected(self) -> None:
        assert _detect_file_type(Path("test.txt")) == "tabular"

    def test_ofx_still_works(self) -> None:
        assert _detect_file_type(Path("test.ofx")) == "ofx"

    def test_pdf_still_works(self) -> None:
        assert _detect_file_type(Path("test.pdf")) == "w2"

    def test_unsupported_extension_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported file type"):
            _detect_file_type(Path("test.jpg"))


def test_resolved_mapping_round_trip() -> None:
    """ResolvedMapping is constructible and exposes the resolved tabular fields."""
    from moneybin.services.import_service import ResolvedMapping

    rm = ResolvedMapping(
        field_mapping={"transaction_date": "Date", "amount": "Amt"},
        date_format="%Y-%m-%d",
        sign_convention="negative_is_expense",
        number_format="us",
        is_multi_account=False,
        confidence="high",
    )
    assert rm.field_mapping["amount"] == "Amt"
    assert rm.sign_convention == "negative_is_expense"
    # Frozen — assignment must raise
    import dataclasses

    try:
        rm.confidence = "low"  # type: ignore[misc]
    except dataclasses.FrozenInstanceError:
        pass
    else:
        raise AssertionError("ResolvedMapping must be frozen")


def test_resolve_account_via_matcher_uses_existing_id_on_match(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    """Matched account name → reuses the existing account_id."""
    from moneybin.database import Database
    from moneybin.services.import_service import ImportService

    db = Database(
        tmp_path / "match.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    try:
        db.execute("""
            INSERT INTO raw.tabular_accounts
            (account_id, account_name, account_number, account_number_masked,
             account_type, institution_name, currency, source_file, source_type,
             source_origin, import_id)
            VALUES
            ('chase-checking', 'Chase Checking', NULL, NULL, NULL, NULL, NULL,
             'old.csv', 'csv', 'chase', 'imp1')
        """)
        aid = ImportService(db)._resolve_account_via_matcher(  # type: ignore[reportPrivateUsage]
            account_name="Chase Checking",
            account_number=None,
            threshold=0.6,
            auto_accept=False,
        )
        assert aid == "chase-checking"
    finally:
        db.close()


def test_resolve_account_via_matcher_creates_new_when_no_candidates(
    mock_secret_store: MagicMock, tmp_path: Path
) -> None:
    """No fuzzy candidates → fall back to slugify (creates a new account)."""
    from moneybin.database import Database
    from moneybin.services.import_service import ImportService

    db = Database(
        tmp_path / "new.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    try:
        # Confirm the table exists so this exercises the empty-table path,
        # not the except-Exception fallback.
        row = db.execute("SELECT COUNT(*) FROM raw.tabular_accounts").fetchone()
        assert row is not None and row[0] == 0
        aid = ImportService(db)._resolve_account_via_matcher(  # type: ignore[reportPrivateUsage]
            account_name="Brand New Account",
            account_number=None,
            threshold=0.6,
            auto_accept=False,
        )
        assert aid == "brand-new-account"
    finally:
        db.close()


def test_resolve_account_via_matcher_auto_accepts_top_candidate(
    mock_secret_store: MagicMock, tmp_path: Path, caplog: LogCaptureFixture
) -> None:
    """With auto_accept=True, a fuzzy candidate is taken without prompting."""
    from moneybin.database import Database
    from moneybin.services.import_service import ImportService

    db = Database(
        tmp_path / "fuzzy.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    try:
        db.execute("""
            INSERT INTO raw.tabular_accounts
            (account_id, account_name, account_number, account_number_masked,
             account_type, institution_name, currency, source_file, source_type,
             source_origin, import_id)
            VALUES
            ('chase-chk', 'Chase Chk', NULL, NULL, NULL, NULL, NULL,
             'old.csv', 'csv', 'chase', 'imp1')
        """)
        with caplog.at_level("INFO"):
            aid = ImportService(db)._resolve_account_via_matcher(  # type: ignore[reportPrivateUsage]
                account_name="Chase Checking",
                account_number=None,
                threshold=0.6,
                auto_accept=True,
            )
        assert aid == "chase-chk"
        assert "auto-accepting" in caplog.text.lower()
    finally:
        db.close()


def test_resolve_account_via_matcher_warns_and_falls_back_when_not_auto(
    mock_secret_store: MagicMock, tmp_path: Path, caplog: LogCaptureFixture
) -> None:
    """Without auto_accept, fuzzy candidates trigger a warning + slugify fallback."""
    from moneybin.database import Database
    from moneybin.services.import_service import ImportService

    db = Database(
        tmp_path / "fuzzy2.duckdb",
        secret_store=mock_secret_store,
        no_auto_upgrade=True,
    )
    try:
        db.execute("""
            INSERT INTO raw.tabular_accounts
            (account_id, account_name, account_number, account_number_masked,
             account_type, institution_name, currency, source_file, source_type,
             source_origin, import_id)
            VALUES
            ('chase-chk', 'Chase Chk', NULL, NULL, NULL, NULL, NULL,
             'old.csv', 'csv', 'chase', 'imp1')
        """)
        with caplog.at_level("WARNING"):
            aid = ImportService(db)._resolve_account_via_matcher(  # type: ignore[reportPrivateUsage]
                account_name="Chase Checking",
                account_number=None,
                threshold=0.6,
                auto_accept=False,
            )
        # slugify("Chase Checking") = "chase-checking" (new account created)
        assert aid == "chase-checking"
        assert "fuzzy" in caplog.text.lower() or "candidate" in caplog.text.lower()
    finally:
        db.close()
