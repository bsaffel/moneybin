# tests/moneybin/test_services/test_tax_service.py
"""Tests for TaxService."""

from __future__ import annotations

from collections.abc import Generator
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

import moneybin.database as db_module
from moneybin.database import Database
from moneybin.services.tax_service import (
    TaxService,
    W2Result,
    W2Summary,
)
from tests.moneybin.db_helpers import create_core_tables_raw


@pytest.fixture()
def tax_db(tmp_path: Path) -> Generator[Database, None, None]:
    """Yield a Database with raw tables and test W-2 data seeded."""
    mock_store = MagicMock()
    mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
    database = Database(
        tmp_path / "test.duckdb",
        secret_store=mock_store,
        no_auto_upgrade=True,
    )
    conn = database.conn
    create_core_tables_raw(conn)

    # Insert test W-2 data (includes PII that should NOT appear in results)
    conn.execute("""
        INSERT INTO raw.w2_forms (
            tax_year, employee_ssn, employer_ein,
            employee_first_name, employee_last_name,
            employer_name, wages, federal_income_tax,
            social_security_wages, social_security_tax,
            medicare_wages, medicare_tax,
            source_file, extracted_at
        ) VALUES
        (2025, '123-45-6789', '98-7654321', 'John', 'Doe',
         'Test Corp', 85000.00, 15000.00, 85000.00, 5270.00,
         85000.00, 1232.50, 'w2_2025.pdf', CURRENT_TIMESTAMP),
        (2024, '123-45-6789', '11-2233445', 'John', 'Doe',
         'Old Employer', 70000.00, 12000.00, 70000.00, 4340.00,
         70000.00, 1015.00, 'w2_2024.pdf', CURRENT_TIMESTAMP)
    """)  # noqa: S608  # test input, not executing SQL

    db_module._database_instance = database  # type: ignore[attr-defined]
    yield database
    db_module._database_instance = None  # type: ignore[attr-defined]
    database.close()


class TestW2:
    """Tests for TaxService.w2()."""

    @pytest.mark.unit
    def test_returns_w2_result(self, tax_db: Database) -> None:
        service = TaxService(tax_db)
        result = service.w2()
        assert isinstance(result, W2Result)
        assert len(result.forms) == 2

    @pytest.mark.unit
    def test_w2_fields(self, tax_db: Database) -> None:
        service = TaxService(tax_db)
        result = service.w2()
        form = next(f for f in result.forms if f.tax_year == 2025)
        assert isinstance(form, W2Summary)
        assert form.employer_name == "Test Corp"
        assert form.wages == Decimal("85000.00")
        assert form.federal_income_tax == Decimal("15000.00")
        assert form.social_security_wages == Decimal("85000.00")
        assert form.social_security_tax == Decimal("5270.00")
        assert form.medicare_wages == Decimal("85000.00")
        assert form.medicare_tax == Decimal("1232.50")

    @pytest.mark.unit
    def test_filter_by_tax_year(self, tax_db: Database) -> None:
        service = TaxService(tax_db)
        result = service.w2(tax_year=2025)
        assert len(result.forms) == 1
        assert result.forms[0].tax_year == 2025

    @pytest.mark.unit
    def test_ssn_not_in_output(self, tax_db: Database) -> None:
        """Verify SSN is excluded from W2Summary — PII protection."""
        service = TaxService(tax_db)
        result = service.w2()
        for form in result.forms:
            d = form.to_dict()
            assert "employee_ssn" not in d
            assert "ssn" not in d
            # Also verify it's not an attribute on the dataclass
            assert not hasattr(form, "employee_ssn")
            assert not hasattr(form, "ssn")

    @pytest.mark.unit
    def test_ein_not_in_output(self, tax_db: Database) -> None:
        """Verify EIN is excluded from W2Summary — PII protection."""
        service = TaxService(tax_db)
        result = service.w2()
        for form in result.forms:
            d = form.to_dict()
            assert "employer_ein" not in d
            assert "ein" not in d
            assert not hasattr(form, "employer_ein")
            assert not hasattr(form, "ein")

    @pytest.mark.unit
    def test_to_envelope_sensitivity_high(self, tax_db: Database) -> None:
        service = TaxService(tax_db)
        result = service.w2()
        envelope = result.to_envelope()
        d = envelope.to_dict()
        assert d["summary"]["sensitivity"] == "high"
        data: list[dict[str, Any]] = d["data"]
        assert len(data) == 2

    @pytest.mark.unit
    def test_envelope_json_no_pii(self, tax_db: Database) -> None:
        """Verify the serialized JSON contains no SSN or EIN."""
        service = TaxService(tax_db)
        result = service.w2()
        json_str = result.to_envelope().to_json()
        assert "123-45-6789" not in json_str
        assert "98-7654321" not in json_str
        assert "11-2233445" not in json_str


class TestEmptyResults:
    """Tests for service behavior with no data in tables."""

    @pytest.fixture()
    def empty_db(self, tmp_path: Path) -> Generator[Database, None, None]:
        mock_store = MagicMock()
        mock_store.get_key.return_value = "test-encryption-key-256bit-placeholder"
        database = Database(
            tmp_path / "test.duckdb",
            secret_store=mock_store,
            no_auto_upgrade=True,
        )
        create_core_tables_raw(database.conn)
        db_module._database_instance = database  # type: ignore[attr-defined]
        yield database
        db_module._database_instance = None  # type: ignore[attr-defined]
        database.close()

    @pytest.mark.unit
    def test_w2_empty_db(self, empty_db: Database) -> None:
        service = TaxService(empty_db)
        result = service.w2()
        assert isinstance(result, W2Result)
        assert result.forms == []
