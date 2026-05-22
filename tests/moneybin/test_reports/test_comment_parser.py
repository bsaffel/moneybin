"""Tests for parse_report_sql — the @-block structured-comment parser."""

from pathlib import Path

import pytest

from moneybin.reports._framework.comment_parser import parse_report_sql

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def test_parses_minimal_report() -> None:
    spec = parse_report_sql(FIXTURE_DIR / "minimal_report.sql")
    assert spec.name == "minimal_demo"
    assert spec.description.startswith("One-liner")
    assert spec.params == []
    assert spec.examples == []
    assert spec.source_path == FIXTURE_DIR / "minimal_report.sql"


def test_parses_full_report_with_params_and_examples() -> None:
    spec = parse_report_sql(FIXTURE_DIR / "full_report.sql")
    assert spec.name == "seasonal_spending"
    assert "Seasonal spending" in spec.description
    assert len(spec.params) == 3

    year = spec.param("year")
    assert year.type_hint == "INTEGER"
    assert year.optional is True
    assert year.default is None
    assert "specific year" in year.doc

    categories = spec.param("categories")
    assert categories.type_hint == "TEXT[]"
    assert categories.default is None

    min_amount = spec.param("min_amount")
    assert min_amount.type_hint == "DECIMAL"
    assert min_amount.default == 0

    assert len(spec.examples) == 2
    assert spec.examples[0] == "reports_seasonal_spending(year=2025)"


def test_missing_name_raises() -> None:
    with pytest.raises(ValueError, match="missing.*@name"):
        parse_report_sql(FIXTURE_DIR / "malformed_report.sql")


def test_real_existing_report_parses() -> None:
    """Once Task 8 migrates net_worth.sql, this parses cleanly."""
    real = Path("sqlmesh/models/reports/net_worth.sql")
    if not real.exists() or "@name" not in real.read_text(encoding="utf-8"):
        pytest.skip("Migration of net_worth.sql happens in Task 8")
    spec = parse_report_sql(real)
    assert spec.name == "net_worth"
