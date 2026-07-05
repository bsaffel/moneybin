"""Tests for CategorizedBy and SOURCE_PRIORITY constants."""

import pytest

from moneybin.services.categorization._shared import (
    PLAID_MIN_CONFIDENCE,
    SOURCE_PRIORITY,
    plaid_confidence_to_numeric,
    priority_case_sql,
)


def test_provider_native_replaces_plaid_at_priority_6():
    assert "plaid" not in SOURCE_PRIORITY
    assert SOURCE_PRIORITY["provider_native"] == 6
    # ladder order preserved around it
    assert (
        SOURCE_PRIORITY["ml"]
        < SOURCE_PRIORITY["provider_native"]
        < SOURCE_PRIORITY["ai"]
    )


def test_priority_case_sql_uses_provider_native():
    sql = priority_case_sql("EXCLUDED.categorized_by")
    assert "WHEN 'provider_native' THEN 6" in sql
    assert "'plaid'" not in sql


@pytest.mark.parametrize(
    "level,expected",
    [
        ("VERY_HIGH", 0.99),
        ("HIGH", 0.90),
        ("MEDIUM", 0.70),
        ("LOW", 0.40),
        ("UNKNOWN", None),
        (None, None),
        ("garbage", None),
    ],
)
def test_plaid_confidence_to_numeric(level: str | None, expected: float | None) -> None:
    assert plaid_confidence_to_numeric(level) == expected


def test_gate_admits_medium_and_above() -> None:
    for lvl in ("VERY_HIGH", "HIGH", "MEDIUM"):
        val = plaid_confidence_to_numeric(lvl)
        assert val is not None and val >= PLAID_MIN_CONFIDENCE
    for lvl in ("LOW", "UNKNOWN"):
        val = plaid_confidence_to_numeric(lvl)
        assert val is None or val < PLAID_MIN_CONFIDENCE
