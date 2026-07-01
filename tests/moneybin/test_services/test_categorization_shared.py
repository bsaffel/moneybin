"""Tests for CategorizedBy and SOURCE_PRIORITY constants."""

from moneybin.services.categorization._shared import SOURCE_PRIORITY, priority_case_sql


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
