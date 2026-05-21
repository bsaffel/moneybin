"""Tests for package-framework exception __str__ formatting.

Each subclass adds structured fields that must appear in str(e) so callers
(validator CLI, MCP tool, framework startup) can surface precise diagnostics.
"""

from moneybin.packages._framework.errors import (
    CapabilityViolation,
    PrefixViolation,
    QualityScaleViolation,
    ValidationError,
)


def test_validation_error_str_includes_package_and_message() -> None:
    """Base ValidationError str() formats [package_name] message."""
    err = ValidationError(package_name="my_pkg", message="something went wrong")
    assert str(err) == "[my_pkg] something went wrong"


def test_capability_violation_str_includes_file_and_target() -> None:
    """CapabilityViolation str() includes sql_file and target fields."""
    err = CapabilityViolation(
        package_name="assets",
        message="write outside declared capabilities",
        sql_file="sql/create_table.sql",
        target="core.fct_transactions",
    )
    result = str(err)
    assert "[assets]" in result
    assert "write outside declared capabilities" in result
    assert "sql/create_table.sql" in result
    assert "core.fct_transactions" in result


def test_prefix_violation_str_includes_surface_and_offender() -> None:
    """PrefixViolation str() includes surface and offender fields."""
    err = PrefixViolation(
        package_name="us_tax",
        message="cross-prefix write detected",
        surface="sql_write",
        offender="assets.dim_holdings",
    )
    result = str(err)
    assert "[us_tax]" in result
    assert "cross-prefix write detected" in result
    assert "sql_write" in result
    assert "assets.dim_holdings" in result


def test_quality_scale_violation_str_includes_claimed_tier_and_missing_evidence() -> (
    None
):
    """QualityScaleViolation str() includes claimed_tier and missing_evidence fields."""
    err = QualityScaleViolation(
        package_name="my_pkg",
        message="tier claim not satisfied",
        claimed_tier="gold",
        missing_evidence="no integration test suite found",
    )
    result = str(err)
    assert "[my_pkg]" in result
    assert "tier claim not satisfied" in result
    assert "gold" in result
    assert "no integration test suite found" in result
