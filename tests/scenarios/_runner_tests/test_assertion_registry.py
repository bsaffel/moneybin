"""Tests for the YAML-callable assertion registry."""

from tests.scenarios._runner._assertion_registry import ASSERTION_REGISTRY


def test_registry_includes_all_yaml_callable_assertions():
    """Registry covers exactly the 17 YAML-callable assertions."""
    expected = {
        "assert_balanced_transfers",
        "assert_column_types",
        "assert_columns_exist",
        "assert_date_continuity",
        "assert_distribution_within_bounds",
        "assert_migrations_at_head",
        "assert_min_rows",
        "assert_no_duplicates",
        "assert_no_nulls",
        "assert_no_orphans",
        "assert_no_unencrypted_db_files",
        "assert_row_count_delta",
        "assert_row_count_exact",
        "assert_sign_convention",
        "assert_sqlmesh_catalog_matches",
        "assert_unique_value_count",
        "assert_valid_foreign_keys",
    }
    assert set(ASSERTION_REGISTRY) == expected


def test_registry_values_are_callable():
    """Every registered assertion is callable."""
    for name, fn in ASSERTION_REGISTRY.items():
        assert callable(fn), f"{name} is not callable"
