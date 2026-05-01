"""Assertion primitives — every function returns AssertionResult, never raises on data failure."""

from moneybin.validation.assertions.business import (
    assert_balanced_transfers,
    assert_date_continuity,
    assert_sign_convention,
)
from moneybin.validation.assertions.completeness import assert_no_nulls
from moneybin.validation.assertions.distributional import (
    assert_distribution_within_bounds,
    assert_unique_value_count,
)
from moneybin.validation.assertions.infrastructure import (
    assert_migrations_at_head,
    assert_min_rows,
    assert_no_unencrypted_db_files,
    assert_sqlmesh_catalog_matches,
)
from moneybin.validation.assertions.integrity import (
    assert_no_orphans,
    assert_valid_foreign_keys,
)
from moneybin.validation.assertions.schema import (
    assert_column_types,
    assert_columns_exist,
    assert_row_count_delta,
    assert_row_count_exact,
)
from moneybin.validation.assertions.uniqueness import assert_no_duplicates

__all__ = [
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
]
