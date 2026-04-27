"""Assertion primitives — every function returns AssertionResult, never raises on data failure."""

from moneybin.validation.assertions.relational import (
    assert_no_duplicates,
    assert_no_nulls,
    assert_no_orphans,
    assert_valid_foreign_keys,
)
from moneybin.validation.assertions.schema import (
    assert_column_types,
    assert_columns_exist,
    assert_row_count_delta,
    assert_row_count_exact,
)

__all__ = [
    "assert_no_duplicates",
    "assert_no_nulls",
    "assert_no_orphans",
    "assert_valid_foreign_keys",
    "assert_columns_exist",
    "assert_column_types",
    "assert_row_count_delta",
    "assert_row_count_exact",
]
