"""Assertion primitives — every function returns AssertionResult, never raises on data failure."""

from tests.validation.assertions.audits import assert_transform_audit
from tests.validation.assertions.completeness import (
    assert_no_nulls,
    assert_source_system_populated,
)
from tests.validation.assertions.distribution import (
    assert_distribution_within_bounds,
    assert_ground_truth_coverage,
    assert_unique_value_count,
)
from tests.validation.assertions.domain import (
    assert_amount_precision,
    assert_date_bounds,
    assert_date_continuity,
)
from tests.validation.assertions.infrastructure import (
    assert_migrations_at_head,
    assert_min_rows,
    assert_no_unencrypted_db_files,
    assert_sqlmesh_catalog_matches,
)
from tests.validation.assertions.integrity import (
    assert_no_orphans,
    assert_valid_foreign_keys,
)
from tests.validation.assertions.schema import (
    assert_column_types,
    assert_columns_exist,
    assert_row_count_delta,
    assert_row_count_exact,
    assert_schema_snapshot,
)
from tests.validation.assertions.uniqueness import assert_no_duplicates

__all__ = [
    "assert_amount_precision",
    "assert_column_types",
    "assert_columns_exist",
    "assert_date_bounds",
    "assert_date_continuity",
    "assert_distribution_within_bounds",
    "assert_ground_truth_coverage",
    "assert_migrations_at_head",
    "assert_min_rows",
    "assert_no_duplicates",
    "assert_no_nulls",
    "assert_no_orphans",
    "assert_no_unencrypted_db_files",
    "assert_row_count_delta",
    "assert_row_count_exact",
    "assert_schema_snapshot",
    "assert_source_system_populated",
    "assert_sqlmesh_catalog_matches",
    "assert_transform_audit",
    "assert_unique_value_count",
    "assert_valid_foreign_keys",
]
