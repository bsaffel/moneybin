"""Explicit YAML-callable assertion registry.

Every entry is a contract: its name is part of scenario YAML's surface area.
Adding a new YAML-callable assertion requires explicitly registering it here —
this prevents accidental exposure of internal helpers that happen to start
with ``assert_``.

A data-quality check on a ``core.*`` relation does NOT get an entry of its own.
It is written once as SQLMesh audit SQL under ``src/moneybin/sqlmesh/audits/``
and reached from YAML through ``assert_transform_audit``:

    - name: sign_convention
      fn: assert_transform_audit
      args:
        audit: fct_transactions_sign_convention

That keeps a scenario and ``moneybin system doctor`` reading one definition.
"""

from __future__ import annotations

from collections.abc import Callable

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
from tests.validation.result import AssertionResult

AssertionFn = Callable[..., AssertionResult]

ASSERTION_REGISTRY: dict[str, AssertionFn] = {
    "assert_amount_precision": assert_amount_precision,
    "assert_column_types": assert_column_types,
    "assert_columns_exist": assert_columns_exist,
    "assert_date_bounds": assert_date_bounds,
    "assert_date_continuity": assert_date_continuity,
    "assert_distribution_within_bounds": assert_distribution_within_bounds,
    "assert_ground_truth_coverage": assert_ground_truth_coverage,
    "assert_migrations_at_head": assert_migrations_at_head,
    "assert_min_rows": assert_min_rows,
    "assert_no_duplicates": assert_no_duplicates,
    "assert_no_nulls": assert_no_nulls,
    "assert_no_orphans": assert_no_orphans,
    "assert_no_unencrypted_db_files": assert_no_unencrypted_db_files,
    "assert_row_count_delta": assert_row_count_delta,
    "assert_row_count_exact": assert_row_count_exact,
    "assert_schema_snapshot": assert_schema_snapshot,
    "assert_source_system_populated": assert_source_system_populated,
    "assert_sqlmesh_catalog_matches": assert_sqlmesh_catalog_matches,
    "assert_transform_audit": assert_transform_audit,
    "assert_unique_value_count": assert_unique_value_count,
    "assert_valid_foreign_keys": assert_valid_foreign_keys,
}


def resolve_assertion(name: str) -> AssertionFn:
    """Return the callable registered under ``name`` or raise KeyError."""
    if name not in ASSERTION_REGISTRY:
        raise KeyError(f"unknown assertion fn: {name!r}")
    return ASSERTION_REGISTRY[name]


__all__ = ["ASSERTION_REGISTRY", "AssertionFn", "resolve_assertion"]
