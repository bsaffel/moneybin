"""Explicit YAML-callable assertion registry.

Every entry is a contract: its name is part of scenario YAML's surface area.
Adding a new YAML-callable assertion requires explicitly registering it here —
this prevents accidental exposure of internal helpers that happen to start
with ``assert_``.
"""

from __future__ import annotations

from collections.abc import Callable

from moneybin.validation.assertions.completeness import assert_no_nulls
from moneybin.validation.assertions.distribution import (
    assert_distribution_within_bounds,
    assert_unique_value_count,
)
from moneybin.validation.assertions.domain import (
    assert_balanced_transfers,
    assert_date_continuity,
    assert_sign_convention,
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
from moneybin.validation.result import AssertionResult

# AssertionFn signature: (db: Database, **kwargs) -> AssertionResult.
AssertionFn = Callable[..., AssertionResult]

ASSERTION_REGISTRY: dict[str, AssertionFn] = {
    "assert_balanced_transfers": assert_balanced_transfers,
    "assert_column_types": assert_column_types,
    "assert_columns_exist": assert_columns_exist,
    "assert_date_continuity": assert_date_continuity,
    "assert_distribution_within_bounds": assert_distribution_within_bounds,
    "assert_migrations_at_head": assert_migrations_at_head,
    "assert_min_rows": assert_min_rows,
    "assert_no_duplicates": assert_no_duplicates,
    "assert_no_nulls": assert_no_nulls,
    "assert_no_orphans": assert_no_orphans,
    "assert_no_unencrypted_db_files": assert_no_unencrypted_db_files,
    "assert_row_count_delta": assert_row_count_delta,
    "assert_row_count_exact": assert_row_count_exact,
    "assert_sign_convention": assert_sign_convention,
    "assert_sqlmesh_catalog_matches": assert_sqlmesh_catalog_matches,
    "assert_unique_value_count": assert_unique_value_count,
    "assert_valid_foreign_keys": assert_valid_foreign_keys,
}


def resolve_assertion(name: str) -> AssertionFn:
    """Return the callable registered under ``name`` or raise KeyError."""
    if name not in ASSERTION_REGISTRY:
        raise KeyError(f"unknown assertion fn: {name!r}")
    return ASSERTION_REGISTRY[name]


__all__ = ["ASSERTION_REGISTRY", "AssertionFn", "resolve_assertion"]
