"""Rule B and the service-report type parity, from `.claude/rules/column-ordering.md`."""

from __future__ import annotations

from collections.abc import Sequence

import pytest

import moneybin.reports.definitions as definitions
from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.catalog import RegisteredReport
from moneybin.reports._framework.cli_register import resolve_default_columns
from moneybin.reports._framework.contract import OutputColumn
from moneybin.reports._framework.registry import discover_reports, spec_of
from moneybin.reports.service_reports import (
    _SNAPSHOT_COLUMN_TYPES,  # pyright: ignore[reportPrivateUsage]  # the pair under test
    _SNAPSHOT_COLUMNS,  # pyright: ignore[reportPrivateUsage]  # the pair under test
    SERVICE_REPORTS,
)

pytestmark = pytest.mark.unit

GRAIN, DESCRIPTIVE, DATE, PROVENANCE, MEASURE = range(5)

# Rule B names labels and dimensions as separate blocks, and the guard checks
# them as one. Which a name column is depends on whether it names the report's
# own grain — `merchant_normalized` is the label on `core:merchants` and a
# dimension on `core:large_transactions`, which is grained by transaction — and
# no declaration carries the grain. Ranking them apart would enforce a
# distinction the guard cannot see, so it checks only that both precede the
# dates. Their relative order is review's call.
_DESCRIPTIVE = frozenset({
    DataClass.USER_NOTE,
    DataClass.MERCHANT_NAME,
    DataClass.INSTITUTION,
    DataClass.DESCRIPTION,
    DataClass.CATEGORY,
    DataClass.CURRENCY,
    DataClass.TXN_TYPE,
})


def _rank(column: OutputColumn) -> int | None:
    """Rule B's category, or None when the declaration cannot say which.

    `AGGREGATE` carries both counts of the grain (`txn_count`) and derived
    scores (`confidence`, `drift_pct`), which belong in different blocks, and
    nothing distinguishes them. Ranking it either way would enforce a
    distinction the guard cannot see, so it is skipped by design.
    """
    if column.money_kind is not None:
        return MEASURE
    if column.data_class is DataClass.RECORD_ID:
        return GRAIN
    if column.data_class in _DESCRIPTIVE:
        return DESCRIPTIVE
    if column.data_class is DataClass.TXN_DATE:
        return DATE
    if column.data_class is DataClass.TIMESTAMP_OBSERVABILITY:
        return PROVENANCE
    return None


def _ranked(columns: Sequence[OutputColumn]) -> list[tuple[str, int]]:
    return [(c.name, rank) for c in columns if (rank := _rank(c)) is not None]


def _in_tree_reports() -> list[RegisteredReport]:
    runner_backed = [spec_of(runner) for runner in discover_reports(definitions)]
    return [*runner_backed, *SERVICE_REPORTS]


@pytest.mark.parametrize("spec", _in_tree_reports(), ids=lambda spec: spec.report_id)
def test_declared_columns_follow_grain_first(spec: RegisteredReport) -> None:
    """Rule B over the whole declared projection."""
    ranked = _ranked(spec.columns)
    ranks = [rank for _, rank in ranked]
    assert ranks == sorted(ranks), (
        f"{spec.report_id} declares columns out of Rule B order: "
        + ", ".join(f"{name}={rank}" for name, rank in ranked)
    )


@pytest.mark.parametrize("spec", _in_tree_reports(), ids=lambda spec: spec.report_id)
def test_default_columns_follow_grain_first(spec: RegisteredReport) -> None:
    """Rule B over the narrow text table, which orders independently."""
    by_name = {column.name: column for column in spec.columns}
    names = resolve_default_columns(spec, {})
    ranked = _ranked([by_name[name] for name in names])
    ranks = [rank for _, rank in ranked]
    assert ranks == sorted(ranks), (
        f"{spec.report_id} orders default_columns out of Rule B order: "
        + ", ".join(f"{name}={rank}" for name, rank in ranked)
    )


def test_snapshot_column_types_stay_parallel() -> None:
    """A service report's `column_types` is positional beside its columns tuple.

    Reordering one without the other hands every column the type of whichever
    column took its slot. This is the only thing standing between that and a
    caller, so it asserts length and the three anchors most likely to drift.
    """
    columns = [column.name for column in _SNAPSHOT_COLUMNS]
    types = _SNAPSHOT_COLUMN_TYPES
    assert len(columns) == len(types), (
        f"_SNAPSHOT_COLUMNS has {len(columns)} entries and _SNAPSHOT_COLUMN_TYPES "
        f"has {len(types)}; they are matched by position"
    )
    by_type = dict(zip(columns, types, strict=True))
    assert by_type["balance_date"] == "DATE"
    assert by_type["net_worth"] == "DECIMAL(18,2)"
    assert by_type["account_count"] == "BIGINT"
