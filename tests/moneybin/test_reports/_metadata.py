"""Shared metadata for synthetic report specs."""

from __future__ import annotations

from collections.abc import Mapping

from moneybin.privacy.taxonomy import DataClass
from moneybin.reports._framework.contract import OutputColumn, ReportSemantics

TEST_SEMANTICS = ReportSemantics(
    unit="count",
    currency=None,
    sign="non-negative",
    kind="count",
    valuation_basis=None,
    fx_basis=None,
    time_basis="point-in-time query result",
    denominator=None,
    comparison_window=None,
    exclusions=(),
    provenance=("reports.test_summary",),
)


def output_columns(classes: Mapping[str, DataClass]) -> tuple[OutputColumn, ...]:
    """Build matching columns for synthetic privacy maps."""
    return tuple(
        OutputColumn(name, f"Synthetic {name} output.", data_class)
        for name, data_class in classes.items()
    )
