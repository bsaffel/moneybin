"""The ``reconciliation`` marker column shared by holdings, gains and lots tables."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Protocol

RECONCILIATION_COLUMN = "reconciliation"


class _Reconcilable(Protocol):
    @property
    def reconciliation_status(self) -> str: ...


def with_reconciliation_column[T: _Reconcilable](
    columns: tuple[tuple[str, Callable[[T], object]], ...],
    default: Sequence[str],
    rows: Sequence[T],
) -> tuple[tuple[tuple[str, Callable[[T], object]], ...], tuple[str, ...]]:
    """Add the marker column, in the default set, only when a row is unreconciled.

    Unreconciled figures still print (docs/specs/reconciliation-status.md D4);
    the column marks which rows double-count. A clean table carries no column
    of dashes, and ``--output json`` always carries the field.
    """
    if not any(r.reconciliation_status == "unreconciled" for r in rows):
        return columns, tuple(default)

    def _marker(row: T) -> object:
        return "unreconciled" if row.reconciliation_status == "unreconciled" else "-"

    return (
        (*columns, (RECONCILIATION_COLUMN, _marker)),
        (*default, RECONCILIATION_COLUMN),
    )
