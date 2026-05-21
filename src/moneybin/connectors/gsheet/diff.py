"""Pure diff logic for soft-delete-aware live mirroring."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DiffResult:
    """Result of diffing current and active row IDs."""

    to_insert: set[
        str
    ]  # rows present in source now; ingest (and undelete if previously soft-deleted)
    to_soft_delete: set[str]  # rows previously active but absent from current pull


def compute_diff(*, current_ids: set[str], active_ids: set[str]) -> DiffResult:
    """Compute the diff between a current pull's row IDs and the previously-active set.

    Semantics:
      - current - active → to_insert (new rows OR rows returning from soft-delete)
      - active - current → to_soft_delete (rows that disappeared from source)
      - current ∩ active → no-op (unchanged)
    """
    return DiffResult(
        to_insert=current_ids - active_ids,
        to_soft_delete=active_ids - current_ids,
    )
