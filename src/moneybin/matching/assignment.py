"""Greedy best-score-first 1:1 bipartite assignment.

When multiple candidates compete for the same source row, the highest-
scoring pair wins. Both rows in a winning pair are marked as "claimed"
and cannot participate in further assignments.
"""

from typing import Protocol


class _Matchable(Protocol):
    """Structural interface for candidate pairs (dedup or transfer)."""

    @property
    def source_type_a(self) -> str: ...
    @property
    def source_transaction_id_a(self) -> str: ...
    @property
    def source_type_b(self) -> str: ...
    @property
    def source_transaction_id_b(self) -> str: ...
    @property
    def confidence_score(self) -> float: ...


def assign_greedy[T: _Matchable](candidates: list[T]) -> list[T]:
    """Assign candidate pairs using greedy best-score-first.

    Args:
        candidates: Scored candidate pairs (any order).

    Returns:
        Non-overlapping subset of pairs, highest scores first.
    """
    sorted_candidates = sorted(
        candidates, key=lambda c: c.confidence_score, reverse=True
    )
    claimed: set[str] = set()
    assigned: list[T] = []

    for pair in sorted_candidates:
        key_a = f"{pair.source_type_a}|{pair.source_transaction_id_a}"
        key_b = f"{pair.source_type_b}|{pair.source_transaction_id_b}"
        if key_a not in claimed and key_b not in claimed:
            claimed.add(key_a)
            claimed.add(key_b)
            assigned.append(pair)

    return assigned
