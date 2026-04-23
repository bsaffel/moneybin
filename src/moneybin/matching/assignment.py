"""Greedy best-score-first 1:1 bipartite assignment.

When multiple candidates compete for the same source row, the highest-
scoring pair wins. Both rows in a winning pair are marked as "claimed"
and cannot participate in further assignments.
"""

from moneybin.matching.scoring import CandidatePair


def assign_greedy(candidates: list[CandidatePair]) -> list[CandidatePair]:
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
    assigned: list[CandidatePair] = []

    for pair in sorted_candidates:
        key_a = f"{pair.source_type_a}|{pair.source_transaction_id_a}"
        key_b = f"{pair.source_type_b}|{pair.source_transaction_id_b}"
        if key_a not in claimed and key_b not in claimed:
            claimed.add(key_a)
            claimed.add(key_b)
            assigned.append(pair)

    return assigned
