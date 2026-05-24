"""Greedy best-score-first 1:1 bipartite assignment.

When multiple candidates compete for the same source row, the highest-
scoring pair wins. Both rows in a winning pair are marked as "claimed"
and cannot participate in further assignments.
"""

from collections.abc import Iterable
from typing import TYPE_CHECKING, Literal, Protocol

if TYPE_CHECKING:
    from moneybin.matching.scoring import CandidatePair


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
    @property
    def account_id_a(self) -> str | None: ...
    @property
    def account_id_b(self) -> str | None: ...


def _claim_key(pair: _Matchable, side: Literal["a", "b"]) -> str:
    """Build a claim key for greedy assignment.

    For transfer pairs (which have account_id_a/account_id_b), include
    account_id to prevent false collisions when account-scoped IDs repeat
    across different accounts.
    """
    if side == "a":
        st = pair.source_type_a
        stid = pair.source_transaction_id_a
        acct = pair.account_id_a
    else:
        st = pair.source_type_b
        stid = pair.source_transaction_id_b
        acct = pair.account_id_b

    if acct is not None:
        return f"{st}|{acct}|{stid}"
    return f"{st}|{stid}"


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
        key_a = _claim_key(pair, "a")
        key_b = _claim_key(pair, "b")
        if key_a not in claimed and key_b not in claimed:
            claimed.add(key_a)
            claimed.add(key_b)
            assigned.append(pair)

    return assigned


# (source_type, source_transaction_id, account_id) — the full triple is the
# node identity. The Stage-1 prep fold (int_transactions__matched.sql) packs
# nodes as source_type || '|' || source_transaction_id grouped per account_id;
# this union-find MUST key on the same 3-tuple so the matcher and the fold
# compute identical components. Stripping source_type would silently diverge.
type NodeKey = tuple[str, str, str]


def _node_a(pair: "CandidatePair") -> NodeKey:
    return (pair.source_type_a, pair.source_transaction_id_a, pair.account_id)


def _node_b(pair: "CandidatePair") -> NodeKey:
    return (pair.source_type_b, pair.source_transaction_id_b, pair.account_id)


class UnionFind:
    """Connected-component union-find over node keys.

    Public because dedup component identity must be computed identically in
    three places — `assign_components` (matcher), `engine` (transfer
    secondary-exclusion), and the pending-match clustering — and the prep fold.
    """

    def __init__(self) -> None:
        """Initialize an empty union-find."""
        self._parent: dict[NodeKey, NodeKey] = {}

    def find(self, x: NodeKey) -> NodeKey:
        """Return the canonical root of x's set, with path compression."""
        self._parent.setdefault(x, x)
        root = x
        while self._parent[root] != root:
            root = self._parent[root]
        while self._parent[x] != root:  # path compression
            self._parent[x], x = root, self._parent[x]
        return root

    def union(self, a: NodeKey, b: NodeKey) -> bool:
        """Merge a and b. Return True if they were in different sets (edge added)."""
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return False
        self._parent[ra] = rb
        return True


def connected_components(
    edges: Iterable[tuple[NodeKey, NodeKey]],
) -> list[list[NodeKey]]:
    """Group the nodes touched by ``edges`` into connected components.

    Returns one member list per component; each touched node appears in exactly
    one list. The single grouping primitive shared by transfer secondary-
    exclusion and pending-match clustering (the matcher's `assign_components` is
    distinct — it builds the spanning forest incrementally rather than grouping
    a fixed edge set).
    """
    uf = UnionFind()
    seen: set[NodeKey] = set()
    ordered: list[NodeKey] = []
    for a, b in edges:
        uf.union(a, b)
        for node in (a, b):
            if node not in seen:
                seen.add(node)
                ordered.append(node)
    groups: dict[NodeKey, list[NodeKey]] = {}
    for node in ordered:
        groups.setdefault(uf.find(node), []).append(node)
    return list(groups.values())


def assign_components(
    candidates: "list[CandidatePair]",
    *,
    seed_edges: list[tuple[NodeKey, NodeKey]],
) -> "list[CandidatePair]":
    """Greedy spanning-forest assignment via union-find.

    Seeds with existing active edges so a new copy attaches to a pre-existing
    component and redundant edges are never re-proposed. Processes candidates
    best-confidence-first with a deterministic tiebreak; keeps an edge only when
    it joins two distinct components (N-1 edges per group, no cycles).
    """
    uf = UnionFind()
    for a, b in seed_edges:
        uf.union(a, b)
    ordered = sorted(
        candidates,
        key=lambda c: (
            -c.confidence_score,
            c.source_type_a,
            c.source_transaction_id_a,
            c.source_type_b,
            c.source_transaction_id_b,
        ),
    )
    added: list[CandidatePair] = []
    for pair in ordered:
        if uf.union(_node_a(pair), _node_b(pair)):
            added.append(pair)
    return added
