"""Side-effect-free whole-event Proposals over source-neutral comparison facts."""

from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from itertools import combinations
from typing import Any

from moneybin import error_codes
from moneybin.errors import UserError
from moneybin.investments.event_assignment import (
    MAX_COMPONENT_STATES,
    Candidate,
    EvidenceBand,
    solve_candidates,
)
from moneybin.investments.event_choices import issue_choices
from moneybin.investments.event_fingerprints import (
    ALGORITHM_VERSION,
    fingerprint,
    leg_dependencies,
    relationship_fingerprint,
)


@dataclass(frozen=True)
class Proposal:
    """Complete persisted review evidence; no acceptance authority."""

    members: tuple[str, ...]
    confidence_band: EvidenceBand
    relationship_fingerprint: str
    candidate_graph_fingerprint: str
    is_competing: bool
    auto_eligible: bool
    legs: tuple[dict[str, Any], ...]
    evidence: tuple[dict[str, Any], ...]
    field_choices: tuple[dict[str, Any], ...]
    supersedes_decision_ids: tuple[str, ...] = ()
    supersession: tuple[dict[str, Any], ...] = ()
    algorithm_version: str = ALGORITHM_VERSION
    alternatives: tuple[tuple[str, ...], ...] = ()
    downstream_effects: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PlanningResult:
    """The same constrained solve owns Proposals and rejection dispositions."""

    proposals: tuple[Proposal, ...]
    suppressed_relationships: tuple[str, ...]
    suppressed_bands: tuple[EvidenceBand, ...]


def _groups(
    nodes: set[str], neighbors: Mapping[str, set[str]]
) -> Iterator[tuple[str, ...]]:
    """Enumerate all cliques, including eligible subsets of maximal cliques.

    A fully-connected k-node component emits every subset of size >= 2 —
    2**k - k - 1 groups — so this recursion is exponential in the size of a
    *densely overlapping* component (repeated same-day events with
    identical evidence on both sides). Measured: k=25 emits ~33.5M groups in
    ~16s. The bound below is checked per connected component, not globally
    across this call's whole node set, and reuses event_assignment's
    MAX_COMPONENT_STATES/error code (same growth shape: emitted-group count
    tracks actual recursion work, the way that module's memo size does).
    Per-component matters here specifically: many small, disjoint,
    legitimate match pairs are common and must never trip a bound sized for
    one dense cluster — measured, a 1,001-node star (sparse, no triangle)
    emits only 1,000 groups in ~0.02s regardless of node count.
    """

    def expand(
        prefix: tuple[str, ...], available: list[str], count: list[int]
    ) -> Iterator[tuple[str, ...]]:
        for index, node in enumerate(available):
            group = (*prefix, node)
            if len(group) >= 2:
                count[0] += 1
                if count[0] > MAX_COMPONENT_STATES:
                    raise UserError(
                        f"Investment matching component ({len(component)} "
                        "source events densely connected) exceeded "
                        f"{MAX_COMPONENT_STATES} candidate relationship "
                        "subsets while enumerating cliques — its cost is "
                        "exponential for a large mutually compatible group, "
                        "so it refuses rather than risk a long stall. This "
                        "happens when many same-day events with identical "
                        "evidence (security, amount, date) repeat across "
                        "both histories.",
                        code=error_codes.INVESTMENT_MATCH_COMPONENT_TOO_LARGE,
                    )
                yield group
            yield from expand(
                group,
                [other for other in available[index + 1 :] if other in neighbors[node]],
                count,
            )

    remaining = set(nodes)
    while remaining:
        # Arbitrary, not sorted: picking a deterministic start (e.g. min())
        # here costs O(len(remaining)) per component and turns many small
        # components into O(n**2) overall — this only needs an O(1) pick.
        # Downstream order doesn't matter; drafts/candidates are deduped and
        # re-sorted independently in build_plan/solve_candidates.
        start = next(iter(remaining))
        component: set[str] = set()
        todo = {start}
        while todo:
            node = todo.pop()
            if node in component:
                continue
            component.add(node)
            todo.update((neighbors.get(node, set()) & remaining) - component)
        remaining -= component
        yield from expand((), sorted(component), [0])


def build_plan(
    headers: Sequence[Mapping[str, Any]],
    legs: Sequence[Mapping[str, Any]],
    evidence: Sequence[Mapping[str, Any]],
    *,
    decisions: Sequence[Mapping[str, Any]] = (),
    locked_components: Sequence[Mapping[str, Any]] = (),
) -> tuple[Proposal, ...]:
    """Return reviewable Proposals from the canonical constrained solve."""
    return evaluate_plan(
        headers,
        legs,
        evidence,
        decisions=decisions,
        locked_components=locked_components,
    ).proposals


def evaluate_plan(
    headers: Sequence[Mapping[str, Any]],
    legs: Sequence[Mapping[str, Any]],
    evidence: Sequence[Mapping[str, Any]],
    *,
    decisions: Sequence[Mapping[str, Any]] = (),
    locked_components: Sequence[Mapping[str, Any]] = (),
) -> PlanningResult:
    """Plan complete relationships without writes, metrics, or ledger changes."""
    by_event: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for leg in legs:
        by_event[leg["source_event_key"]].append(leg)
    nodes = {
        row["source_event_key"]: row
        for row in headers
        if row["is_match_eligible"]
        and row["member_count"] == len(by_event[row["source_event_key"]])
    }
    pair_rows: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in evidence:
        pair_rows[
            tuple(sorted((row["left_source_event_key"], row["right_source_event_key"])))
        ].append(row)
    bands: dict[tuple[str, str], EvidenceBand] = {}
    neighbors: dict[str, set[str]] = defaultdict(set)
    for (a, b), rows in pair_rows.items():
        if a not in nodes or b not in nodes:
            continue
        if (nodes[a]["source_type"], nodes[a]["source_origin"]) == (
            nodes[b]["source_type"],
            nodes[b]["source_origin"],
        ):
            continue
        if len(rows) != len(by_event[a]) or len(rows) != len(by_event[b]):
            continue
        if not all(row["is_candidate"] for row in rows):
            continue
        if len({row["left_native_reference"] for row in rows}) != len(rows) or len({
            row["right_native_reference"] for row in rows
        }) != len(rows):
            continue
        bands[a, b] = (
            "native"
            if all(row["has_validated_native_relationship"] for row in rows)
            else "exact"
            if all(row["is_exact_economic_identity"] for row in rows)
            else "fuzzy"
        )
        neighbors[a].add(b)
        neighbors[b].add(a)

    reserved: dict[str, set[str]] = {}
    blocked: set[str] = set()
    topology: dict[str, dict[str, Any]] = {}
    row_events: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for leg in legs:
        row_events[
            leg["source_type"], leg["source_origin"], leg["native_reference"]
        ].add(leg["source_event_key"])
    for locked in locked_components:
        reserved_rows: list[tuple[str, str, str]] = [
            (row[0], row[1], row[2]) for row in locked["reserved_rows"]
        ]
        successors = {event for row in reserved_rows for event in row_events[row]}
        reserved[locked["decision_id"]] = successors
        if (
            not reserved_rows
            or any(len(row_events[row]) != 1 for row in reserved_rows)
            or not successors <= nodes.keys()
        ):
            blocked.update(successors)
        topology[locked["decision_id"]] = {
            **dict(locked),
            "members": sorted(locked["members"]),
            "reserved_rows": sorted(reserved_rows),
            "current_successors": sorted(successors),
        }

    # The graph remains complete before rejection pruning; unrelated alternatives
    # must stale prior Proposals without reopening an unchanged rejected subset.
    graph_neighbors = {key: set(value) for key, value in neighbors.items()}
    for successors in reserved.values():
        for node in successors:
            graph_neighbors.setdefault(node, set()).update(successors - {node})
    graph_for: dict[str, str] = {}
    remaining = set(nodes)
    rejected = [row for row in decisions if row["status"] == "rejected"]
    while remaining:
        component: set[str] = set()
        todo = {min(remaining)}
        while todo:
            node = todo.pop()
            if node in component:
                continue
            component.add(node)
            todo.update(graph_neighbors.get(node, set()) - component)
        remaining -= component
        graph = fingerprint(
            "investment_graph",
            {
                "nodes": sorted(
                    [
                        leg_dependencies(leg)
                        for leg in legs
                        if leg["source_event_key"] in component
                    ],
                    key=lambda row: (row["source_event_key"], row["native_reference"]),
                ),
                "edges": sorted(
                    [
                        dict(row)
                        for row in evidence
                        if row["left_source_event_key"] in component
                        and row["right_source_event_key"] in component
                    ],
                    key=lambda row: (
                        row["left_source_event_key"],
                        row["right_source_event_key"],
                        row["leg_role"],
                    ),
                ),
                "rejections": sorted([
                    (sorted(row["members"]), row["relationship_fingerprint"])
                    for row in rejected
                    if set(row["members"]) <= component
                ]),
                "supersession": [
                    topology[key]
                    for key in sorted(topology)
                    if reserved[key] & component
                ],
            },
        )
        graph_for.update(dict.fromkeys(component, graph))

    drafts: dict[
        Candidate, tuple[str, tuple[dict[str, Any], ...], tuple[dict[str, Any], ...]]
    ] = {}
    suppressed: dict[str, EvidenceBand] = {}
    for members in _groups(set(nodes) - blocked, neighbors):
        selected = set(members)
        supersedes = tuple(
            sorted(key for key, values in reserved.items() if values & selected)
        )
        if any(not reserved[key] <= selected for key in supersedes):
            continue
        supersession = tuple(topology[key] for key in supersedes)
        if (
            supersedes
            and len(supersedes) == 1
            and selected == set(topology[supersedes[0]]["members"])
            and topology[supersedes[0]].get("status", "accepted") != "stale"
        ):
            # Unchanged accepted membership is not another replacement Proposal.
            continue
        relationship = relationship_fingerprint(members, legs, evidence, supersession)
        is_rejected = any(
            set(row["members"]) <= selected
            and relationship_fingerprint(
                row["members"],
                legs,
                evidence,
                tuple(
                    item
                    for item in supersession
                    if set(item["current_successors"]) <= set(row["members"])
                ),
            )
            == row["relationship_fingerprint"]
            for row in rejected
        )
        band: EvidenceBand = min(
            (bands[pair] for pair in combinations(members, 2)),
            key=lambda value: {"fuzzy": 0, "exact": 1, "native": 2}[value],
        )
        member_legs = [leg for member in members for leg in by_event[member]]
        days = max(
            row.get("date_threshold_days", 0)
            for pair in combinations(members, 2)
            for row in pair_rows[pair]
        )
        choices = issue_choices(
            member_legs, relationship=relationship, date_threshold_days=days
        )
        if choices is None:
            continue
        if is_rejected:
            suppressed[relationship] = band
            continue
        drafts[Candidate(members, band, supersedes)] = (
            relationship,
            choices,
            supersession,
        )
    solved = solve_candidates(list(drafts))
    competing_graphs = {
        graph_for[item.candidate.members[0]]
        for item in solved.proposals
        if item.is_competing
    }
    proposals: list[Proposal] = []
    for assigned in solved.proposals:
        candidate = assigned.candidate
        relationship, choices, supersession = drafts[candidate]
        members = candidate.members
        proposals.append(
            Proposal(
                members,
                candidate.confidence_band,
                relationship,
                graph_for[members[0]],
                assigned.is_competing,
                graph_for[members[0]] not in competing_graphs
                and candidate.confidence_band != "fuzzy"
                and not choices
                and not candidate.supersedes_decision_ids,
                tuple(
                    dict(leg)
                    for member in members
                    for leg in sorted(
                        by_event[member], key=lambda leg: leg["native_reference"]
                    )
                ),
                tuple(
                    dict(row)
                    for pair in combinations(members, 2)
                    for row in pair_rows[pair]
                ),
                choices,
                candidate.supersedes_decision_ids,
                supersession,
                alternatives=tuple(
                    other.candidate.members
                    for other in solved.proposals
                    if other.candidate != candidate
                    and other.candidate.claims & candidate.claims
                ),
                downstream_effects={
                    "golden_membership_changed": False,
                    "source_events_to_consolidate": len(members),
                    "affected_outputs": [
                        "lots",
                        "holdings",
                        "realized_gains",
                        "income",
                        "fees",
                    ],
                },
            )
        )
    return PlanningResult(
        tuple(proposals),
        tuple(sorted(suppressed)),
        tuple(suppressed[key] for key in sorted(suppressed)),
    )
