"""Exact whole-event set packing with every optimal relationship retained."""

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

type EvidenceBand = Literal["native", "exact", "fuzzy"]
type Objective = tuple[int, int, int]
type Solution = tuple[Objective, int, int]


@dataclass(frozen=True)
class Candidate:
    """One eligible, complete Proposal and its indivisible prior-Match claims."""

    members: tuple[str, ...]
    confidence_band: EvidenceBand
    supersedes_decision_ids: tuple[str, ...] = ()

    @property
    def objective(self) -> Objective:
        """Count each member once at the Proposal's weakest evidence band."""
        count = len(self.members)
        return (
            count,
            count if self.confidence_band != "fuzzy" else 0,
            count if self.confidence_band == "native" else 0,
        )

    @property
    def claims(self) -> frozenset[tuple[str, str]]:
        """Keep source-event and prior-decision identity namespaces distinct."""
        return frozenset(
            [("event", member) for member in self.members]
            + [("decision", decision) for decision in self.supersedes_decision_ids]
        )


@dataclass(frozen=True)
class AssignedProposal:
    """A relationship occurring in at least one optimal assignment."""

    candidate: Candidate
    is_competing: bool


@dataclass(frozen=True)
class Assignment:
    """Optimal objective and the union of its reviewable relationships."""

    objective: Objective
    proposals: tuple[AssignedProposal, ...]


def _add(left: Objective, right: Objective) -> Objective:
    return (left[0] + right[0], left[1] + right[1], left[2] + right[2])


def _solve_component(candidates: list[Candidate]) -> Assignment:
    claims = sorted({claim for candidate in candidates for claim in candidate.claims})
    claim_bits = {claim: 1 << index for index, claim in enumerate(claims)}
    masks = [sum(claim_bits[c] for c in candidate.claims) for candidate in candidates]
    by_bit: dict[int, list[int]] = defaultdict(list)
    for index, mask in enumerate(masks):
        for bit in claim_bits.values():
            if mask & bit:
                by_bit[bit].append(index)

    memo: dict[int, Solution] = {0: ((0, 0, 0), 0, 0)}
    transitions: dict[int, tuple[int, list[int]]] = {}
    initial = (1 << len(claims)) - 1
    stack = [initial]
    while stack:
        available = stack[-1]
        if available in memo:
            stack.pop()
            continue
        if available not in transitions:
            feasible = [
                index for index, mask in enumerate(masks) if mask & available == mask
            ]
            if not feasible:
                memo[available] = ((0, 0, 0), 0, 0)
                continue
            used = 0
            for index in feasible:
                used |= masks[index]
            bit = used & -used
            options = [
                index
                for index in by_bit[bit]
                if masks[index] & available == masks[index]
            ]
            transitions[available] = bit, options
        bit, options = transitions[available]
        dependencies = [
            available ^ bit,
            *(available ^ masks[index] for index in options),
        ]
        missing = [state for state in dependencies if state not in memo]
        if missing:
            stack.extend(missing)
            continue
        best, possible, required = memo[available ^ bit]
        for index in options:
            score, union, intersection = memo[available ^ masks[index]]
            score = _add(score, candidates[index].objective)
            union |= 1 << index
            intersection |= 1 << index
            if score > best:
                best, possible, required = score, union, intersection
            elif score == best:
                possible |= union
                required &= intersection
        memo[available] = best, possible, required
        stack.pop()

    score, possible, required = memo[initial]
    return Assignment(
        score,
        tuple(
            AssignedProposal(candidate, not bool(required & (1 << index)))
            for index, candidate in enumerate(candidates)
            if possible & (1 << index)
        ),
    )


def solve_candidates(candidates: Sequence[Candidate]) -> Assignment:
    """Solve independent claim components; order only controls presentation."""
    ordered = sorted(
        set(candidates),
        key=lambda candidate: (candidate.members, candidate.supersedes_decision_ids),
    )
    by_claim: dict[tuple[str, str], set[int]] = defaultdict(set)
    for index, candidate in enumerate(ordered):
        for claim in candidate.claims:
            by_claim[claim].add(index)
    remaining = set(range(len(ordered)))
    objective: Objective = (0, 0, 0)
    proposals: list[AssignedProposal] = []
    while remaining:
        todo = {min(remaining)}
        component: set[int] = set()
        while todo:
            index = todo.pop()
            if index in component:
                continue
            component.add(index)
            for claim in ordered[index].claims:
                todo.update(by_claim[claim] - component)
        remaining -= component
        result = _solve_component([ordered[index] for index in sorted(component)])
        objective = _add(objective, result.objective)
        proposals.extend(result.proposals)
    return Assignment(
        objective, tuple(sorted(proposals, key=lambda p: p.candidate.members))
    )
