"""Deterministic resolution of user-facing entity references."""

from __future__ import annotations

import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class EntityCandidate:
    """One entity that can be selected by an ID, name, or alias."""

    entity_id: str
    display_name: str
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ResolvedEntity:
    """A reference that resolved at a named rung of the ladder."""

    entity_id: str
    matched_by: Literal["id", "exact", "normalized"]


@dataclass(frozen=True, slots=True)
class AmbiguousEntity:
    """A reference matching multiple candidates at the same rung."""

    reference: str
    candidate_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class MissingEntity:
    """A reference that did not match any candidate."""

    reference: str


EntityResolution = ResolvedEntity | AmbiguousEntity | MissingEntity


def resolve_entity_reference(
    reference: str, candidates: Iterable[EntityCandidate]
) -> EntityResolution:
    """Resolve a reference by ID, exact text, then unique normalized text."""
    candidate_list = tuple(candidates)

    id_matches = {
        candidate.entity_id
        for candidate in candidate_list
        if candidate.entity_id == reference
    }
    if id_matches:
        return _resolution(reference, id_matches, "id")

    exact_matches = {
        candidate.entity_id
        for candidate in candidate_list
        if any(value.casefold() == reference.casefold() for value in _names(candidate))
    }
    if exact_matches:
        return _resolution(reference, exact_matches, "exact")

    normalized_reference = normalize_reference(reference)
    normalized_matches = {
        candidate.entity_id
        for candidate in candidate_list
        if any(
            normalize_reference(value) == normalized_reference
            for value in _names(candidate)
        )
    }
    if normalized_matches:
        return _resolution(reference, normalized_matches, "normalized")

    return MissingEntity(reference=reference)


def _names(candidate: EntityCandidate) -> tuple[str, ...]:
    """Every name this candidate answers to, skipping the ones that say nothing.

    An empty name compares equal to itself, so an entity that has none would
    otherwise match an empty reference -- and match it against every other
    nameless entity at the same time. Filtering here rather than at each builder
    keeps that true for every entity type the contract serves.
    """
    return tuple(
        value for value in (candidate.display_name, *candidate.aliases) if value
    )


def normalize_reference(value: str) -> str:
    """Fold a reference to the form the third resolution rung compares.

    Public because a caller that *reserves* a name has to fold it exactly the
    way the matcher does. Guarding on a weaker fold leaves the difference as a
    hole: a stored name that this collapses onto a reserved one still matches a
    request for it.
    """
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def _resolution(
    reference: str,
    candidate_ids: set[str],
    matched_by: Literal["id", "exact", "normalized"],
) -> EntityResolution:
    if len(candidate_ids) == 1:
        return ResolvedEntity(
            entity_id=next(iter(candidate_ids)), matched_by=matched_by
        )
    return AmbiguousEntity(
        reference=reference, candidate_ids=tuple(sorted(candidate_ids))
    )
