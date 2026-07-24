"""Tests for deterministic entity-reference resolution."""

from moneybin.services.entity_reference import (
    AmbiguousEntity,
    EntityCandidate,
    MissingEntity,
    ResolvedEntity,
    resolve_entity_reference,
)

CANDIDATES = (
    EntityCandidate(
        entity_id="acct_1", display_name="Everyday Checking", aliases=("checking",)
    ),
    EntityCandidate(entity_id="acct_2", display_name="acct_2"),
)

COLLIDING_CANDIDATES = (
    EntityCandidate(entity_id="acct_2", display_name="checking  "),
    EntityCandidate(entity_id="acct_1", display_name=" checking "),
)


def test_explicit_id_wins_over_name_collision() -> None:
    result = resolve_entity_reference("acct_2", CANDIDATES)
    assert result == ResolvedEntity(entity_id="acct_2", matched_by="id")


def test_exact_display_name_and_alias_are_case_insensitive() -> None:
    assert resolve_entity_reference("EVERYDAY CHECKING", CANDIDATES) == ResolvedEntity(
        entity_id="acct_1", matched_by="exact"
    )
    assert resolve_entity_reference("CHECKING", CANDIDATES) == ResolvedEntity(
        entity_id="acct_1", matched_by="exact"
    )


def test_normalized_match_resolves_collapsed_whitespace_and_nfkc() -> None:
    candidates = (EntityCandidate(entity_id="acct_1", display_name="Café Account"),)
    assert resolve_entity_reference(
        "  CAFÉ\u3000account  ", candidates
    ) == ResolvedEntity(entity_id="acct_1", matched_by="normalized")


def test_normalized_match_must_be_unique() -> None:
    result = resolve_entity_reference("checking", COLLIDING_CANDIDATES)
    assert isinstance(result, AmbiguousEntity)
    assert result.candidate_ids == ("acct_1", "acct_2")


def test_unknown_reference_returns_structured_missing_result() -> None:
    assert resolve_entity_reference("vacation", CANDIDATES) == MissingEntity(
        reference="vacation"
    )
