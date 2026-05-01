"""Tests for the YAML-driven expectation registry."""

from tests.scenarios._runner._expectation_registry import (
    EXPECTATION_REGISTRY,
    verify_expectations,
)


def test_registry_covers_every_expectation_kind() -> None:
    """Registry covers every ExpectationSpec.kind value."""
    expected = {
        "match_decision",
        "gold_record_count",
        "category_for_transaction",
        "provenance_for_transaction",
        "transfers_match_ground_truth",
    }
    assert set(EXPECTATION_REGISTRY) == expected


def test_registry_values_are_callable() -> None:
    """Every adapter is callable."""
    for kind, fn in EXPECTATION_REGISTRY.items():
        assert callable(fn), f"{kind} adapter is not callable"


def test_verify_expectations_on_empty_list() -> None:
    """Empty input → empty output, no DB needed."""
    assert verify_expectations(db=None, specs=[]) == []  # type: ignore[arg-type]
