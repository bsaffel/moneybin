"""Tests for the scenario pipeline step registry."""

from unittest.mock import MagicMock, patch

import pytest

from tests.scenarios._runner.loader import VALID_STEP_NAMES, SetupSpec
from tests.scenarios._runner.steps import STEP_REGISTRY, run_step


def test_step_names_match_registry() -> None:
    """`VALID_STEP_NAMES` and `STEP_REGISTRY` keys must stay in lockstep."""
    assert set(STEP_REGISTRY) == VALID_STEP_NAMES


def test_every_known_step_has_a_callable() -> None:
    """Every documented step name must resolve to a callable."""
    for name in {
        "generate",
        "load_fixtures",
        "transform",
        "match",
        "seed_merchants",
        "categorize",
        "migrate",
        "transform_via_subprocess",
    }:
        assert callable(STEP_REGISTRY[name]), name


def test_match_step_invokes_matching_service() -> None:
    """The match step constructs MatchingService(db) and calls .run()."""
    db = MagicMock()
    setup = SetupSpec(persona="family", seed=42, years=1)
    with patch("tests.scenarios._runner.steps.MatchingService") as svc:
        run_step("match", setup, db, env={})
    svc.assert_called_once_with(db)
    svc.return_value.run.assert_called_once_with(auto_accept_transfers=True)


def test_unknown_step_raises() -> None:
    """run_step raises KeyError for unregistered step names."""
    with pytest.raises(KeyError):
        run_step("does_not_exist", SetupSpec(persona="x"), MagicMock(), env={})
