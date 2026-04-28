"""Tests for the scenario pipeline step registry."""

from unittest.mock import MagicMock, patch

import pytest

from moneybin.testing.scenarios.loader import SetupSpec
from moneybin.testing.scenarios.steps import STEP_REGISTRY, run_step


def test_every_known_step_has_a_callable() -> None:
    """Every documented step name must resolve to a callable."""
    for name in {
        "generate",
        "load_fixtures",
        "transform",
        "match",
        "categorize",
        "migrate",
        "transform_via_subprocess",
    }:
        assert callable(STEP_REGISTRY[name]), name


def test_match_step_invokes_matching_service() -> None:
    """The match step constructs MatchingService(db) and calls .run()."""
    db = MagicMock()
    setup = SetupSpec(persona="family", seed=42, years=1)
    with patch("moneybin.testing.scenarios.steps.MatchingService") as svc:
        run_step("match", setup, db, env={})
    svc.assert_called_once_with(db)
    svc.return_value.run.assert_called_once()


def test_unknown_step_raises() -> None:
    """run_step raises KeyError for unregistered step names."""
    with pytest.raises(KeyError):
        run_step("does_not_exist", SetupSpec(persona="x"), MagicMock(), env={})
