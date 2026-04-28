"""Pydantic-backed scenario YAML loader."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

# Lazy import to avoid circular dependency with steps.py.
_VALID_STEP_NAMES = {
    "generate",
    "load_fixtures",
    "transform",
    "match",
    "categorize",
    "migrate",
    "transform_via_subprocess",
}

REPO_ROOT = Path(__file__).resolve().parents[4]
FIXTURES_ROOT = (REPO_ROOT / "tests" / "fixtures").resolve()


class ScenarioValidationError(ValueError):
    """Raised when a scenario YAML fails Pydantic validation."""


class FixtureSpec(BaseModel):
    """A fixture file referenced by a scenario's setup block."""

    model_config = ConfigDict(extra="forbid")
    path: str
    account: str
    source_type: Literal["csv", "ofx", "pdf"]

    @model_validator(mode="after")
    def _validate_path(self) -> FixtureSpec:
        resolved = (REPO_ROOT / self.path).resolve()
        try:
            resolved.relative_to(FIXTURES_ROOT)
        except ValueError as exc:
            raise ValueError(
                f"fixture path {self.path!r} must resolve under tests/fixtures/"
            ) from exc
        return self


class SetupSpec(BaseModel):
    """Scenario setup: persona, seed, year span, and fixtures."""

    model_config = ConfigDict(extra="forbid")
    persona: str
    seed: int = 42
    years: int = 1
    fixtures: list[FixtureSpec] = Field(default_factory=list)


class AssertionSpec(BaseModel):
    """A single assertion entry in a scenario."""

    model_config = ConfigDict(extra="forbid")
    name: str
    fn: str
    args: dict[str, Any] = Field(default_factory=dict)


class ThresholdSpec(BaseModel):
    """Minimum threshold for an evaluation metric."""

    model_config = ConfigDict(extra="forbid")
    metric: str
    min: float


class EvaluationSpec(BaseModel):
    """A scored evaluation with a minimum threshold."""

    model_config = ConfigDict(extra="forbid")
    name: str
    fn: str
    threshold: ThresholdSpec
    args: dict[str, Any] = Field(default_factory=dict)


class ExpectationSpec(BaseModel):
    """A typed expectation about pipeline behavior.

    Free-form per-kind body; verifier enforces shape.
    """

    # Free-form per-kind body; verifier enforces shape.
    model_config = ConfigDict(extra="allow")
    kind: Literal[
        "match_decision",
        "gold_record_count",
        "category_for_transaction",
        "provenance_for_transaction",
    ]
    description: str = ""


class GatesSpec(BaseModel):
    """Selectors controlling which checks gate scenario success."""

    model_config = ConfigDict(extra="forbid")
    required_assertions: Literal["all"] | list[str] = "all"
    required_evaluations: Literal["all"] | list[str] = "all"
    required_expectations: Literal["all"] | list[str] = "all"


class Scenario(BaseModel):
    """A complete validated scenario specification."""

    model_config = ConfigDict(extra="forbid")
    name: str = Field(alias="scenario")
    description: str = ""
    setup: SetupSpec
    pipeline: list[str]
    assertions: list[AssertionSpec] = Field(default_factory=list)
    evaluations: list[EvaluationSpec] = Field(default_factory=list)
    expectations: list[ExpectationSpec] = Field(default_factory=list)
    gates: GatesSpec = Field(default_factory=GatesSpec)

    @model_validator(mode="after")
    def _validate_steps(self) -> Scenario:
        unknown = [s for s in self.pipeline if s not in _VALID_STEP_NAMES]
        if unknown:
            raise ValueError(f"unknown pipeline steps: {unknown}")
        return self


def load_scenario_from_string(raw: str) -> Scenario:
    """Parse and validate a scenario from a YAML string."""
    try:
        data = yaml.safe_load(raw)
        return Scenario.model_validate(data)
    except (ValidationError, ValueError, yaml.YAMLError) as exc:
        raise ScenarioValidationError(str(exc)) from exc


def load_scenario(path: Path) -> Scenario:
    """Load and validate a scenario from a YAML file path."""
    return load_scenario_from_string(path.read_text())


SHIPPED_SCENARIOS_DIR = Path(__file__).parent / "data"


def list_shipped_scenarios() -> list[Scenario]:
    """Load all shipped scenario YAMLs bundled with the package."""
    return [load_scenario(p) for p in sorted(SHIPPED_SCENARIOS_DIR.glob("*.yaml"))]


def load_shipped_scenario(name: str) -> Scenario | None:
    """Load a single shipped scenario by name, or return None if missing."""
    path = SHIPPED_SCENARIOS_DIR / f"{name}.yaml"
    if not path.is_file():
        return None
    return load_scenario(path)
