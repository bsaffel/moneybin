"""Pydantic-backed scenario YAML loader."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

# Lazy import to avoid circular dependency with steps.py.
# Kept in sync with ``STEP_REGISTRY`` via ``test_step_names_match_registry``.
VALID_STEP_NAMES = {
    "generate",
    "load_fixtures",
    "import_file",
    "transform",
    "match",
    "seed_merchants",
    "categorize",
    "migrate",
    "transform_via_subprocess",
}

# Anchor at <repo>/tests/scenarios/data — scenario YAML and fixtures live
# under tests/, not the installed package.
_DATA_ROOT = Path(__file__).resolve().parent.parent / "data"
REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES_ROOT = (_DATA_ROOT / "fixtures").resolve()
# Real-format fixtures (OFX/QFX/QBO/CSV files imported through the actual
# ImportService pipeline) live under tests/fixtures/ — separate from the
# CSV-shaped raw fixtures that load_fixtures uses.
IMPORT_FIXTURES_ROOT = (REPO_ROOT / "tests" / "fixtures").resolve()


class ScenarioValidationError(ValueError):
    """Raised when a scenario YAML fails Pydantic validation."""


class FixtureSpec(BaseModel):
    """A fixture file referenced by a scenario's setup block."""

    model_config = ConfigDict(extra="forbid")
    path: str
    account: str
    # Only csv/ofx are implemented in fixture_loader; pdf will be added when
    # a loader exists. Reject at validation rather than crashing mid-run.
    source_type: Literal["csv", "ofx"]

    @model_validator(mode="after")
    def _validate_path(self) -> FixtureSpec:
        resolved = (FIXTURES_ROOT / self.path).resolve()
        try:
            resolved.relative_to(FIXTURES_ROOT)
        except ValueError as exc:
            raise ValueError(
                f"fixture path {self.path!r} must resolve under "
                f"{FIXTURES_ROOT.relative_to(REPO_ROOT)}"
            ) from exc
        return self


class ImportFileSpec(BaseModel):
    """A real-format file to import via ``ImportService.import_file``.

    Resolved relative to ``IMPORT_FIXTURES_ROOT`` (``tests/fixtures/``). The
    ``import_file`` pipeline step iterates ``setup.imports`` in order, so
    re-import scenarios can declare the same path twice with different
    expectations.
    """

    model_config = ConfigDict(extra="forbid")
    path: str
    account_name: str | None = None
    institution: str | None = None
    force: bool = False
    apply_transforms: bool = False
    expect_failure: bool = False
    expect_error_substring: str | None = None

    @model_validator(mode="after")
    def _validate_path(self) -> ImportFileSpec:
        resolved = (IMPORT_FIXTURES_ROOT / self.path).resolve()
        try:
            resolved.relative_to(IMPORT_FIXTURES_ROOT)
        except ValueError as exc:
            raise ValueError(
                f"import path {self.path!r} must resolve under "
                f"{IMPORT_FIXTURES_ROOT.relative_to(REPO_ROOT)}"
            ) from exc
        return self

    @model_validator(mode="after")
    def _check_failure_spec(self) -> ImportFileSpec:
        # The substring check only runs inside the except branch gated on
        # expect_failure — without this guard, a typo'd scenario would
        # silently skip the substring assertion and look like it passed.
        if self.expect_error_substring and not self.expect_failure:
            raise ValueError("expect_error_substring requires expect_failure: true")
        return self


class SetupSpec(BaseModel):
    """Scenario setup: persona, seed, year span, and fixtures."""

    model_config = ConfigDict(extra="forbid")
    persona: str
    seed: int = 42
    years: int = 1
    fixtures: list[FixtureSpec] = Field(default_factory=list)
    imports: list[ImportFileSpec] = Field(default_factory=list)


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
        "transfers_match_ground_truth",
    ]
    description: str = ""


class GatesSpec(BaseModel):
    """Selectors controlling which checks gate scenario success.

    Currently only the ``"all"`` selector is supported — every assertion,
    evaluation, and expectation must pass. A subset selector (``list[str]``)
    can be added when there's a real need for partial gating; until then
    the schema stays narrow so authors can't write something the runner
    silently ignores.
    """

    model_config = ConfigDict(extra="forbid")
    required_assertions: Literal["all"] = "all"
    required_evaluations: Literal["all"] = "all"
    required_expectations: Literal["all"] = "all"


class Scenario(BaseModel):
    """A complete validated scenario specification."""

    model_config = ConfigDict(extra="forbid")
    # Restrict name to path-safe chars so it can be embedded in tempdir paths
    # (runner.py uses scenario.name as a mkdtemp prefix).
    name: str = Field(alias="scenario", pattern=r"^[a-z0-9][a-z0-9\-]*$")
    description: str = ""
    setup: SetupSpec
    pipeline: list[str]
    assertions: list[AssertionSpec] = Field(default_factory=list)
    evaluations: list[EvaluationSpec] = Field(default_factory=list)
    expectations: list[ExpectationSpec] = Field(default_factory=list)
    gates: GatesSpec = Field(default_factory=GatesSpec)

    @model_validator(mode="after")
    def _validate_steps(self) -> Scenario:
        unknown = [s for s in self.pipeline if s not in VALID_STEP_NAMES]
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


SHIPPED_SCENARIOS_DIR = _DATA_ROOT


def list_shipped_scenarios() -> list[Scenario]:
    """Load all shipped scenario YAMLs bundled with the package."""
    return [load_scenario(p) for p in sorted(SHIPPED_SCENARIOS_DIR.glob("*.yaml"))]


def load_shipped_scenario(name: str) -> Scenario | None:
    """Load a single shipped scenario by name, or return None if missing."""
    path = SHIPPED_SCENARIOS_DIR / f"{name}.yaml"
    if not path.is_file():
        return None
    return load_scenario(path)
