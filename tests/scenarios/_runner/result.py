"""Lightweight result type returned by ``run_scenario``."""

from __future__ import annotations

from dataclasses import dataclass, field

from moneybin.validation.result import AssertionResult, EvaluationResult
from tests.scenarios._runner.expectations import ExpectationResult


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    """Outcome of running a single scenario end-to-end."""

    scenario: str
    duration_seconds: float
    halted: str | None = None
    tmpdir: str | None = None
    assertions: list[AssertionResult] = field(default_factory=list)
    expectations: list[ExpectationResult] = field(default_factory=list)
    evaluations: list[EvaluationResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        if self.halted is not None:
            return False
        return (
            all(a.passed for a in self.assertions)
            and all(e.passed for e in self.expectations)
            and all(v.passed for v in self.evaluations)
        )

    def failure_summary(self) -> str:
        """Render a multi-line description of every failing check."""
        lines: list[str] = [f"scenario {self.scenario!r} failed"]
        if self.halted:
            lines.append(f"  halted: {self.halted}")
        for a in self.assertions:
            if not a.passed:
                reason = a.error or (str(a.details) if a.details else "failed")
                lines.append(f"  assertion {a.name}: {reason}")
        for e in self.expectations:
            if not e.passed:
                lines.append(f"  expectation {e.name}")
        for v in self.evaluations:
            if not v.passed:
                lines.append(
                    f"  evaluation {v.name}: "
                    f"{v.metric}={v.value} < threshold={v.threshold}"
                )
        return "\n".join(lines)
