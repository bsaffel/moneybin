"""Lightweight result type returned by ``run_scenario``.

Replaces the bespoke ``ResponseEnvelope`` previously used by the
``moneybin synthetic verify`` CLI. Scenario tests assert on this
dataclass directly; pytest-json-report captures pass/fail in CI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    """Outcome of running a single scenario end-to-end."""

    scenario: str
    passed: bool
    duration_seconds: float
    halted: str | None = None
    tmpdir: str | None = None
    assertions: list[dict[str, Any]] = field(default_factory=list)
    expectations: list[dict[str, Any]] = field(default_factory=list)
    evaluations: list[dict[str, Any]] = field(default_factory=list)

    def failure_summary(self) -> str:
        """Render a multi-line description of every failing check.

        Used as the ``assert`` failure message so pytest output points
        the reader directly at which assertion/expectation/evaluation
        broke and why, without dumping PII-bearing details.
        """
        lines: list[str] = [f"scenario {self.scenario!r} failed"]
        if self.halted:
            lines.append(f"  halted: {self.halted}")
        for a in self.assertions:
            if not a["passed"]:
                lines.append(f"  assertion {a['name']}: {a.get('error') or 'failed'}")
        for e in self.expectations:
            if not e["passed"]:
                lines.append(f"  expectation {e['name']}")
        for v in self.evaluations:
            if not v["passed"]:
                lines.append(
                    f"  evaluation {v['name']}: "
                    f"{v['metric']}={v['value']} < threshold={v['threshold']}"
                )
        return "\n".join(lines)
