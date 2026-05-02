"""Structured result types returned by validation primitives."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class AssertionResult:
    """Outcome of a single named assertion check."""

    name: str
    passed: bool
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def raise_if_failed(self) -> None:
        """Raise AssertionError if the assertion did not pass."""
        if not self.passed:
            raise AssertionError(
                f"assertion {self.name!r} failed: details={self.details} error={self.error}"
            )


@dataclass(frozen=True, slots=True)
class EvaluationResult:
    """Outcome of a metric evaluation against a threshold."""

    name: str
    metric: str
    value: float
    threshold: float
    passed: bool
    breakdown: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExpectationResult:
    """Outcome of verifying a single per-record expectation against the database."""

    name: str
    kind: str
    passed: bool
    details: dict[str, Any] = field(default_factory=dict)
