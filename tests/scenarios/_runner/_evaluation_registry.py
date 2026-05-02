"""Explicit YAML-callable evaluation registry.

Parallel to ``_assertion_registry`` and ``_expectation_registry``: every entry
is a contract whose name is part of scenario YAML's surface area. Adding a
new YAML-callable evaluation requires explicitly registering it here, so an
internal helper exposed in ``moneybin.validation.evaluations.__all__`` can't
become callable from YAML by accident.
"""

from __future__ import annotations

from collections.abc import Callable

from moneybin.validation.evaluations import (
    score_categorization,
    score_dedup,
    score_transfer_detection,
)
from moneybin.validation.result import EvaluationResult

EvaluationFn = Callable[..., EvaluationResult]

EVALUATION_REGISTRY: dict[str, EvaluationFn] = {
    "score_categorization": score_categorization,
    "score_dedup": score_dedup,
    "score_transfer_detection": score_transfer_detection,
}


def resolve_evaluation(fn_name: str) -> EvaluationFn:
    """Return the callable registered under ``fn_name`` or raise KeyError."""
    if fn_name not in EVALUATION_REGISTRY:
        raise KeyError(f"unknown evaluation fn: {fn_name!r}")
    return EVALUATION_REGISTRY[fn_name]


__all__ = ["EVALUATION_REGISTRY", "EvaluationFn", "resolve_evaluation"]
