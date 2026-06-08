"""Confidence scorer for PDF extraction recipes (Req 2: threshold-based auto-derive).

A weighted blend of required-field completeness (0.7) and important-field
completeness (0.3). Scores >= _THRESHOLD are considered high-confidence and
eligible for automatic recipe derivation (Task 7); lower scores fall back to
seed routing (Phase 1 path).
"""

from __future__ import annotations

_REQUIRED_WEIGHT = 0.7
_IMPORTANT_WEIGHT = 0.3
_THRESHOLD = 0.7  # Req 2; spec default — module constant for Phase 2a


def score(
    *,
    required_filled: int,
    required_total: int,
    important_filled: int,
    important_total: int,
) -> float:
    """Return a confidence score in [0.0, 1.0] for an extraction result.

    When a total is zero the corresponding ratio defaults to 1.0 (vacuously
    satisfied — no fields of that class were declared).
    """
    req = required_filled / required_total if required_total else 1.0
    imp = important_filled / important_total if important_total else 1.0
    return _REQUIRED_WEIGHT * req + _IMPORTANT_WEIGHT * imp


def is_high_confidence(s: float) -> bool:
    """Return True if *s* meets or exceeds the auto-derive threshold."""
    return s >= _THRESHOLD
