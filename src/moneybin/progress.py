"""Neutral operation progress events emitted by services."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    """A truthful operation boundary, optionally with a known count."""

    stage: str
    completed: int | None = None
    total: int | None = None
