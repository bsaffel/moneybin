"""Pipeline integrity checks — DoctorService."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InvariantResult:
    """Result of one pipeline invariant check."""

    name: str
    status: Literal["pass", "fail", "warn", "skipped"]
    detail: str | None
    affected_ids: list[str]


@dataclass(frozen=True)
class DoctorReport:
    """Aggregated result of all pipeline invariant checks."""

    invariants: list[InvariantResult]
    transaction_count: int
