"""Typed inputs shared across expectation predicates."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class SourceTransactionRef:
    """Reference to a single source-system transaction by (id, source_type)."""

    source_transaction_id: str
    # Mirrors loader.FixtureSpec.source_type — extend together when a new
    # source type is added.
    source_type: Literal["csv", "ofx", "pdf"]
