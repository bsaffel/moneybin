"""Typed inputs shared across expectation predicates."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


class SourceTransactionRef(BaseModel):
    """Reference to a single source-system transaction by (id, source_type).

    Pydantic model with ``extra='forbid'`` so a misspelled key in scenario YAML
    (e.g. ``source_typ``) fails at construction with a clear message rather
    than silently passing through and surfacing later as an opaque SQL error.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_transaction_id: str
    # Mirrors loader.FixtureSpec.source_type — extend together when a new
    # source type is added.
    source_type: Literal["csv", "ofx"]
