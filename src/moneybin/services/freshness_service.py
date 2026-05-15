"""Typed accessor for model-level freshness from meta.model_freshness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from moneybin.database import Database


@dataclass(slots=True)
class ModelFreshness:
    """Freshness facts for a single SQLMesh model.

    See docs/specs/core-updated-at-convention.md for column semantics.
    """

    model_name: str
    last_changed_at: datetime | None
    last_applied_at: datetime | None


def get_model_freshness(db: Database, model_name: str) -> ModelFreshness | None:
    """Return freshness for one model, or None if not yet applied.

    Reads from meta.model_freshness; returns None when the model has never
    been materialized (no row in sqlmesh._snapshots).
    """
    row = db.execute(
        "SELECT last_changed_at, last_applied_at "
        "FROM meta.model_freshness "
        "WHERE model_name = ?",
        [model_name],
    ).fetchone()
    if row is None:
        return None
    return ModelFreshness(
        model_name=model_name,
        last_changed_at=row[0],
        last_applied_at=row[1],
    )
