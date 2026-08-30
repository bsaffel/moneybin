"""Shared helpers for repository tests that assert audit and metric effects."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from prometheus_client import REGISTRY

from moneybin.database import Database


def audit_rows_for(db: Database, target_id: str) -> list[tuple[Any, ...]]:
    """Return ordered audit rows for one target."""
    return db.conn.execute(
        """
        SELECT action, target_schema, target_table, target_id,
               before_value, after_value, actor, parent_audit_id
          FROM app.audit_log
         WHERE target_id = ?
         ORDER BY occurred_at ASC, audit_id ASC
        """,
        [target_id],
    ).fetchall()


def metric(repository: str, action: str) -> float:
    """Return one repository mutation metric value."""
    return (
        REGISTRY.get_sample_value(
            "moneybin_app_mutation_audit_emitted_total",
            {"repository": repository, "action": action},
        )
        or 0.0
    )


def metric_for(repository: str) -> Callable[[str], float]:
    """Bind the shared metric reader to one repository label."""
    return lambda action: metric(repository, action)
