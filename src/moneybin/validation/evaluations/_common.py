"""Shared helpers and exceptions for evaluation modules."""

from __future__ import annotations

from moneybin.database import Database


class GroundTruthMissingError(RuntimeError):
    """Raised when an evaluation runs against a DB without `synthetic.ground_truth`."""


def has_ground_truth(db: Database) -> bool:
    rows = db.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'synthetic' AND table_name = 'ground_truth'
        """
    ).fetchall()
    return bool(rows)
