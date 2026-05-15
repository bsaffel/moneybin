"""Transform service: SQLMesh interaction layer.

Owns SQLMesh Context lifecycle for state-reading and apply operations.
freshness() deliberately bypasses Context to keep the system_status hot
path cheap and side-effect free.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

import duckdb

from moneybin.database import Database
from moneybin.tables import DIM_ACCOUNTS, IMPORT_LOG

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TransformFreshness:
    """Snapshot of transform freshness vs. raw imports."""

    pending: bool
    last_apply_at: datetime | None
    latest_import_at: datetime | None


class TransformService:
    """SQLMesh interaction layer.

    All MCP/CLI transform_* tools and the system_status pending signal go
    through this service. freshness() is the only hot-path method; it must
    not init a SQLMesh Context (Context init has side effects and multi-
    second latency).
    """

    def __init__(self, db: Database) -> None:
        """Bind to an open Database connection."""
        self._db = db

    def freshness(self) -> TransformFreshness:
        """Return raw-vs-dim staleness without initializing SQLMesh.

        Pending iff the newest completed import is newer than the newest
        ``core.dim_accounts.updated_at``. When ``core.dim_accounts`` is
        missing (pre-first-transform), pending=True if any imports exist.

        Both timestamps are cast to naive ``TIMESTAMP`` in SQL before being
        returned to Python. ``raw.import_log.completed_at`` is already naive
        ``TIMESTAMP``, but ``core.dim_accounts.updated_at`` is materialized
        from ``CURRENT_TIMESTAMP`` by SQLMesh, which DuckDB types as
        ``TIMESTAMP WITH TIME ZONE``. Mixing tz-aware and naive datetimes in
        Python's ``>`` comparison raises ``TypeError`` — normalize in SQL so
        both sides of the comparison are the same type.
        """
        latest_import_at = self._max_completed_import_at()
        last_apply_at = self._max_dim_accounts_updated_at()

        if latest_import_at is None:
            return TransformFreshness(
                pending=False,
                last_apply_at=last_apply_at,
                latest_import_at=None,
            )
        if last_apply_at is None:
            return TransformFreshness(
                pending=True,
                last_apply_at=None,
                latest_import_at=latest_import_at,
            )
        return TransformFreshness(
            pending=latest_import_at > last_apply_at,
            last_apply_at=last_apply_at,
            latest_import_at=latest_import_at,
        )

    def _max_completed_import_at(self) -> datetime | None:
        try:
            row = self._db.execute(
                f"SELECT MAX(completed_at)::TIMESTAMP FROM {IMPORT_LOG.full_name} "
                f"WHERE status NOT IN ('reverted', 'failed')"  # noqa: S608  # TableRef constant
            ).fetchone()
        except duckdb.CatalogException:
            # CatalogException when raw.import_log not yet created (pre-first-import)
            return None
        return row[0] if row and row[0] is not None else None

    def _max_dim_accounts_updated_at(self) -> datetime | None:
        try:
            row = self._db.execute(
                f"SELECT MAX(updated_at)::TIMESTAMP FROM {DIM_ACCOUNTS.full_name}"  # noqa: S608  # TableRef constant
            ).fetchone()
        except duckdb.CatalogException:
            # CatalogException when core.dim_accounts not yet created (pre-first-transform)
            return None
        return row[0] if row and row[0] is not None else None
