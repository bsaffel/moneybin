"""Transform service: SQLMesh interaction layer.

Owns SQLMesh Context lifecycle for state-reading and apply operations.
freshness() deliberately bypasses Context to keep the system_status hot
path cheap and side-effect free.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import duckdb

from moneybin.config import get_settings
from moneybin.database import Database, sqlmesh_context
from moneybin.matching.priority import seed_source_priority
from moneybin.metrics.registry import SQLMESH_RUN_DURATION_SECONDS
from moneybin.seeds import refresh_views
from moneybin.tables import DIM_ACCOUNTS, IMPORT_LOG

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TransformFreshness:
    """Snapshot of transform freshness vs. raw imports."""

    pending: bool
    last_apply_at: datetime | None
    latest_import_at: datetime | None


@dataclass(frozen=True)
class TransformStatus:
    """Snapshot of SQLMesh environment + freshness."""

    environment: str
    initialized: bool
    last_apply_at: datetime | None
    pending: bool
    latest_import_at: datetime | None


@dataclass(frozen=True)
class ApplyResult:
    """Outcome of a transform apply."""

    applied: bool
    duration_seconds: float
    error: str | None = None


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

    def apply(self) -> ApplyResult:
        """Apply pending SQLMesh changes.

        Seeds ``app.seed_source_priority`` before running so
        ``int_transactions__merged`` can resolve per-field winners. Without
        this, the LEFT JOIN onto ``seed_source_priority`` produces NULL
        priorities for every row, causing ARG_MIN(value, NULL_key) to drop
        non-NULL values for fields that key on a CASE-with-NULL-fallthrough
        pattern (description, memo, etc.). Callers that go straight to
        transforms would otherwise materialize NULL descriptions in
        core.fct_transactions.
        """
        logger.info("Running SQLMesh transforms")
        seed_source_priority(self._db, get_settings().matching)

        t0 = time.monotonic()
        try:
            with sqlmesh_context(self._db) as ctx:
                ctx.plan(auto_apply=True, no_prompts=True)
            # Full plan rebuilds seeds.* too, so refresh the views that read them.
            refresh_views(self._db)
            elapsed = time.monotonic() - t0
            logger.info(f"SQLMesh transforms completed in {elapsed:.2f}s")
            return ApplyResult(applied=True, duration_seconds=elapsed)
        finally:
            SQLMESH_RUN_DURATION_SECONDS.labels(model="transform_apply").observe(
                time.monotonic() - t0
            )

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

    def status(self) -> TransformStatus:
        """Current SQLMesh environment state plus freshness signal.

        Reads SQLMesh env via Context (multi-second on first init; acceptable
        for the explicit transform_status tool but not for system_status).
        """
        freshness = self.freshness()
        initialized = False
        env_apply_at: datetime | None = None

        try:
            with sqlmesh_context(self._db) as ctx:
                env = ctx.state_reader.get_environment("prod")
                if env is not None:
                    initialized = True
                    if env.finalized_ts is not None:
                        env_apply_at = datetime.fromtimestamp(
                            env.finalized_ts / 1000, tz=UTC
                        ).replace(tzinfo=None)
        except Exception:  # noqa: BLE001 — SQLMesh may fail to init on a fresh DB
            logger.debug("SQLMesh status read failed", exc_info=True)

        # Prefer SQLMesh's finalized_ts when present (authoritative for the
        # plan); fall back to freshness's dim_accounts-derived signal otherwise.
        last_apply_at = (
            env_apply_at if env_apply_at is not None else freshness.last_apply_at
        )

        return TransformStatus(
            environment="prod",
            initialized=initialized,
            last_apply_at=last_apply_at,
            pending=freshness.pending,
            latest_import_at=freshness.latest_import_at,
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
