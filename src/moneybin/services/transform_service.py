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


@dataclass(frozen=True)
class TransformPlan:
    """Preview of pending SQLMesh changes."""

    has_changes: bool
    directly_modified: list[str]
    indirectly_modified: list[str]
    added: list[str]
    removed: list[str]


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of a parse/resolve check across all models."""

    valid: bool
    errors: list[dict[str, str]]


@dataclass(frozen=True)
class AuditResult:
    """Outcome of running SQLMesh audits over a date window."""

    passed: int
    failed: int
    audits: list[dict[str, str | None]]


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

    def plan(self) -> TransformPlan:
        """Preview pending SQLMesh changes without applying.

        Reads ``Plan.directly_modified`` (Set[SnapshotId]),
        ``Plan.indirectly_modified`` (Dict[SnapshotId, Set[SnapshotId]]),
        ``Plan.new_snapshots`` (List[Snapshot]), and removed snapshots from
        ``context_diff.removed_snapshots`` (Dict[SnapshotId, ...]). All keys
        carry a ``.name`` attribute.
        """
        with sqlmesh_context(self._db) as ctx:
            sqlmesh_plan = ctx.plan_builder().build()
            directly = sorted(s.name for s in sqlmesh_plan.directly_modified)
            indirectly = sorted({
                s.name
                for s_set in sqlmesh_plan.indirectly_modified.values()
                for s in s_set
            })
            added = sorted(s.name for s in sqlmesh_plan.new_snapshots)
            removed = sorted(
                s.name for s in sqlmesh_plan.context_diff.removed_snapshots
            )
        has_changes = bool(directly or indirectly or added or removed)
        return TransformPlan(
            has_changes=has_changes,
            directly_modified=directly,
            indirectly_modified=indirectly,
            added=added,
            removed=removed,
        )

    def validate(self) -> ValidationResult:
        """Parse + resolve all models. Reports errors without applying."""
        errors: list[dict[str, str]] = []
        try:
            with sqlmesh_context(self._db) as ctx:
                ctx.plan_builder().build()
        except Exception as e:  # noqa: BLE001 — SQLMesh raises a variety of parse/resolve errors
            errors.append({"model": "<unknown>", "message": str(e)})
        return ValidationResult(valid=not errors, errors=errors)

    def audit(self, start: str, end: str) -> AuditResult:
        """Run SQLMesh data-quality audits over [start, end] (YYYY-MM-DD).

        ``Context.audit()`` returns only a bool, so we iterate snapshots and
        call ``ctx.snapshot_evaluator.audit()`` per snapshot to recover per-
        audit detail (matches the pattern in
        ``sqlmesh.core.context.Context.audit``).
        """
        audits: list[dict[str, str | None]] = []
        passed = 0
        failed = 0
        try:
            with sqlmesh_context(self._db) as ctx:
                for snapshot in ctx.snapshots.values():
                    for audit_result in ctx.snapshot_evaluator.audit(
                        snapshot=snapshot,
                        start=start,
                        end=end,
                        snapshots=ctx.snapshots,
                    ):
                        name = audit_result.audit.name
                        if audit_result.skipped:
                            continue
                        if audit_result.count:
                            audits.append({
                                "name": name,
                                "status": "failed",
                                "detail": f"{audit_result.count} row(s) failed",
                            })
                            failed += 1
                        else:
                            audits.append({
                                "name": name,
                                "status": "passed",
                                "detail": None,
                            })
                            passed += 1
        except Exception as e:  # noqa: BLE001 — surface SQLMesh failure as one failed audit
            return AuditResult(
                passed=0,
                failed=1,
                audits=[
                    {
                        "name": "<audit invocation>",
                        "status": "failed",
                        "detail": str(e),
                    }
                ],
            )
        return AuditResult(passed=passed, failed=failed, audits=audits)

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
