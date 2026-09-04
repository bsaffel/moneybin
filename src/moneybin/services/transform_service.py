"""Transform service: SQLMesh interaction layer.

Owns SQLMesh Context lifecycle for state-reading and apply operations.
freshness() deliberately bypasses Context to keep the system_status hot
path cheap and side-effect free.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Collection
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, cast

import duckdb

from moneybin import sqlmesh_registry
from moneybin.database import Database, sqlmesh_context
from moneybin.metrics.registry import SQLMESH_RUN_DURATION_SECONDS
from moneybin.seeds import refresh_views
from moneybin.services.matching_service import MatchingService
from moneybin.tables import DIM_ACCOUNTS, IMPORT_LOG, MODEL_FRESHNESS

logger = logging.getLogger(__name__)

# Every raw table a SQLMesh model reads, mapped to the column recording when
# its rows landed in *this* database. Deliberately landing time, not
# ``extracted_at``: OFX stamps ``extracted_at`` when parsing begins and Plaid
# carries the provider's own sync time, both of which can predate an apply
# that ran while the rows were still in flight — those rows would then read as
# already-transformed forever. Landing time is written inside the write
# transaction, which the single-writer lock orders strictly against an apply.
#
# `test_pending_scan_set_covers_every_raw_table_the_transforms_read` asserts
# this key set equals `raw_tables_read_by_models()`, so a new raw table wired
# into a model fails loudly here rather than going silently unwatched.
_RAW_LANDING_COLUMNS: dict[str, str] = {
    "ofx_accounts": "loaded_at",
    "ofx_balances": "loaded_at",
    "ofx_institutions": "loaded_at",
    "ofx_transactions": "loaded_at",
    "plaid_accounts": "loaded_at",
    "plaid_balances": "loaded_at",
    "plaid_investment_holding_lots": "loaded_at",
    "plaid_investment_holdings": "loaded_at",
    "plaid_investment_holdings_snapshots": "loaded_at",
    "plaid_investment_transactions": "loaded_at",
    "plaid_securities": "loaded_at",
    "plaid_transactions": "loaded_at",
    "security_prices": "loaded_at",
    "tabular_accounts": "loaded_at",
    "tabular_transactions": "loaded_at",
    # The two manual-entry tables record their insert as `created_at` and have
    # no `loaded_at` — same instant, different name.
    "manual_investment_transactions": "created_at",
    "manual_transactions": "created_at",
}

# Those of the above whose rows name the import batch that wrote them, so a
# reverted or failed batch can be filtered back out. Sync and price-feed
# tables open no `import_log` batch at all and are unconditionally counted.
_RAW_IMPORT_SCOPED: frozenset[str] = frozenset({
    "manual_investment_transactions",
    "manual_transactions",
    "ofx_accounts",
    "ofx_balances",
    "ofx_institutions",
    "ofx_transactions",
    "tabular_accounts",
    "tabular_transactions",
})


class _PlannedSnapshot(Protocol):
    name: str
    is_full: bool


class _SnapshotInterval(Protocol):
    snapshot_id: object


class _InitialPlan(Protocol):
    snapshots: dict[object, _PlannedSnapshot]
    missing_intervals: Collection[_SnapshotInterval]


def _plan_backfills_full_models(
    plan: _InitialPlan, full_models: Collection[str]
) -> bool:
    """Return whether the initial SQLMesh plan schedules every FULL model."""
    planned_full_models = {
        plan.snapshots[interval.snapshot_id].name
        for interval in plan.missing_intervals
        if plan.snapshots[interval.snapshot_id].is_full
    }
    return set(full_models) <= planned_full_models


def _build_raw_landing_scan(
    tables: Collection[str], *, filter_bad_imports: bool = True
) -> str:
    """Compose the MAX-over-landing-times query from the declarations above.

    The full-set form is emitted once at import so the hot path runs a fixed
    string; ``_max_raw_landed_at`` rebuilds a narrowed one only when a table
    is missing. Each arm projects the table's landing column and its import
    batch (NULL where the table has none) so one status filter covers every
    source.
    """
    arms: list[str] = []
    for table in sorted(tables):
        column = _RAW_LANDING_COLUMNS[table]
        batch = (
            "import_id" if table in _RAW_IMPORT_SCOPED else "NULL::VARCHAR AS import_id"
        )
        arms.append(f'SELECT "{column}" AS landed_at, {batch} FROM raw."{table}"')
    union = "\n    UNION ALL\n    ".join(arms)
    # The ::TIMESTAMPTZ cast resolves each naive landing stamp in the *reading*
    # session's zone, which is the writing zone only while the profile stays on
    # one machine. Carry the database across zones and these stamps skew against
    # the model execution stamp — an absolute epoch out of `_intervals` — by the
    # offset delta, which can strand `pending` true or briefly mask real
    # staleness until the next apply re-anchors it. A naive value cannot yield
    # its instant at read time, so the fix is storing the landing columns as
    # TIMESTAMPTZ, not a different cast.
    if not filter_bad_imports:
        return f"""
            SELECT MAX(landed_at)::TIMESTAMPTZ FROM (
            {union}
            ) AS candidates
        """  # noqa: S608  # identifiers come from the module constants above
    return f"""
        WITH bad_imports AS (
            SELECT import_id FROM {IMPORT_LOG.full_name}
            WHERE status IN ('reverted', 'failed')
        ), candidates AS (
            {union}
        )
        SELECT MAX(landed_at)::TIMESTAMPTZ FROM candidates
        WHERE import_id IS NULL
           OR import_id NOT IN (SELECT import_id FROM bad_imports)
    """  # noqa: S608  # identifiers come from the module constants above


_RAW_LANDING_SCAN = _build_raw_landing_scan(_RAW_LANDING_COLUMNS)

# SQLMesh calls a model *symbolic* when it never executes it, so it never
# records an interval for one and `last_executed_at` stays NULL forever.
# `_oldest_model_execution_at` must skip them or its never-backfilled check
# trips on every read: every table `external_models.yaml` declares is EXTERNAL,
# and that file covers the `raw.*` sources the transforms read.
#
# `test_symbolic_model_kinds_match_sqlmesh` asserts this equals the set SQLMesh
# itself calls symbolic, so a kind added upstream fails loudly here rather than
# silently pinning `pending` true for the life of the profile.
_SYMBOLIC_MODEL_KINDS: frozenset[str] = frozenset({"EMBEDDED", "EXTERNAL"})

# Kinds a refresh does not re-execute, on top of the symbolic ones. Their
# interval stamp freezes at the first build, so leaving them in the minimum
# below pins `pending` true from the next import onward and *no* `refresh_run`
# can clear it — the mirror-image fail-closed of the bug this scan fixes.
#
# * VIEW — recomputed at query time, so a view is never stale by construction.
#   SQLMesh records one interval when it creates the view and none afterwards:
#   `apply()` restates only `kind.is_full`, and `run()` finds no missing
#   interval for a view whose interval is already complete.
# * SEED — rebuilt only when the seed file changes, and seeds read no `raw.*`
#   table, so a raw landing can never make one stale.
#
# What remains is exactly the set a refresh rebuilds: FULL today, plus the
# INCREMENTAL_* kinds `apply()`'s `run()` step already covers.
_UNREBUILT_MODEL_KINDS: frozenset[str] = _SYMBOLIC_MODEL_KINDS | {"SEED", "VIEW"}


def _oldest_execution_scan(model_count: int) -> str:
    """Scan for the least-recently-rebuilt model, over a bound name list.

    Scoped to the models the project still declares, not every name SQLMesh
    state retains. Renaming or removing a model leaves its ``_snapshots`` and
    ``_intervals`` rows behind until the janitor's TTL expires them, so the
    view keeps returning a row that no apply can ever rebuild — ``apply()``
    only runs what the project still defines. Left in the minimum, that frozen
    stamp pins ``pending`` true from the next raw landing onward with nothing a
    user or agent can do to clear it.

    A NULL kind stays in scope deliberately: an unrecognized model should hold
    the minimum down, not slip past it.
    """
    kinds = ", ".join(f"'{kind}'" for kind in sorted(_UNREBUILT_MODEL_KINDS))
    names = ", ".join("?" for _ in range(model_count))
    return f"""
        SELECT
            (MIN(last_executed_at) AT TIME ZONE 'UTC'),
            COUNT(*) FILTER (WHERE last_executed_at IS NULL)
        FROM {MODEL_FRESHNESS.full_name}
        WHERE COALESCE(model_kind, '') NOT IN ({kinds})
          AND LOWER(model_name) IN ({names})
    """  # noqa: S608  # kinds are a module constant; names are `?` placeholders


@dataclass(frozen=True)
class TransformFreshness:
    """Snapshot of transform freshness vs. raw imports.

    ``missing_models`` names registered SQLMesh models with no relation in the
    catalog. They count as pending — a model that was never built is the most
    stale a model can be — and they are listed rather than folded into the
    boolean so the caller can say *what* a refresh would fix.
    """

    pending: bool
    last_apply_at: datetime | None
    latest_import_at: datetime | None
    missing_models: tuple[str, ...] = ()


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

        Soft-fails on SQLMesh errors: returns ``ApplyResult(applied=False,
        error=<type name>)`` instead of raising so MCP/CLI callers see a
        structured error envelope. The exception type name is surfaced (not
        ``str(e)``) because SQLMesh error messages can embed file paths and
        SQL fragments containing user data.
        """
        logger.info("Running transforms")

        t0 = time.monotonic()
        try:
            try:
                # Seed first so int_transactions__merged can resolve per-field
                # winners. Inside the try block so a seed failure (write error,
                # stale catalog) returns the structured ApplyResult envelope
                # instead of propagating raw to MCP/CLI callers.
                MatchingService(self._db).seed_priority()
                with sqlmesh_context(self._db) as ctx:
                    # raw.* tables are loaded by Python OUTSIDE SQLMesh, so SQLMesh
                    # gets no signal that data changed. Three steps cover every
                    # model kind after an import/pull:
                    #   1. plan()  — absorb model-DEFINITION changes (no-op when
                    #      the SQL is unchanged) and bootstrap a fresh env.
                    #   2. run()   — process new intervals for INCREMENTAL models.
                    #      ignore_cron so a same-day re-pull isn't gated by the
                    #      per-model cron cadence. (No incremental models exist
                    #      today, so this is currently a no-op — wired now so the
                    #      pipeline is correct when one lands.)
                    #   3. restate FULL models only when step 1 did not schedule
                    #      every one. A plan may change only a VIEW or environment
                    #      metadata, which does not rebuild FULL models; otherwise
                    #      the restatement forces the rebuild that picks up new raw
                    #      rows. Scoped to is_full so INCREMENTAL models are NOT
                    #      blanket-restated (that would reprocess all history and
                    #      defeat incrementality).
                    # DEPRECATED-WHEN: the first INCREMENTAL model lands — verify
                    # step 2 keeps it fresh and that it stays out of the restate
                    # set below.
                    initial_plan = ctx.plan(no_prompts=True)
                    full_models = [
                        name for name, model in ctx.models.items() if model.kind.is_full
                    ]
                    initial_plan_backfills_full_models = _plan_backfills_full_models(
                        cast(_InitialPlan, initial_plan), full_models
                    )
                    ctx.apply(initial_plan)
                    # ctx.run() returns a CompletionStatus; unlike plan() it does
                    # NOT raise on scheduler/audit/model failure (SQLMesh's own
                    # CLI checks is_failure and raises "Run failed"). Surface a
                    # failed data-processing pass so the except below converts it
                    # to ApplyResult(applied=False) instead of silently proceeding
                    # to the restate and reporting success.
                    run_status = ctx.run(ignore_cron=True)
                    if run_status.is_failure:
                        raise RuntimeError("SQLMesh run reported a failure status")
                    if full_models and not initial_plan_backfills_full_models:
                        ctx.plan(
                            restate_models=full_models,
                            auto_apply=True,
                            no_prompts=True,
                        )
                # Full plan rebuilds seeds.* too, so refresh views that read them.
                refresh_views(self._db)
            except Exception as e:  # noqa: BLE001 — surface SQLMesh failure as structured result
                elapsed = time.monotonic() - t0
                error_type = type(e).__name__
                # Envelope carries only the type name (str(e) can embed file
                # paths / SQL fragments and may cross to a cloud LLM). The local
                # log carries the full message + traceback so the failure is
                # diagnosable via `moneybin logs` — SanitizedLogFormatter masks
                # PII patterns there. Coherent with the match/categorize steps
                # in refresh.py, which already log {exc} + exc_info.
                logger.warning(
                    f"Transforms failed after {elapsed:.2f}s: {error_type}: {e}",
                    exc_info=True,
                )
                return ApplyResult(
                    applied=False, duration_seconds=elapsed, error=error_type
                )

            # Durable boundary: a crash after a full transform apply must not
            # lose the rebuilt core/app state. CHECKPOINT runs after SQLMesh's
            # own context tears down (outside the with-block above) so DuckDB
            # sees no in-flight statements. Per docs/specs/database-writer-
            # coordination.md § "PR B hardening pass".
            #
            # Deliberately OUTSIDE the SQLMesh try/except: the transforms have
            # already committed, so a CHECKPOINT failure (a durability hint, not
            # correctness) must NOT flip the result to applied=False and make the
            # caller think the apply failed and re-run it. Log and continue.
            try:
                self._db.checkpoint("post_transform")
            except Exception as e:  # noqa: BLE001 — checkpoint is best-effort durability, not correctness
                logger.warning(
                    f"post_transform checkpoint failed (transforms applied): "
                    f"{type(e).__name__}"
                )

            elapsed = time.monotonic() - t0
            logger.info(f"Transforms completed in {elapsed:.2f}s")
            return ApplyResult(applied=True, duration_seconds=elapsed)
        finally:
            # Guarded because this is a `finally`: an unguarded raise here does
            # not just lose a timing sample, it replaces the return above, so a
            # committed transform reports as a crash. Callers act on that —
            # `rematch_after_merge` has already zeroed its retirement counter by
            # then and re-attaches it only on the success path, so the reversal
            # of a transfer the user accepted would go undisclosed.
            try:
                SQLMESH_RUN_DURATION_SECONDS.labels(model="transform_apply").observe(
                    time.monotonic() - t0
                )
            except Exception as exc:  # noqa: BLE001  # telemetry must not undo a committed apply
                # Type, not message: a metrics-client failure can name the
                # profile path, and SanitizedLogFormatter masks known PII
                # patterns, not arbitrary paths.
                logger.warning(
                    f"Could not record the transform duration: {type(exc).__name__}"
                )

    def freshness(self) -> TransformFreshness:
        """Return warehouse staleness without initializing SQLMesh.

        Pending when either is true:

        * a registered SQLMesh model has no relation in the catalog — it was
          never built, which no timestamp comparison can detect because an
          unbuilt model has no timestamp; or
        * a raw row landed after the last SQLMesh apply finished.

        The second check scans every raw table a model reads
        (:data:`_RAW_LANDING_COLUMNS`) against SQLMesh's own record of when it
        last rebuilt each model, read with a plain SELECT because a ``Context``
        costs seconds. The apply side is the *oldest* model's execution time
        rather than the newest promotion of ``prod``, so a selective plan
        cannot clear the flag for models it never rebuilt — see
        :meth:`_oldest_model_execution_at`.

        One known limit remains: the landing stamps are naive and resolve
        through the *reading* session's zone, while the execution stamp is an
        absolute epoch, so the comparison holds only while the profile is read
        in the zone that wrote it. Carrying a profile across zones can skew it
        by the offset delta until the next apply re-anchors both sides.

        ``last_apply_at`` (``dim_accounts.updated_at``) and
        ``latest_import_at`` (``import_log.completed_at``) remain
        wall-clock values for display only; they do not drive the
        pending decision.
        """
        presence = sqlmesh_registry.model_presence(self._db)
        # A warehouse nobody has built yet is not stale — it is what a profile
        # looks like between `db init` and its first refresh. Reporting that as
        # pending would make the flag permanently true on a healthy first run.
        missing_models = () if presence.never_built else presence.missing
        landed_at = self._max_raw_landed_at()
        applied_at = self._oldest_model_execution_at()

        if landed_at is None:
            raw_ahead = False
        elif applied_at is None:
            raw_ahead = True
        else:
            raw_ahead = landed_at > applied_at

        return TransformFreshness(
            pending=bool(missing_models) or raw_ahead,
            last_apply_at=self._max_dim_accounts_updated_at(),
            latest_import_at=self._max_completed_import_at(),
            missing_models=missing_models,
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
            # type(e).__name__ instead of str(e): SQLMesh error messages can
            # embed file paths and SQL fragments containing user data.
            errors.append({"model": "<unknown>", "message": type(e).__name__})
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
            # type(e).__name__ instead of str(e): SQLMesh error messages can
            # embed file paths and SQL fragments containing user data.
            return AuditResult(
                passed=0,
                failed=1,
                audits=[
                    {
                        "name": "<audit invocation>",
                        "status": "failed",
                        "detail": type(e).__name__,
                    }
                ],
            )
        return AuditResult(passed=passed, failed=failed, audits=audits)

    def _max_completed_import_at(self) -> datetime | None:
        # 'complete' and 'partial' are exactly the statuses that set
        # completed_at, so the filter now matches the aggregate. The previous
        # `NOT IN ('reverted', 'failed')` also admitted in-flight 'importing'
        # batches to make them count as unseen data — but their completed_at
        # is NULL until finalize, so MAX skipped every one of them, and this
        # field drives no freshness decision anyway (see `freshness`).
        # Deliberately broader than SystemService._last_import_at, which shows
        # only fully-complete imports: a 'partial' batch did land rows.
        try:
            row = self._db.execute(
                f"SELECT MAX(completed_at)::TIMESTAMP FROM {IMPORT_LOG.full_name} "
                f"WHERE status IN ('complete', 'partial')"  # noqa: S608  # TableRef constant
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

    def _max_raw_landed_at(self) -> datetime | None:
        """Latest landing time across every raw table the transforms read.

        Excludes rows belonging to a reverted or failed import batch — a
        revert deletes its raw rows, but a failed ingest can leave rows
        behind, and those must not pin ``pending`` true forever. Tables that
        open no ``import_log`` batch (sync, price feeds) always pass.
        """
        try:
            row = self._db.execute(_RAW_LANDING_SCAN).fetchone()
        except duckdb.CatalogException:
            # One absent table must not blind the scan to the other sixteen.
            # Read-only opens never run `init_schemas`, so a raw table added
            # by a newer release is genuinely missing until the next write —
            # and returning None there reports "nothing pending" for every
            # source, the exact fail-open this scan exists to close.
            present, has_import_log = self._present_raw_landing_tables()
            if not present:
                return None
            row = self._db.execute(
                _build_raw_landing_scan(present, filter_bad_imports=has_import_log)
            ).fetchone()
        return row[0] if row and row[0] is not None else None

    def _present_raw_landing_tables(self) -> tuple[frozenset[str], bool]:
        """Declared landing tables this catalog actually has, plus import_log."""
        rows = self._db.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_catalog = current_database() AND table_schema = 'raw'"
        ).fetchall()
        names = {str(row[0]) for row in rows}
        return frozenset(names & set(_RAW_LANDING_COLUMNS)), "import_log" in names

    def _oldest_model_execution_at(self) -> datetime | None:
        """When the least-recently-rebuilt SQLMesh model was last backfilled.

        The *oldest* model, not the newest promotion. SQLMesh finalizes the
        environment on every promotion of ``prod``, so a selective plan —
        ``moneybin transform restate --model``, or a seed-only plan that has
        work to do — advances ``_environments.finalized_ts`` without rebuilding
        anything else. Keyed on that stamp, one restatement of an unrelated
        model reports the entire warehouse fresh while every untouched model
        still holds pre-import data: a fail-open in the signal whose whole job
        is catching staleness.

        ``meta.model_freshness.last_executed_at`` comes from ``_intervals``,
        which moves only when a model is genuinely backfilled, so an untouched
        model keeps its true age and holds the minimum down.

        Scoped to the kinds a refresh actually rebuilds — see
        :data:`_UNREBUILT_MODEL_KINDS` — and to the models the project still
        declares, see :func:`_oldest_execution_scan`. Both exclusions guard the
        same fail-closed: a stamp that no refresh can advance, left in the
        minimum, pins ``pending`` true from the next import onward with no way
        to clear it.

        Returns ``None`` — which reads as pending — when the view is absent (no
        apply yet), unreadable, or when any rebuildable model has never been
        backfilled. Every degraded path fails closed: ``freshness()`` has no
        catch of its own and sits on the ``system_status`` path.
        """
        registered = sorted(sqlmesh_registry.registered_model_names())
        if not registered:
            # No declared models means the registry could not read the project
            # at all. Reporting "nothing to rebuild" there is the fail-open
            # this whole scan exists to close.
            return None
        try:
            row = self._db.execute(
                _oldest_execution_scan(len(registered)), registered
            ).fetchone()
        except duckdb.Error:
            logger.debug("model-execution stamp read failed", exc_info=True)
            return None
        if row is None or row[0] is None or row[1]:
            return None
        return row[0]
