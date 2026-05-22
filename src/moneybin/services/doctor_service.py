"""Pipeline integrity checks — DoctorService."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from sqlglot import exp

from moneybin.config import get_settings
from moneybin.database import Database, sqlmesh_context
from moneybin.errors import RecoveryAction
from moneybin.tables import (
    AUDIT_LOG,
    CATEGORY_OVERRIDES,
    FCT_TRANSACTIONS,
    GSHEET_CONNECTIONS,
    USER_CATEGORIES,
    TableRef,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InvariantResult:
    """Result of one pipeline invariant check.

    The optional ``recovery_actions`` field carries structured recovery
    hints when an audit fails or warns. PR 4 (doctor recipe registry)
    will populate this from per-audit recipes; for now it defaults to
    ``None`` so existing call sites are unaffected.
    """

    name: str
    status: Literal["pass", "fail", "warn", "skipped"]
    detail: str | None
    affected_ids: list[str]
    recovery_actions: list[RecoveryAction] | None = None


@dataclass(frozen=True)
class DoctorReport:
    """Aggregated result of all pipeline invariant checks."""

    invariants: list[InvariantResult]
    transaction_count: int

    @property
    def failing(self) -> int:
        """Count of invariants with status 'fail'."""
        return sum(1 for r in self.invariants if r.status == "fail")

    @property
    def warning(self) -> int:
        """Count of invariants with status 'warn'."""
        return sum(1 for r in self.invariants if r.status == "warn")

    @property
    def passing(self) -> int:
        """Count of invariants with status 'pass'."""
        return sum(1 for r in self.invariants if r.status == "pass")

    @property
    def skipped(self) -> int:
        """Count of invariants with status 'skipped'."""
        return sum(1 for r in self.invariants if r.status == "skipped")


class DoctorService:
    """Run pipeline integrity invariants and aggregate results."""

    def __init__(self, db: Database) -> None:
        """Store the open database connection for invariant queries."""
        self._db = db

    def run_all(self, verbose: bool = False, full: bool = False) -> DoctorReport:
        """Run all invariants and return a DoctorReport.

        ``full`` opts the protected-``app.*`` audit-coverage checks out of their
        sampled, lookback-windowed mode and into a full-table scan.
        """
        transaction_count = self._get_transaction_count()
        sqlmesh_results = self._run_sqlmesh_audits(verbose)
        staging = self._run_staging_coverage()
        categorization = self._run_categorization_coverage()
        app_integrity = self._run_app_integrity(full=full)
        invariants = [*sqlmesh_results, staging, categorization, *app_integrity]
        return DoctorReport(invariants=invariants, transaction_count=transaction_count)

    def _run_app_integrity(self, *, full: bool) -> list[InvariantResult]:
        """Per-table integrity checks for protected ``app.*`` tables (Invariant 10).

        Covers every table this PR wraps in a repo (``user_categories``,
        ``category_overrides``, ``gsheet_connections``); later repository PRs
        append one coverage call per newly-wrapped table plus that table's
        FK/orphan specifics.
        """
        return [
            self._run_app_audit_coverage(USER_CATEGORIES, "category_id", full=full),
            self._run_app_audit_coverage(CATEGORY_OVERRIDES, "category_id", full=full),
            self._run_app_audit_coverage(
                GSHEET_CONNECTIONS, "connection_id", full=full
            ),
            self._run_user_categories_uniqueness(),
        ]

    def _run_app_audit_coverage(
        self, table_ref: TableRef, pk_col: str, *, full: bool = False
    ) -> InvariantResult:
        """Flag rows mutated without a paired ``app.audit_log`` row (Req 9).

        Sampled by default: only rows whose ``updated_at`` falls within
        ``doctor.audit_coverage_lookback_days`` are checked, capped at
        ``doctor.audit_coverage_sample_cap``. ``full=True`` scans every row.
        Requires the table to carry an ``updated_at`` column (every protected
        table does).

        Limitations (by design — this is a sampled runtime *heuristic*, not a
        proof): it scans rows that currently exist and keys on ``updated_at`` as
        the mutation watermark, so a raw bypass that (a) deletes a row, or (b)
        mutates without bumping ``updated_at``, leaves nothing for the scan to
        flag. The *structural* guard against raw bypass writes is the lint rule
        (rejects raw ``INSERT``/``UPDATE``/``DELETE`` against protected tables
        outside ``*_repo.py``); content-based coverage is a hosted-tier follow-up
        (see ``private/followups.md``). ``pk_col`` is a code-supplied constant,
        quoted defensively per ``.claude/rules/security.md``.
        """
        name = f"app_audit_coverage_{table_ref.name}"
        settings = get_settings().doctor
        safe_pk = exp.to_identifier(pk_col, quoted=True).sql("duckdb")
        if full:
            sampled_sql = (
                f"SELECT {safe_pk} AS pk, updated_at FROM {table_ref.full_name}"  # noqa: S608  # TableRef + sqlglot-quoted identifier
            )
            sample_params: list[object] = []
        else:
            sampled_sql = (
                f"SELECT {safe_pk} AS pk, updated_at FROM {table_ref.full_name} "  # noqa: S608  # TableRef + sqlglot-quoted identifier
                "WHERE updated_at >= (now()::TIMESTAMP - (? * INTERVAL 1 DAY)) "
                "ORDER BY updated_at DESC LIMIT ?"
            )
            sample_params = [
                settings.audit_coverage_lookback_days,
                settings.audit_coverage_sample_cap,
            ]
        # Match (schema, table, id) — target_table alone collides across schemas —
        # AND require an audit at-or-after the row's latest mutation. A row audited
        # once then mutated again by a raw bypass advances updated_at past every
        # existing audit row, so "any audit exists" would false-pass; comparing
        # against updated_at catches it. Repo writes set the row's updated_at and
        # the audit's occurred_at from CURRENT_TIMESTAMP in one transaction (equal
        # in DuckDB), so legitimate mutations satisfy occurred_at >= updated_at.
        try:
            rows = self._db.execute(
                f"""
                WITH sampled AS ({sampled_sql})
                SELECT s.pk
                FROM sampled s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {AUDIT_LOG.full_name} a
                    WHERE a.target_schema = ?
                      AND a.target_table = ?
                      AND a.target_id = s.pk
                      AND a.occurred_at >= s.updated_at
                )
                ORDER BY s.pk
                """,  # noqa: S608  # TableRef constants, parameterized values
                [*sample_params, table_ref.schema, table_ref.name],
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — table may not exist before first write
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"coverage check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} {table_ref.name} row(s) mutated without a "
                    "paired app.audit_log row"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_user_categories_uniqueness(self) -> InvariantResult:
        """Flag duplicate ``(category, subcategory)`` rows in ``app.user_categories``.

        V015 relaxed the DB-level ``UNIQUE (category, subcategory)`` constraint
        (``category_id`` is the sole PK now); this is the soft enforcement that
        keeps ``resolve_category_id`` from arbitrating between colliding rows.
        """
        name = "app_user_categories_uniqueness"
        try:
            rows = self._db.execute(
                f"""
                SELECT category, subcategory, string_agg(category_id, ',') AS ids
                FROM {USER_CATEGORIES.full_name}
                GROUP BY category, subcategory
                HAVING COUNT(*) > 1
                ORDER BY category, subcategory
                """  # noqa: S608  # TableRef constant, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — table may not exist before first write
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"uniqueness check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            # `is not None` (not truthiness): DuckDB groups NULL and "" as
            # distinct collision sets, so render them distinctly in the detail.
            labels = [
                f"{cat}/{sub}" if sub is not None else str(cat) for cat, sub, _ in rows
            ]
            affected: list[str] = []
            for *_unused, ids in rows:
                affected.extend(ids.split(","))
            return InvariantResult(
                name=name,
                status="fail",
                detail=f"duplicate (category, subcategory): {', '.join(labels)}",
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _get_transaction_count(self) -> int:
        try:
            row = self._db.execute(
                f"SELECT COUNT(*) FROM {FCT_TRANSACTIONS.full_name}"  # noqa: S608 — TableRef constant
            ).fetchone()
            return int(row[0]) if row else 0
        except Exception:  # noqa: BLE001 — core schema may not exist before first transform
            logger.debug(
                "core.fct_transactions not available; transaction count unavailable"
            )
            return 0

    def _run_sqlmesh_audits(self, verbose: bool) -> list[InvariantResult]:
        """Discover and run all named SQLMesh standalone audits."""
        results: list[InvariantResult] = []
        try:
            with sqlmesh_context(self._db) as ctx:
                for name, audit in ctx.standalone_audits.items():
                    try:
                        sql = audit.render_audit_query().sql(dialect="duckdb")
                        # Audit SQL must return the violation entity ID in column 0.
                        rows = self._db.execute(sql).fetchall()  # noqa: S608 — rendered from trusted audit files
                    except Exception as e:  # noqa: BLE001 — per-audit isolation
                        results.append(
                            InvariantResult(
                                name=name,
                                status="skipped",
                                detail=f"audit failed: {e}",
                                affected_ids=[],
                            )
                        )
                        continue
                    if rows:
                        affected = [str(r[0]) for r in rows] if verbose else []
                        results.append(
                            InvariantResult(
                                name=name,
                                status="fail",
                                detail=f"{len(rows)} violation(s)",
                                affected_ids=affected,
                            )
                        )
                    else:
                        results.append(
                            InvariantResult(
                                name=name,
                                status="pass",
                                detail=None,
                                affected_ids=[],
                            )
                        )
        except Exception as e:  # noqa: BLE001 — SQLMesh raises broad exceptions
            logger.warning(f"SQLMesh audit discovery failed: {e}")
            results.append(
                InvariantResult(
                    name="sqlmesh_audits_unavailable",
                    status="skipped",
                    detail=f"SQLMesh audit discovery failed: {e}",
                    affected_ids=[],
                )
            )
        return results

    def _run_staging_coverage(self) -> InvariantResult:
        # app.match_decisions has no is_primary column — cannot compute
        # dedup secondary count. Mark skipped rather than silently wrong.
        # Revisit when match_decisions gains a primary/secondary designation.
        return InvariantResult(
            name="staging_coverage",
            status="skipped",
            detail="requires is_primary column in app.match_decisions — schema not yet available",
            affected_ids=[],
        )

    def _run_categorization_coverage(self) -> InvariantResult:
        """Warn (not fail) when <50% of non-transfer transactions are categorized."""
        try:
            row = self._db.execute(
                f"""
                SELECT
                    COUNT(*) FILTER (WHERE category IS NULL) AS uncategorized,
                    COUNT(*) AS total
                FROM {FCT_TRANSACTIONS.full_name}
                WHERE NOT COALESCE(is_transfer, FALSE)
                """  # noqa: S608 — TableRef constant, not user input
            ).fetchone()
        except Exception:  # noqa: BLE001 — core schema may not exist before first transform
            return InvariantResult(
                name="categorization_coverage",
                status="skipped",
                detail="fct_transactions not available",
                affected_ids=[],
            )
        if not row or row[1] == 0:
            return InvariantResult(
                name="categorization_coverage",
                status="pass",
                detail=None,
                affected_ids=[],
            )
        uncategorized, total = int(row[0]), int(row[1])
        # Use unrounded ratio for the threshold so values like 49.6% categorized
        # correctly trigger the warning instead of rounding up to 50 and passing.
        pct_categorized = (total - uncategorized) / total * 100
        if pct_categorized < 50:
            pct_uncategorized = round(uncategorized / total * 100)
            return InvariantResult(
                name="categorization_coverage",
                status="warn",
                detail=f"{pct_uncategorized}% of non-transfer transactions are uncategorized",
                affected_ids=[],
            )
        return InvariantResult(
            name="categorization_coverage",
            status="pass",
            detail=None,
            affected_ids=[],
        )
