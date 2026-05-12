"""Pipeline integrity checks — DoctorService."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from moneybin.database import Database, sqlmesh_context
from moneybin.tables import FCT_TRANSACTIONS

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class InvariantResult:
    """Result of one pipeline invariant check."""

    name: str
    status: Literal["pass", "fail", "warn", "skipped"]
    detail: str | None
    affected_ids: list[str]


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

    def run_all(self, verbose: bool = False) -> DoctorReport:
        """Run all invariants and return a DoctorReport."""
        transaction_count = self._get_transaction_count()
        sqlmesh_results = self._run_sqlmesh_audits(verbose)
        staging = self._run_staging_coverage()
        categorization = self._run_categorization_coverage()
        invariants = [*sqlmesh_results, staging, categorization]
        return DoctorReport(invariants=invariants, transaction_count=transaction_count)

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
            with sqlmesh_context() as ctx:
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
