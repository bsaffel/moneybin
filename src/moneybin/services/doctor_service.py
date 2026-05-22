"""Pipeline integrity checks — DoctorService."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from moneybin.database import Database, sqlmesh_context
from moneybin.errors import RecoveryAction
from moneybin.tables import (
    FCT_TRANSACTIONS,
    INT_TRANSACTIONS_UNIONED,
    MATCH_DECISIONS,
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

    def run_all(self, verbose: bool = False) -> DoctorReport:
        """Run all invariants and return a DoctorReport."""
        transaction_count = self._get_transaction_count()
        sqlmesh_results = self._run_sqlmesh_audits(verbose)
        dedup_reconciliation = self._run_dedup_reconciliation()
        categorization = self._run_categorization_coverage()
        invariants = [*sqlmesh_results, dedup_reconciliation, categorization]
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

    def _run_dedup_reconciliation(self) -> InvariantResult:
        """Reconcile raw→core row counts against recorded dedup decisions.

        Every imported row that disappears between the unioned staging layer
        and the core fact table must be explained by an accepted dedup match
        decision. The invariant:

            raw_total - core_count == dedup_absorbed

        where ``dedup_absorbed`` is the count of accepted, non-reversed dedup
        decisions. Each such decision collapses exactly one secondary row into
        its group — so the decision count equals the expected number of
        absorbed rows. A mismatch means rows collapsed without a decision (a
        leak) or a decision failed to collapse its rows (an un-applied match).

        The ``dedup_absorbed == COUNT(decisions)`` identity holds only while
        matching stays 1:1 (each transaction in at most one match — the
        invariant the matcher enforces and ``int_transactions__matched.sql``
        relies on). If multi-hop (3+ way) merges are ever introduced, this
        formula must change to ``SUM(group_size - 1)`` over the connected
        components, in lockstep with that model.
        """
        try:
            raw_total = self._scalar_int(
                f"SELECT COUNT(*) FROM {INT_TRANSACTIONS_UNIONED.full_name}"  # noqa: S608 — TableRef constant
            )
            core_count = self._scalar_int(
                f"SELECT COUNT(DISTINCT transaction_id) FROM {FCT_TRANSACTIONS.full_name}"  # noqa: S608 — TableRef constant
            )
            dedup_absorbed = self._scalar_int(
                f"""
                SELECT COUNT(*) FROM {MATCH_DECISIONS.full_name}
                WHERE match_status = 'accepted'
                  AND reversed_at IS NULL
                  AND match_type = 'dedup'
                """  # noqa: S608 — TableRef constant
            )
        except Exception:  # noqa: BLE001 — prep/core views absent before first transform
            logger.debug("prep/core layer not available; dedup_reconciliation skipped")
            return InvariantResult(
                name="dedup_reconciliation",
                status="skipped",
                detail="prep/core layer not available; run transform first",
                affected_ids=[],
            )
        observed_absorbed = raw_total - core_count
        if observed_absorbed == dedup_absorbed:
            return InvariantResult(
                name="dedup_reconciliation",
                status="pass",
                detail=None,
                affected_ids=[],
            )
        return InvariantResult(
            name="dedup_reconciliation",
            status="fail",
            detail=(
                f"{observed_absorbed} row(s) collapsed between staging and core "
                f"but {dedup_absorbed} dedup decision(s) account for it "
                f"(raw={raw_total}, core={core_count})"
            ),
            affected_ids=[],
        )

    def _scalar_int(self, sql: str) -> int:
        """Execute a single-value COUNT query and return it as an int."""
        row = self._db.execute(sql).fetchone()
        return int(row[0]) if row else 0

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
