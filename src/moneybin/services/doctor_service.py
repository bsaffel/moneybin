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
    ACCOUNT_SETTINGS,
    AUDIT_LOG,
    BALANCE_ASSERTIONS,
    BUDGETS,
    CATEGORIES,
    CATEGORIZATION_RULES,
    CATEGORY_OVERRIDES,
    DIM_ACCOUNTS,
    FCT_TRANSACTIONS,
    GSHEET_CONNECTIONS,
    INT_TRANSACTIONS_MATCHED,
    INT_TRANSACTIONS_UNIONED,
    PROPOSED_RULES,
    TRANSACTION_CATEGORIES,
    USER_CATEGORIES,
    USER_MERCHANTS,
    TableRef,
)

logger = logging.getLogger(__name__)

# Composite-PK projection for app.balance_assertions audit coverage. Must match
# BalanceAssertionsRepo's `target_id` ("{account_id}|{assertion_date ISO}"):
# DuckDB casts a DATE to the same YYYY-MM-DD string date.isoformat() produces.
_BALANCE_ASSERTIONS_PK_EXPR = (
    f"{exp.to_identifier('account_id', quoted=True).sql('duckdb')} || '|' || "
    f"CAST({exp.to_identifier('assertion_date', quoted=True).sql('duckdb')} AS VARCHAR)"
)


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
        dedup_reconciliation = self._run_dedup_reconciliation()
        categorization = self._run_categorization_coverage()
        app_integrity = self._run_app_integrity(full=full)
        invariants = [
            *sqlmesh_results,
            dedup_reconciliation,
            categorization,
            *app_integrity,
        ]
        return DoctorReport(invariants=invariants, transaction_count=transaction_count)

    def _run_app_integrity(self, *, full: bool) -> list[InvariantResult]:
        """Per-table integrity checks for protected ``app.*`` tables (Invariant 10).

        Covers every table wrapped in a repo so far (``user_categories``,
        ``category_overrides``, ``gsheet_connections``, ``user_merchants``,
        ``categorization_rules``, ``proposed_rules``, ``transaction_categories``,
        ``account_settings``, ``balance_assertions``, ``budgets``); later
        repository PRs append one coverage call per newly-wrapped table plus that
        table's FK/orphan specifics.

        Tables without an ``updated_at`` column pass their natural watermark:
        ``proposed_rules`` → ``proposed_at``, ``transaction_categories`` →
        ``categorized_at``. ``balance_assertions`` has a composite PK, so it
        passes ``pk_expr`` to project the same composite ``target_id`` its repo
        emits. Those watermarks catch bypass INSERTs; bypass UPDATEs are the lint
        rule's job — the heuristic limitation the helper documents.
        """
        return [
            self._run_app_audit_coverage(USER_CATEGORIES, "category_id", full=full),
            self._run_app_audit_coverage(CATEGORY_OVERRIDES, "category_id", full=full),
            self._run_app_audit_coverage(
                GSHEET_CONNECTIONS, "connection_id", full=full
            ),
            self._run_app_audit_coverage(USER_MERCHANTS, "merchant_id", full=full),
            self._run_app_audit_coverage(CATEGORIZATION_RULES, "rule_id", full=full),
            self._run_app_audit_coverage(
                PROPOSED_RULES,
                "proposed_rule_id",
                updated_col="proposed_at",
                full=full,
            ),
            self._run_app_audit_coverage(
                TRANSACTION_CATEGORIES,
                "transaction_id",
                updated_col="categorized_at",
                full=full,
            ),
            self._run_app_audit_coverage(ACCOUNT_SETTINGS, "account_id", full=full),
            self._run_app_audit_coverage(
                BALANCE_ASSERTIONS,
                "account_id",
                pk_expr=_BALANCE_ASSERTIONS_PK_EXPR,
                full=full,
            ),
            self._run_app_audit_coverage(BUDGETS, "budget_id", full=full),
            self._run_user_categories_uniqueness(),
            self._run_user_merchants_orphans(),
            self._run_proposed_rules_rule_fk(),
            self._run_transaction_categories_fk(),
            self._run_account_settings_account_fk(),
            self._run_balance_assertions_account_fk(),
            self._run_budgets_category_fk(),
        ]

    def _run_app_audit_coverage(
        self,
        table_ref: TableRef,
        pk_col: str,
        *,
        updated_col: str = "updated_at",
        pk_expr: str | None = None,
        full: bool = False,
    ) -> InvariantResult:
        """Flag rows mutated without a paired ``app.audit_log`` row (Req 9).

        Sampled by default: only rows whose mutation watermark falls within
        ``doctor.audit_coverage_lookback_days`` are checked, capped at
        ``doctor.audit_coverage_sample_cap``. ``full=True`` scans every row.
        ``updated_col`` names the watermark column — ``updated_at`` for most
        protected tables, but tables without one pass their natural mutation
        timestamp (e.g. ``proposed_rules`` → ``proposed_at``,
        ``transaction_categories`` → ``categorized_at``).

        ``pk_expr`` overrides how the row's ``target_id`` is projected: most
        tables key on a single ``pk_col``, but a composite-PK table (e.g.
        ``balance_assertions``) passes a code-supplied SQL expression that
        reconstructs the same composite ``target_id`` its repo emits. The check
        name always derives from ``table_ref.name``; ``pk_col`` is only the
        single-column projection fallback, used when ``pk_expr`` is ``None`` (and
        otherwise ignored).

        Limitations (by design — this is a sampled runtime *heuristic*, not a
        proof): it scans rows that currently exist and keys on the watermark, so
        a raw bypass that (a) deletes a row, or (b) mutates without bumping the
        watermark, leaves nothing for the scan to flag. The *structural* guard
        against raw bypass writes is the lint rule (rejects raw
        ``INSERT``/``UPDATE``/``DELETE`` against protected tables outside
        ``*_repo.py``); content-based coverage is a hosted-tier follow-up (see
        ``private/followups.md``). ``pk_col`` and ``updated_col`` are
        code-supplied constants, quoted defensively per
        ``.claude/rules/security.md``.
        """
        name = f"app_audit_coverage_{table_ref.name}"
        settings = get_settings().doctor
        # `pk_expr` (a code-supplied composite-key expression) wins over the
        # single-column projection when present; both are code constants.
        safe_pk = (
            pk_expr
            if pk_expr is not None
            else exp.to_identifier(pk_col, quoted=True).sql("duckdb")
        )
        safe_updated = exp.to_identifier(updated_col, quoted=True).sql("duckdb")
        if full:
            sampled_sql = (
                f"SELECT {safe_pk} AS pk, {safe_updated} AS updated_at "  # noqa: S608  # TableRef + sqlglot-quoted identifiers
                f"FROM {table_ref.full_name}"
            )
            sample_params: list[object] = []
        else:
            sampled_sql = (
                f"SELECT {safe_pk} AS pk, {safe_updated} AS updated_at "  # noqa: S608  # TableRef + sqlglot-quoted identifiers
                f"FROM {table_ref.full_name} "
                f"WHERE {safe_updated} >= (now()::TIMESTAMP - (? * INTERVAL 1 DAY)) "
                f"ORDER BY {safe_updated} DESC LIMIT ?"
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
                SELECT category, subcategory,
                       string_agg(category_id, ',' ORDER BY category_id) AS ids
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

    def _run_user_merchants_orphans(self) -> InvariantResult:
        """Warn on merchants with no categorization references (warn-only by design).

        Deleting a categorization does not delete the merchant it referenced
        (categorization-matching-mechanics.md), so orphaned merchants accumulate
        as normal wear, not corruption — hence ``warn``, never ``fail``. Gated on
        the audit lookback window so freshly-created merchants not yet fanned out
        to transactions are not flagged.
        """
        name = "app_user_merchants_orphans"
        settings = get_settings().doctor
        try:
            rows = self._db.execute(
                f"""
                SELECT m.merchant_id
                FROM {USER_MERCHANTS.full_name} m
                WHERE m.updated_at < (now()::TIMESTAMP - (? * INTERVAL 1 DAY))
                  AND NOT EXISTS (
                    SELECT 1 FROM {TRANSACTION_CATEGORIES.full_name} c
                    WHERE c.merchant_id = m.merchant_id
                  )
                ORDER BY m.merchant_id
                """,  # noqa: S608  # TableRef constants, parameterized value
                [settings.audit_coverage_lookback_days],
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — table may not exist before first write
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"orphan check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="warn",
                detail=f"{len(affected)} merchant(s) referenced by no categorization",
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_proposed_rules_rule_fk(self) -> InvariantResult:
        """Flag ``proposed_rules.rule_id`` that doesn't resolve in ``categorization_rules``.

        ``rule_id`` is NULL until a proposal is approved and links to its
        promoted rule; a non-NULL value that names no existing rule is a
        dangling FK (V016 replaced the text-keyed linkage with this id). NULL is
        not a violation — only set-but-unresolved ids are flagged.
        """
        name = "app_proposed_rules_rule_fk"
        try:
            rows = self._db.execute(
                f"""
                SELECT p.proposed_rule_id
                FROM {PROPOSED_RULES.full_name} p
                WHERE p.rule_id IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM {CATEGORIZATION_RULES.full_name} r
                    WHERE r.rule_id = p.rule_id
                  )
                ORDER BY p.proposed_rule_id
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — table may not exist before first write
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"FK check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} proposed_rules row(s) reference a "
                    "categorization_rules.rule_id that does not exist"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_transaction_categories_fk(self) -> InvariantResult:
        """Flag ``transaction_categories`` rows with no ``core.fct_transactions`` row.

        ``transaction_id`` is a 1:1 FK to the canonical transaction; a
        categorization referencing a transaction that no longer exists (e.g. a
        re-import dropped it) is an orphan. Skipped before the first transform
        builds ``core.fct_transactions``.
        """
        name = "app_transaction_categories_fk"
        try:
            rows = self._db.execute(
                f"""
                SELECT c.transaction_id
                FROM {TRANSACTION_CATEGORIES.full_name} c
                WHERE NOT EXISTS (
                    SELECT 1 FROM {FCT_TRANSACTIONS.full_name} t
                    WHERE t.transaction_id = c.transaction_id
                )
                ORDER BY c.transaction_id
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core.fct_transactions may not exist yet
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"FK check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} transaction_categories row(s) reference a "
                    "transaction_id absent from core.fct_transactions"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_account_settings_account_fk(self) -> InvariantResult:
        """Flag ``account_settings.account_id`` with no ``core.dim_accounts`` row.

        ``account_id`` is the NOT-NULL PK and a 1:1 FK to the canonical account;
        a settings row for an account that no longer exists (e.g. a re-import
        dropped it) is an orphan. Skipped before the first transform builds
        ``core.dim_accounts``.
        """
        name = "app_account_settings_account_fk"
        try:
            rows = self._db.execute(
                f"""
                SELECT s.account_id
                FROM {ACCOUNT_SETTINGS.full_name} s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {DIM_ACCOUNTS.full_name} a
                    WHERE a.account_id = s.account_id
                )
                ORDER BY s.account_id
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core.dim_accounts may not exist yet
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"FK check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} account_settings row(s) reference an "
                    "account_id absent from core.dim_accounts"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_balance_assertions_account_fk(self) -> InvariantResult:
        """Flag ``balance_assertions.account_id`` with no ``core.dim_accounts`` row.

        Reports distinct violating ``account_id`` values (one account may have
        assertions across many dates). Skipped before ``core.dim_accounts`` exists.
        """
        name = "app_balance_assertions_account_fk"
        try:
            rows = self._db.execute(
                f"""
                SELECT DISTINCT b.account_id
                FROM {BALANCE_ASSERTIONS.full_name} b
                WHERE NOT EXISTS (
                    SELECT 1 FROM {DIM_ACCOUNTS.full_name} a
                    WHERE a.account_id = b.account_id
                )
                ORDER BY b.account_id
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core.dim_accounts may not exist yet
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"FK check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} account(s) with balance_assertions reference an "
                    "account_id absent from core.dim_accounts"
                ),
                affected_ids=affected,
            )
        return InvariantResult(name=name, status="pass", detail=None, affected_ids=[])

    def _run_budgets_category_fk(self) -> InvariantResult:
        """Flag ``budgets.category_id`` that doesn't resolve in ``core.dim_categories``.

        ``category_id`` is nullable (NULL for orphaned legacy rows from V014's
        dual-write backfill); NULL is not a violation, so only set-but-unresolved
        ids are flagged. Skipped before ``core.dim_categories`` exists.
        """
        name = "app_budgets_category_fk"
        try:
            rows = self._db.execute(
                f"""
                SELECT b.budget_id
                FROM {BUDGETS.full_name} b
                WHERE b.category_id IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM {CATEGORIES.full_name} c
                    WHERE c.category_id = b.category_id
                  )
                ORDER BY b.budget_id
                """  # noqa: S608  # TableRef constants, no user input
            ).fetchall()
        except Exception as e:  # noqa: BLE001 — core.dim_categories may not exist yet
            return InvariantResult(
                name=name,
                status="skipped",
                detail=f"FK check unavailable: {e}",
                affected_ids=[],
            )
        if rows:
            affected = [str(r[0]) for r in rows]
            return InvariantResult(
                name=name,
                status="fail",
                detail=(
                    f"{len(affected)} budgets row(s) reference a "
                    "category_id absent from core.dim_categories"
                ),
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

    def _run_dedup_reconciliation(self) -> InvariantResult:
        """Reconcile raw→core row counts against recorded dedup decisions.

        Every imported row that disappears between the unioned staging layer
        and the core fact table must be explained by an accepted dedup match
        decision. The invariant:

            raw_total - core_count == dedup_absorbed

        where ``dedup_absorbed`` is ``Σ(group_size - 1)`` over every connected
        component in ``prep.int_transactions__matched`` — equivalently,
        ``COUNT(*) - COUNT(DISTINCT match_group_id)`` over rows where
        ``match_group_id IS NOT NULL``.

        This formula is exact for any group topology including N-way merges and
        cyclic accepted-edge sets (e.g. three accepted edges over a 3-node
        group still absorbs only 2 rows). A mismatch means rows collapsed
        without a recorded match (a leak) or a match failed to collapse its
        rows (an un-applied decision).
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
                SELECT COUNT(*) - COUNT(DISTINCT match_group_id)
                FROM {INT_TRANSACTIONS_MATCHED.full_name}
                WHERE match_group_id IS NOT NULL
                """  # noqa: S608 — TableRef constant; = SUM(group_size - 1) over components
            )
        except Exception as e:  # noqa: BLE001 — degrade gracefully; surface cause at DEBUG
            # Expected case: prep/core views absent before the first transform.
            # Bind + exc_info so a real fault (renamed column, permissions) is
            # diagnosable rather than masked by the static skip detail.
            logger.debug(f"dedup_reconciliation skipped: {e}", exc_info=True)
            return InvariantResult(
                name="dedup_reconciliation",
                status="skipped",
                detail="prep/core layer not available; run transform first",
                affected_ids=[],
            )
        observed_absorbed = raw_total - core_count
        if observed_absorbed < 0:
            # core can't legitimately hold more rows than staging — report the
            # impossible direction plainly instead of a nonsensical negative
            # "absorbed" count an agent can't act on.
            return InvariantResult(
                name="dedup_reconciliation",
                status="fail",
                detail=(
                    f"core has more rows than staging "
                    f"(core_count={core_count} > raw_total={raw_total}); "
                    "a transaction reached core without passing through the "
                    "staging layer"
                ),
                affected_ids=[],
            )
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
                f"expected {dedup_absorbed} absorbed row(s) from accepted dedup "
                f"decisions but observed {observed_absorbed} "
                f"(raw_total={raw_total}, core_count={core_count})"
            ),
            affected_ids=[],
        )

    def _scalar_int(self, sql: str) -> int:
        """Execute a single-row query and return column 0 as an int (0 if no row)."""
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
